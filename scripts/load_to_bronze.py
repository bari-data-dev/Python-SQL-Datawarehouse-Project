import os
import sys
import json
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from datetime import datetime

# ----------------------#
# Load Environment Vars #
# ----------------------#
load_dotenv()

DB_PORT = os.getenv("DB_PORT")
if DB_PORT is None:
    raise ValueError("DB_PORT not set in .env")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(DB_PORT),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

EXCLUDE_KEYS = {"csv_row_number", "source_file", "logical_source_file", "batch_id"}


# ----------------------#
# Helper DB Functions   #
# ----------------------#
def get_client_id(cursor, client_schema):
    cursor.execute("""
        SELECT client_id
        FROM tools.client_reference
        WHERE client_schema = %s
    """, (client_schema,))
    row = cursor.fetchone()
    if not row:
        raise Exception(f"client_schema '{client_schema}' tidak ditemukan di client_reference")
    return row[0]


def log_job_execution(cursor, job_name, client_id, file_name, batch_id,
                      status, start_time, error_message=None):
    cursor.execute("""
        INSERT INTO tools.job_execution_log (
            job_name, client_id, file_name, batch_id,
            status, start_time, end_time, error_message
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        job_name, client_id, file_name, batch_id, status,
        start_time, datetime.now(), error_message
    ))


def update_file_audit_log(cursor, client_id, file_name, batch_id, status):
    cursor.execute("""
        UPDATE tools.file_audit_log
        SET load_status = %s
        WHERE client_id = %s
          AND parquet_file_name = %s
          AND batch_id = %s
    """, (status, client_id, file_name, batch_id))


def log_error_record(cursor, client_id, batch_id,
                     logical_source_file, file_name, record, error_msg):
    cursor.execute("""
        INSERT INTO tools.load_error_log (
            client_id, batch_id, logical_source_file, file_name,
            error_message, raw_data, error_time
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        client_id, batch_id, logical_source_file, file_name,
        error_msg, json.dumps(record), datetime.now()
    ))


def get_reference_versions(cursor, client_id):
    cursor.execute("""
        SELECT mapping_version, config_version
        FROM tools.client_reference
        WHERE client_id = %s
    """, (client_id,))
    row = cursor.fetchone()
    if not row:
        raise Exception(f"client_id '{client_id}' tidak ditemukan di client_reference")
    return row[0], row[1]


def get_target_table(cursor, client_id, logical_source_file, config_version):
    cursor.execute("""
        SELECT target_schema, target_table
        FROM tools.client_config
        WHERE client_id = %s
          AND logical_source_file = %s
          AND config_version = %s
    """, (client_id, logical_source_file, config_version))
    row = cursor.fetchone()
    if not row:
        raise Exception(f"Config untuk file '{logical_source_file}' tidak ditemukan di client_config")
    return row[0], row[1]


def get_column_mapping(cursor, client_id, logical_source_file, mapping_version):
    cursor.execute("""
        SELECT source_column, target_column
        FROM tools.column_mapping
        WHERE client_id = %s
          AND logical_source_file = %s
          AND mapping_version = %s
    """, (client_id, logical_source_file, mapping_version))
    rows = cursor.fetchall()
    if not rows:
        raise Exception(f"Column mapping tidak ditemukan untuk file '{logical_source_file}'")
    return {source: target for source, target in rows}


def ensure_dwh_batch_id_column(cursor, target_schema, target_table):
    cursor.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
          AND column_name = 'dwh_batch_id'
    """, (target_schema, target_table))
    if cursor.fetchone() is None:
        print(f"ℹ️ Menambahkan kolom dwh_batch_id ke {target_schema}.{target_table}")
        cursor.execute(f"""
            ALTER TABLE {target_schema}.{target_table}
            ADD COLUMN dwh_batch_id VARCHAR(255)
        """)


def clean_value(val):
    if isinstance(val, str) and val.strip() == "":
        return None
    return val


def rename_and_clean_record(record, column_map):
    return {
        column_map[k]: clean_value(v)
        for k, v in record.items()
        if k in column_map and k not in EXCLUDE_KEYS
    }


# ----------------------#
# Main Processing Logic #
# ----------------------#
def main(client_schema, parquet_file_name, chunk_size=10000):
    start_time = datetime.now()
    job_name = "load_to_bronze"
    file_path = os.path.join("data", client_schema, "incoming", parquet_file_name)

    try:
        # Baca header dulu untuk metadata
        df_head = pd.read_parquet(file_path, engine="pyarrow", columns=["logical_source_file", "batch_id"])
        if df_head.empty:
            raise Exception("File kosong atau tidak memiliki record valid")
        logical_source_file = df_head.iloc[0]["logical_source_file"]
        batch_id = df_head.iloc[0]["batch_id"]

        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                client_id = get_client_id(cursor, client_schema)
                mapping_version, config_version = get_reference_versions(cursor, client_id)
                target_schema, target_table = get_target_table(cursor, client_id, logical_source_file, config_version)
                column_map = get_column_mapping(cursor, client_id, logical_source_file, mapping_version)

                ensure_dwh_batch_id_column(cursor, target_schema, target_table)

                # DELETE existing data for this batch_id before insert
                cursor.execute(
                    f"DELETE FROM {target_schema}.{target_table} WHERE dwh_batch_id = %s",
                    (batch_id,)
                )

                # Streaming read parquet in row groups (lebih aman ketimbang chunksize iterrows)
                parquet_file = pd.read_parquet(file_path, engine="pyarrow")
                for start in range(0, len(parquet_file), chunk_size):
                    chunk = parquet_file.iloc[start:start+chunk_size]

                    cleaned_data = []
                    for row_dict in chunk.to_dict(orient="records"):
                        try:
                            cleaned_row = rename_and_clean_record(row_dict, column_map)
                            cleaned_row["dwh_batch_id"] = batch_id
                            cleaned_data.append(cleaned_row)
                        except Exception as e:
                            log_error_record(cursor, client_id, batch_id,
                                             logical_source_file, parquet_file_name, row_dict, str(e))

                    if cleaned_data:
                        columns = list(cleaned_data[0].keys())
                        values = [[row[col] for col in columns] for row in cleaned_data]
                        insert_sql = f"""
                            INSERT INTO {target_schema}.{target_table} ({', '.join(columns)})
                            VALUES %s
                        """
                        execute_values(cursor, insert_sql, values)

                update_file_audit_log(cursor, client_id, parquet_file_name, batch_id, "SUCCESS")
                log_job_execution(cursor, job_name, client_id, parquet_file_name, batch_id, "SUCCESS", start_time)

    except Exception as e:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                try:
                    client_id = get_client_id(cursor, client_schema)
                    update_file_audit_log(cursor, client_id, parquet_file_name, batch_id, "FAILED")
                except:
                    pass
                try:
                    log_job_execution(cursor, job_name,
                                      client_id if "client_id" in locals() else None,
                                      parquet_file_name,
                                      batch_id if "batch_id" in locals() else None,
                                      "FAILED", start_time, str(e))
                except:
                    pass
        print(f"❌ Gagal memproses file '{parquet_file_name}': {e}")
        sys.exit(1)

    print(f"✅ Berhasil load file '{parquet_file_name}' ke {target_schema}.{target_table}")


# ----------------------#
# 🚀 Main Entry Point   #
# ----------------------#
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python load_to_bronze.py <client_schema> <parquet_file_name>")
        sys.exit(1)

    client_schema = sys.argv[1]
    parquet_file_name = sys.argv[2]
    main(client_schema, parquet_file_name)
