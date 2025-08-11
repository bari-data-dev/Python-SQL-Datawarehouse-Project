import os
import pandas as pd
import argparse
import json
import shutil
import psycopg2
import re
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def validate_client_name(client: str) -> bool:
    return re.match(r"^[a-z0-9_]+$", client) is not None

def get_client_id(conn, client_schema):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT client_id
            FROM tools.client_reference
            WHERE client_schema = %s
        """, (client_schema,))
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Client schema '{client_schema}' tidak ditemukan di client_reference.")
        return row[0]

def get_logical_source_files(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT cc.logical_source_file
            FROM tools.client_config cc
            JOIN tools.client_reference cr 
              ON cc.client_id = cr.client_id
             AND cc.config_version = cr.config_version
            WHERE cc.client_id = %s 
              AND cc.is_active = true
        """, (client_id,))
        return set(row[0].lower() for row in cur.fetchall())

def extract_batch_id(file_name: str) -> str:
    match = re.search(r"_?(BATCH\d{6})", file_name, re.IGNORECASE)
    if not match:
        raise ValueError(f"Batch ID tidak ditemukan di filename: {file_name}")
    return match.group(1).upper()

def insert_file_audit_log(conn, **kwargs):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO tools.file_audit_log (
                client_id, logical_source_file, physical_file_name, json_file_name,
                file_received_time, json_converted_time, total_rows,
                convert_status, processed_by, batch_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            kwargs["client_id"],
            kwargs["logical_source_file"],
            kwargs["physical_file_name"],
            kwargs["json_file_name"],
            kwargs["file_received_time"],
            kwargs["json_converted_time"],
            kwargs["total_rows"],
            kwargs["convert_status"],
            kwargs.get("processed_by", "autoloader"),
            kwargs.get("batch_id")
        ))
    conn.commit()

def insert_job_execution_log(conn, client_id, file_name, status, error_message=None, batch_id=None):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO tools.job_execution_log (
                job_name, client_id, file_name, status, start_time, end_time, error_message, batch_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            "csv_to_json.py", client_id, file_name, status,
            datetime.now(), datetime.now(), error_message, batch_id
        ))
    conn.commit()

def convert_csv_to_json(client_schema, file_name):
    if not validate_client_name(client_schema):
        print("❌ Nama client schema tidak valid.")
        return

    client_schema = client_schema.lower()
    input_dir = os.path.join("raw", client_schema, "incoming")
    failed_dir = os.path.join("raw", client_schema, "failed")
    output_dir = os.path.join("data", client_schema, "incoming")

    for path in [output_dir, failed_dir]:
        os.makedirs(path, exist_ok=True)

    file_path = os.path.join(input_dir, file_name)
    if not os.path.isfile(file_path):
        print(f"❌ File tidak ditemukan: {file_path}")
        return

    batch_id = extract_batch_id(file_name)

    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
    except Exception as e:
        print(f"❌ Gagal koneksi DB: {e}")
        return

    try:
        client_id = get_client_id(conn, client_schema)
    except ValueError as e:
        print(f"❌ {e}")
        conn.close()
        return

    logical_sources = get_logical_source_files(conn, client_id)

    base_name = os.path.splitext(file_name)[0].lower()
    json_file = file_name.replace(".csv", ".json")
    output_path = os.path.join(output_dir, json_file)

    file_received_time = datetime.fromtimestamp(os.path.getmtime(file_path))
    json_converted_time = datetime.now()

    matched_logical_file = next(
        (ls for ls in logical_sources if base_name.startswith(ls.lower())), None
    )

    if not matched_logical_file:
        print(f"⛔ {file_name} tidak cocok logical_source_file")
        insert_job_execution_log(conn, client_id, file_name, "FAILED",
                                 f"logical_source_file not found: {base_name}",
                                 batch_id=batch_id)
        shutil.move(file_path, os.path.join(failed_dir, file_name))
        print(f"📁 Dipindahkan ke: {failed_dir}")
        conn.close()
        return

    try:
        print(f"🔄 Memproses: {file_name}")
        df = pd.read_csv(file_path, dtype=str).fillna("")
        df.insert(0, "csv_row_number", range(1, len(df) + 1))
        df["source_file"] = file_name
        df["logical_source_file"] = matched_logical_file
        df["batch_id"] = batch_id

        if os.path.exists(output_path):
            print(f"⚠️ Lewat, sudah ada: {output_path}")
            conn.close()
            return

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(df.to_dict(orient="records"), f, ensure_ascii=False, indent=2)

        print(f"✅ JSON disimpan: {output_path}")

        insert_file_audit_log(
            conn=conn,
            client_id=client_id,
            logical_source_file=matched_logical_file,
            physical_file_name=file_name,
            json_file_name=json_file,
            file_received_time=file_received_time,
            json_converted_time=json_converted_time,
            total_rows=len(df),
            convert_status="SUCCESS",
            batch_id=batch_id
        )

        insert_job_execution_log(conn, client_id, file_name, "SUCCESS", batch_id=batch_id)

    except Exception as e:
        print(f"❌ Gagal proses {file_name}: {e}")
        insert_file_audit_log(
            conn=conn,
            client_id=client_id,
            logical_source_file=matched_logical_file if 'matched_logical_file' in locals() else None,
            physical_file_name=file_name,
            json_file_name=json_file,
            file_received_time=file_received_time,
            json_converted_time=json_converted_time,
            total_rows=0,
            convert_status="FAILED",
            batch_id=batch_id
        )
        insert_job_execution_log(conn, client_id, file_name, "FAILED", str(e), batch_id=batch_id)
        shutil.move(file_path, os.path.join(failed_dir, file_name))
        print(f"📁 Dipindahkan ke: {failed_dir}")

    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("client", help="Nama client schema, contoh: client1")
    parser.add_argument("csv_file", help="Nama file CSV, contoh: cust-info_BATCH000002.csv")
    args = parser.parse_args()

    convert_csv_to_json(args.client, args.csv_file)
