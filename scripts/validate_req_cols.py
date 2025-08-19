import os
import sys
from datetime import datetime
from dotenv import load_dotenv

import psycopg2
import psycopg2.extras as extras
import pandas as pd
import pyarrow.parquet as pq

# ----------------------#
# 🔧 Config DB          #
# ----------------------#
load_dotenv()

db_port = os.getenv("DB_PORT")
if db_port is None:
    raise ValueError("DB_PORT not set in .env")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(db_port),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

# ----------------------#
# 🧠 Helper Functions   #
# ----------------------#

def get_client_id_by_schema(client_schema):
    """Ambil client_id berdasarkan client_schema dari tabel reference."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT client_id
        FROM tools.client_reference
        WHERE client_schema = %s
        """,
        (client_schema,),
    )
    result = cur.fetchone()
    cur.close()
    conn.close()

    if not result:
        raise ValueError(f"❌ client_schema '{client_schema}' tidak ditemukan di client_reference")

    return result[0]


def get_required_column_version(client_id):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT required_column_version
        FROM tools.client_reference
        WHERE client_id = %s
        """,
        (client_id,),
    )
    result = cur.fetchone()
    cur.close()
    conn.close()
    return result[0] if result else None


def get_required_columns(client_id, version, logical_source_file):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT column_name
        FROM tools.required_columns
        WHERE client_id = %s
          AND required_column_version = %s
          AND logical_source_file = %s
        """,
        (client_id, version, logical_source_file),
    )
    result = cur.fetchall()
    cur.close()
    conn.close()
    return {r[0] for r in result}


def update_file_audit_log(client_id, parquet_file_name, batch_id, status, valid_rows=None, invalid_rows=None):
    """
    NOTE: signature preserved. parquet_file_name will contain parquet file name for compatibility.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    query = """
        UPDATE tools.file_audit_log
        SET row_validation_status = %s
            {valid_clause}
            {invalid_clause}
        WHERE client_id = %s
          AND parquet_file_name = %s
          AND batch_id = %s
    """

    valid_clause = ", valid_rows = %s" if valid_rows is not None else ""
    invalid_clause = ", invalid_rows = %s" if invalid_rows is not None else ""

    full_query = query.format(valid_clause=valid_clause, invalid_clause=invalid_clause)

    params = [status]
    if valid_rows is not None:
        params.append(valid_rows)
    if invalid_rows is not None:
        params.append(invalid_rows)

    params.extend([client_id, parquet_file_name, batch_id])

    cur.execute(full_query, tuple(params))
    affected = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    return affected > 0


def batch_insert_row_validation_log(rows):
    """
    Bulk-insert rows into tools.row_validation_log.
    rows: list of tuples (client_id, file_name, row_number, column_name, error_type, error_detail, batch_id)
    """
    if not rows:
        return
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    sql = """
        INSERT INTO tools.row_validation_log (
            client_id, file_name, row_number, column_name,
            error_type, error_detail, batch_id
        ) VALUES %s
    """
    extras.execute_values(cur, sql, rows, page_size=1000)
    conn.commit()
    cur.close()
    conn.close()


def insert_row_validation_log(client_id, file_name, row_number, column_name, error_type, error_detail, batch_id):
    """
    Preserve single-insert function (keamanan backward compatibility). Not used in main loop to keep performance.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO tools.row_validation_log (
            client_id, file_name, row_number, column_name,
            error_type, error_detail, batch_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (client_id, file_name, row_number, column_name, error_type, error_detail, batch_id),
    )
    conn.commit()
    cur.close()
    conn.close()


def insert_job_log(job_name, client_id, parquet_file_name, status, start_time, end_time, batch_id, error_message=None):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO tools.job_execution_log (
            job_name, client_id, file_name, status,
            start_time, end_time, batch_id, error_message
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (job_name, client_id, parquet_file_name, status, start_time, end_time, batch_id, error_message),
    )
    conn.commit()
    cur.close()
    conn.close()


# ----------------------#
# 🎯 Validation Logic   #
# ----------------------#

def validate_required_columns(client_schema, parquet_file_name, batch_size=100_000, flush_log_batch=5_000):
    """
    Refactor untuk Parquet:
    - parquet_file_name tetap parameter (diisi nama file .parquet)
    - baca schema & total rows via pyarrow metadata (hemat memori)
    - iterasi data per-batch (pyarrow -> pandas chunk), validasi vectorized
    - kumpulkan row error ke buffer, flush bulk insert setiap flush_log_batch
    """
    start_time = datetime.now()
    job_name = os.path.basename(__file__)
    status = "SUCCESS"
    error_message = None

    # Ambil client_id
    try:
        client_id = get_client_id_by_schema(client_schema)
    except ValueError as e:
        print(e)
        return False

    parquet_path = os.path.join("data", client_schema, "incoming", parquet_file_name)

    if not os.path.exists(parquet_path):
        status = "FAILED"
        error_message = f"File not found: {parquet_path}"
        insert_job_log(job_name, client_id, parquet_file_name, status, start_time, datetime.now(), None, error_message)
        print(f"❌ {error_message}")
        return False

    # Dapatkan schema & total rows tanpa load data penuh
    try:
        pf = pq.ParquetFile(parquet_path)
        try:
            total_rows = int(pf.metadata.num_rows)
        except Exception:
            # fallback: 0 or unknown
            total_rows = None
    except Exception as e:
        status = "FAILED"
        error_message = f"Failed reading parquet metadata: {e}"
        insert_job_log(job_name, client_id, parquet_file_name, status, start_time, datetime.now(), None, error_message)
        print(f"❌ {error_message}")
        return False

    # Ambil 1 row untuk metadata penting: logical_source_file & batch_id
    try:
        # baca 1 row hanya untuk metadata
        table_head = pf.read_row_groups(list(range(min(1, pf.num_row_groups))))  # read minimal
        # safer approach: use dataset head via pandas; but we'll try read with pyarrow
        # fallback to using pandas on small sample
        sample_df = pf.read_row_groups([0], columns=["logical_source_file", "batch_id"]).to_pandas() if pf.num_row_groups > 0 else None
    except Exception:
        sample_df = None

    if sample_df is None or sample_df.shape[0] == 0:
        # try using pandas read of first 1 row (safe for very large parquet because only first row read)
        try:
            sample_df = pd.read_parquet(parquet_path, columns=["logical_source_file", "batch_id"], engine="pyarrow").head(1)
        except Exception as e:
            status = "FAILED"
            error_message = f"Unable to read sample metadata from parquet: {e}"
            insert_job_log(job_name, client_id, parquet_file_name, status, start_time, datetime.now(), None, error_message)
            print(f"❌ {error_message}")
            return False

    # extract metadata scalars
    if "logical_source_file" not in sample_df.columns:
        status = "FAILED"
        error_message = "Missing 'logical_source_file' in parquet metadata/columns"
        insert_job_log(job_name, client_id, parquet_file_name, status, start_time, datetime.now(), None, error_message)
        print(f"❌ {error_message}")
        return False

    logical_source_file = sample_df.iloc[0].get("logical_source_file")
    batch_id = sample_df.iloc[0].get("batch_id")

    if not logical_source_file:
        status = "FAILED"
        error_message = "Missing logical_source_file value in parquet metadata"
        insert_job_log(job_name, client_id, parquet_file_name, status, start_time, datetime.now(), batch_id, error_message)
        print(f"❌ {error_message}")
        return False

    if not batch_id:
        status = "FAILED"
        error_message = "Missing batch_id value in parquet metadata"
        insert_job_log(job_name, client_id, parquet_file_name, status, start_time, datetime.now(), None, error_message)
        print(f"❌ {error_message}")
        return False

    # Ambil required columns dari DB
    version = get_required_column_version(client_id)
    if not version:
        status = "FAILED"
        error_message = f"No required_column_version found for client_id '{client_id}'"
        insert_job_log(job_name, client_id, parquet_file_name, status, start_time, datetime.now(), batch_id, error_message)
        print(f"❌ {error_message}")
        return False

    required_columns = get_required_columns(client_id, version, logical_source_file)
    if not required_columns:
        # jika tidak ada required columns, anggap success
        print(f"⚠️ No required columns configured for logical_source_file: '{logical_source_file}'")
        update_file_audit_log(client_id, parquet_file_name, batch_id, "SUCCESS")
        insert_job_log(job_name, client_id, parquet_file_name, "SUCCESS", start_time, datetime.now(), batch_id)
        return True

    # Start streaming validation per-batch
    errors_found = False
    invalid_rows_total = 0
    log_buffer = []
    rows_processed = 0

    try:
        # iterate record batches (pyarrow) -> convert to pandas chunk
        for record_batch in pf.iter_batches(batch_size=batch_size):
            chunk_df = record_batch.to_pandas()
            # ensure consistent dtypes and replace NaN -> ""
            chunk_df = chunk_df.fillna("")

            # determine row_number: prefer csv_row_number column if present, else use running counter
            if "csv_row_number" in chunk_df.columns:
                row_numbers = chunk_df["csv_row_number"].astype(int).tolist()
            else:
                row_numbers = list(range(rows_processed + 1, rows_processed + 1 + len(chunk_df)))

            # vectorized check per required column
            chunk_invalid_rows = set()
            for col in required_columns:
                if col in chunk_df.columns:
                    # check empty or whitespace-only
                    # convert to str then strip (works even if values are numeric)
                    series = chunk_df[col].astype(str)
                    mask = series.str.strip() == ""
                    if mask.any():
                        errors_found = True
                        idxs = mask[mask].to_numpy().nonzero()[0]
                        for idx in idxs:
                            rn = row_numbers[idx]
                            # prepare error tuple
                            log_buffer.append((
                                client_id,
                                parquet_file_name,
                                int(rn),
                                col,
                                "NULL_REQUIRED_VALUE",
                                f"Required column '{col}' is null or empty at row {rn}",
                                batch_id
                            ))
                            chunk_invalid_rows.add(rn)
                else:
                    # column missing entirely in this chunk -> mark all rows invalid for this column
                    errors_found = True
                    for i, rn in enumerate(row_numbers):
                        log_buffer.append((
                            client_id,
                            parquet_file_name,
                            int(rn),
                            col,
                            "NULL_REQUIRED_VALUE",
                            f"Required column '{col}' is missing in file at row {rn}",
                            batch_id
                        ))
                        chunk_invalid_rows.add(rn)

            invalid_rows_total += len(chunk_invalid_rows)
            rows_processed += len(chunk_df)

            # Flush log buffer in batches to avoid memory blowup and many roundtrips
            if len(log_buffer) >= flush_log_batch:
                batch_insert_row_validation_log(log_buffer)
                log_buffer = []

        # After loop flush remaining logs
        if log_buffer:
            batch_insert_row_validation_log(log_buffer)
            log_buffer = []

    except Exception as e:
        status = "FAILED"
        error_message = f"Error during parquet iteration/validation: {e}"
        insert_job_log(job_name, client_id, parquet_file_name, status, start_time, datetime.now(), batch_id, error_message)
        print(f"❌ {error_message}")
        return False

    # Determine total_rows: prefer metadata if available, else rows_processed
    total_rows_final = total_rows if total_rows is not None else rows_processed
    valid_rows = total_rows_final - invalid_rows_total if total_rows_final is not None else None

    # Persist results
    if errors_found:
        # update audit log: status FAILED
        update_success = update_file_audit_log(client_id, parquet_file_name, batch_id, "FAILED", valid_rows, invalid_rows_total)
        if not update_success:
            error_message = "Validation errors found, but file_audit_log not updated (record not found)"
        else:
            error_message = "Validation failed: null or empty values found in required columns"

        insert_job_log(job_name, client_id, parquet_file_name, "FAILED", start_time, datetime.now(), batch_id, error_message)
        print("❌ Validation failed: null values found in required columns. Logged to DB.")
        return False

    # success path
    update_file_audit_log(client_id, parquet_file_name, batch_id, "SUCCESS", valid_rows, 0)
    insert_job_log(job_name, client_id, parquet_file_name, "SUCCESS", start_time, datetime.now(), batch_id)
    print("✅ Validation passed: all required columns are filled.")
    return True


# ----------------------#
# 🚀 Main Entry Point   #
# ----------------------#

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python validate_req_cols.py <client_schema> <parquet_file_name>")
        sys.exit(1)

    client_schema = sys.argv[1]
    parquet_file_name = sys.argv[2]

    print(f"🔍 Validating required columns in {parquet_file_name} for client '{client_schema}'...\n")

    if not validate_required_columns(client_schema, parquet_file_name):
        print("🛑 UNCLEAN: Loading to bronze may contains Null values on Required Columns.")
        sys.exit(1)
    else:
        print("✅ PROCEED: Ready for next step.")
        sys.exit(0)
