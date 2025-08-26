import os
import sys
import re
import json
import shutil
import psycopg2
import pyarrow.parquet as pq
import gc
import time
import traceback
from datetime import datetime
from dotenv import load_dotenv

# load environment variables
load_dotenv()

# -----------------------------
# DB config via dotenv
# -----------------------------
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


# -----------------------------
# Helpers
# -----------------------------
def get_connection():
    """
    Use DB_CONFIG (loaded from .env) to create a psycopg2 connection.
    """
    return psycopg2.connect(**DB_CONFIG)


def extract_batch_id(filename: str):
    m = re.search(r"(BATCH\d{6})", filename, re.IGNORECASE)
    if m:
        return m.group(1).upper()
    return None


def normalize_name(s: str):
    if s is None:
        return ""
    s = str(s)
    base = s.strip()
    base = base.lower()
    base = base.replace(" ", "_")
    base = base.replace("-", "_")
    return base


def find_file_entry(batch_info: dict, physical_file_name: str):
    for f in batch_info.get("files", []):
        if f.get("physical_file_name") == physical_file_name:
            return f
    return None


def safe_move(src: str, dst: str, retries: int = 5, retry_delay: float = 0.25):
    """
    Robust move:
      - Try os.replace (atomic on same filesystem).
      - If that fails (cross-device or other), fallback to copy2 + remove with retries.
    Returns True on success, False on failure.
    """
    try:
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        try:
            os.replace(src, dst)
            return True
        except Exception:
            # fallback to copy2 + remove
            shutil.copy2(src, dst)
            for i in range(retries):
                try:
                    os.remove(src)
                    return True
                except Exception as e:
                    if i < retries - 1:
                        time.sleep(retry_delay)
                    else:
                        print(
                            f"⚠️ safe_move: failed to remove source after copy: {src} -> {dst}: {e}"
                        )
                        traceback.print_exc()
                        return False
    except Exception as e:
        print(f"⚠️ safe_move: unexpected error moving {src} -> {dst}: {e}")
        traceback.print_exc()
        return False


def move_parquet_to_failed(parquet_path: str, client_schema: str, source_system: str):
    """
    Move parquet file to data/<client_schema>/<source_system>/failed/
    - create directory if missing
    - replace existing file if present
    - logs exceptions (no longer silent)
    """
    try:
        if not parquet_path or not os.path.exists(parquet_path):
            return
        failed_dir = os.path.join("data", client_schema, source_system, "failed")
        os.makedirs(failed_dir, exist_ok=True)
        dest = os.path.join(failed_dir, os.path.basename(parquet_path))
        if os.path.exists(dest):
            try:
                os.remove(dest)
            except Exception as e:
                print(
                    f"⚠️ move_parquet_to_failed: cannot remove existing dest {dest}: {e}"
                )
                traceback.print_exc()
        ok = safe_move(parquet_path, dest)
        if not ok:
            print(f"⚠️ move_parquet_to_failed: move failed for {parquet_path} -> {dest}")
    except Exception as e:
        print(f"⚠️ move_parquet_to_failed: unexpected error for {parquet_path}: {e}")
        traceback.print_exc()


# DB operations
def get_column_mapping_columns(
    cur, client_id, logical_source_file, source_system, source_type
):
    # Retrieve active mapping rows for the client and logical_source_file.
    sql = (
        "SELECT source_column FROM tools.column_mapping "
        "WHERE client_id = %s "
        "AND logical_source_file = %s "
        "AND is_active = true "
        "AND source_system = %s "
        "AND source_type = %s "
        "ORDER BY mapping_id"
    )
    cur.execute(sql, (client_id, logical_source_file, source_system, source_type))
    rows = cur.fetchall()
    return [r[0] for r in rows]


def update_file_audit_mapping_status(
    conn,
    client_id,
    physical_file_name,
    source_system,
    source_type,
    logical_source_file,
    batch_id,
    status,
):
    cur = conn.cursor()
    sql = (
        "UPDATE tools.file_audit_log SET mapping_validation_status = %s "
        "WHERE client_id = %s AND physical_file_name = %s AND source_system = %s "
        "AND source_type = %s AND logical_source_file = %s AND batch_id = %s"
    )
    cur.execute(
        sql,
        (
            status,
            client_id,
            physical_file_name,
            source_system,
            source_type,
            logical_source_file,
            batch_id,
        ),
    )
    affected = cur.rowcount
    conn.commit()
    cur.close()
    return affected


def insert_job_execution_log(
    conn,
    client_id,
    job_name,
    status,
    error_message,
    file_name,
    batch_id,
    start_time,
    end_time,
):
    cur = conn.cursor()
    sql = (
        "INSERT INTO tools.job_execution_log "
        "(client_id, job_name, status, error_message, file_name, batch_id, start_time, end_time) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    )
    cur.execute(
        sql,
        (
            client_id,
            job_name,
            status,
            error_message,
            file_name,
            batch_id,
            start_time,
            end_time,
        ),
    )
    conn.commit()
    cur.close()


def insert_mapping_validation_log(
    conn, client_id, missing, extra, expected, received, file_name, batch_id
):
    cur = conn.cursor()
    sql = (
        "INSERT INTO tools.mapping_validation_log "
        "(client_id, missing_columns, extra_columns, expected_columns, received_columns, file_name, batch_id) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    )
    cur.execute(
        sql,
        (
            client_id,
            missing,
            extra,
            expected,
            received,
            file_name,
            batch_id,
        ),
    )
    conn.commit()
    cur.close()


# -----------------------------
# Main
# -----------------------------
def main():
    if len(sys.argv) != 3:
        print("Usage: python validate_mapping.py <client_schema> <physical_file_name>")
        sys.exit(2)

    client_schema = sys.argv[1]
    physical_file_name = sys.argv[2]
    start_time = datetime.now()
    job_name = "Mapping Validation"

    # <<< ensure pf exists in all branches by initializing early >>>
    pf = None

    # extract batch id
    batch_id = extract_batch_id(physical_file_name)
    if not batch_id:
        print(f"❌ Cannot extract batch_id from file name: {physical_file_name}")
        sys.exit(1)

    batch_info_path = os.path.join(
        "batch_info",
        client_schema,
        "incoming",
        f"batch_output_{client_schema}_{batch_id}.json",
    )
    if not os.path.exists(batch_info_path):
        print(f"❌ Batch info not found: {batch_info_path}")
        sys.exit(1)

    with open(batch_info_path, "r") as bf:
        try:
            batch_info = json.load(bf)
        except Exception as e:
            print(f"❌ Failed to parse batch_info: {e}")
            traceback.print_exc()
            sys.exit(1)

    file_entry = find_file_entry(batch_info, physical_file_name)
    if not file_entry:
        print(
            f"❌ File {physical_file_name} not found inside batch_info {batch_info_path}"
        )
        sys.exit(1)

    parquet_name = file_entry.get("parquet_name")
    logical_source_file = file_entry.get("logical_source_file")
    source_system = (file_entry.get("source_system") or "").lower()
    source_type = (file_entry.get("source_type") or "").lower()
    client_id = batch_info.get("client_id")

    if client_id is None:
        print(f"❌ client_id not found in batch_info {batch_info_path}")
        sys.exit(1)

    if not parquet_name:
        print(
            f"❌ parquet_name not present in batch_info for file {physical_file_name}. Has convert step run?"
        )
        # log failed job
        try:
            conn = get_connection()
            insert_job_execution_log(
                conn,
                client_id,
                job_name,
                "FAILED",
                "parquet_name_missing",
                physical_file_name,
                batch_id,
                start_time,
                datetime.now(),
            )
            # attempt to move parquet if path available (best-effort)
            parquet_path = os.path.join(
                "data",
                client_schema,
                (file_entry.get("source_system") or "").lower(),
                "incoming",
                parquet_name or "",
            )
            # ensure any pyarrow handles released (best-effort) before move
            try:
                if pf is not None:
                    pf = None
                gc.collect()
            except Exception:
                pass
            move_parquet_to_failed(parquet_path, client_schema, source_system)
            conn.close()
        except Exception:
            pass
        sys.exit(1)

    parquet_path = os.path.join(
        "data", client_schema, source_system, "incoming", parquet_name
    )
    if not os.path.exists(parquet_path):
        print(f"❌ Parquet not found: {parquet_path}")
        try:
            conn = get_connection()
            insert_job_execution_log(
                conn,
                client_id,
                job_name,
                "FAILED",
                f"parquet_missing:{parquet_path}",
                physical_file_name,
                batch_id,
                start_time,
                datetime.now(),
            )
            # cannot move because file missing
            conn.close()
        except Exception:
            pass
        sys.exit(1)

    # read parquet schema
    try:
        pf = pq.ParquetFile(parquet_path)
        parquet_cols = set(pf.schema.names)
    except Exception as e:
        print(f"❌ Failed to read parquet schema: {e}")
        traceback.print_exc()
        try:
            conn = get_connection()
            insert_job_execution_log(
                conn,
                client_id,
                job_name,
                "FAILED",
                f"parquet_read_error:{e}",
                physical_file_name,
                batch_id,
                start_time,
                datetime.now(),
            )
            # move file to failed (best-effort) — release pf first
            try:
                if pf is not None:
                    pf = None
                gc.collect()
            except Exception:
                pass
            move_parquet_to_failed(parquet_path, client_schema, source_system)
            conn.close()
        except Exception:
            pass
        sys.exit(1)

    # normalize parquet column names
    normalized_parquet_cols = set([normalize_name(c) for c in parquet_cols])

    # fetch mapping from DB
    try:
        conn = get_connection()
        cur = conn.cursor()
        mapping_cols = get_column_mapping_columns(
            cur, client_id, logical_source_file, source_system, source_type
        )
        cur.close()
    except Exception as e:
        print(f"❌ Failed to fetch column mapping from DB: {e}")
        traceback.print_exc()
        try:
            if conn:
                insert_job_execution_log(
                    conn,
                    client_id,
                    job_name,
                    "FAILED",
                    f"db_error:{e}",
                    physical_file_name,
                    batch_id,
                    start_time,
                    datetime.now(),
                )
                # do NOT move parquet here — DB errors may be transient; but release pf if planning to move
                try:
                    if pf is not None:
                        pf = None
                    gc.collect()
                except Exception:
                    pass
                conn.close()
        except Exception:
            pass
        sys.exit(1)

    if not mapping_cols:
        # no mapping found
        print("❌ Column Mapping Not Found")
        try:
            insert_mapping_validation_log(
                conn, client_id, "", "", "", "", physical_file_name, batch_id
            )
            insert_job_execution_log(
                conn,
                client_id,
                job_name,
                "FAILED",
                "Column Mapping Not Found",
                physical_file_name,
                batch_id,
                start_time,
                datetime.now(),
            )
            # update file_audit_log as FAILED
            try:
                update_file_audit_mapping_status(
                    conn,
                    client_id,
                    physical_file_name,
                    source_system,
                    source_type,
                    logical_source_file,
                    batch_id,
                    "FAILED",
                )
            except Exception:
                pass
            # move parquet to failed
            try:
                if pf is not None:
                    pf = None
                gc.collect()
            except Exception:
                pass
            move_parquet_to_failed(parquet_path, client_schema, source_system)
            conn.close()
        except Exception:
            pass
        sys.exit(1)

    normalized_mapping_cols = set([normalize_name(c) for c in mapping_cols])

    # Compare sets
    missing = sorted(list(normalized_mapping_cols - normalized_parquet_cols))
    extra = sorted(list(normalized_parquet_cols - normalized_mapping_cols))

    # Build comma-separated strings
    expected_csv = ",".join(sorted(normalized_mapping_cols))
    received_csv = ",".join(sorted(normalized_parquet_cols))
    missing_csv = ",".join(missing) if missing else ""
    extra_csv = ",".join(extra) if extra else ""

    if missing or extra:
        status = "FAILED"
        error_message = f"Missing: {missing_csv}; Extra: {extra_csv}"
        print("❌ Validation failed:", error_message)
        try:
            insert_mapping_validation_log(
                conn,
                client_id,
                missing_csv,
                extra_csv,
                expected_csv,
                received_csv,
                parquet_name,
                batch_id,
            )
        except Exception as e:
            print(f"⚠️ Failed to insert mapping_validation_log: {e}")
            traceback.print_exc()
        try:
            affected = update_file_audit_mapping_status(
                conn,
                client_id,
                physical_file_name,
                source_system,
                source_type,
                logical_source_file,
                batch_id,
                "FAILED",
            )
            if affected == 0:
                print(
                    "⚠️ Warning: file_audit_log update affected 0 rows (no exact match)."
                )
        except Exception as e:
            print(f"⚠️ Failed to update file_audit_log: {e}")
            traceback.print_exc()
        try:
            insert_job_execution_log(
                conn,
                client_id,
                job_name,
                status,
                error_message,
                physical_file_name,
                batch_id,
                start_time,
                datetime.now(),
            )
        except Exception as e:
            print(f"⚠️ Failed to insert job_execution_log: {e}")
            traceback.print_exc()
        # move parquet to failed (replace if exists)
        try:
            if pf is not None:
                pf = None
            gc.collect()
        except Exception:
            pass
        try:
            move_parquet_to_failed(parquet_path, client_schema, source_system)
        except Exception:
            pass
        conn.close()
        sys.exit(1)

    # success
    try:
        affected = update_file_audit_mapping_status(
            conn,
            client_id,
            physical_file_name,
            source_system,
            source_type,
            logical_source_file,
            batch_id,
            "SUCCESS",
        )
        if affected == 0:
            print("⚠️ Warning: file_audit_log update affected 0 rows (no exact match).")
    except Exception as e:
        print(f"⚠️ Failed to update file_audit_log: {e}")

    try:
        insert_job_execution_log(
            conn,
            client_id,
            job_name,
            "SUCCESS",
            None,
            parquet_name,
            batch_id,
            start_time,
            datetime.now(),
        )
    except Exception as e:
        print(f"⚠️ Failed to insert job_execution_log: {e}")

    # close connection
    try:
        conn.close()
    except Exception:
        pass

    print("✅ Validation passed: Parquet schema matches column mapping.")
    sys.exit(0)


if __name__ == "__main__":
    main()
