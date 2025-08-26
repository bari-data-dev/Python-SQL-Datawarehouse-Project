import os
import sys
import re
import json
import duckdb
import psycopg2
import pyarrow.parquet as pq
import gc
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
    s = s.strip().lower().replace(" ", "_").replace("-", "_")
    return s


# DB ops
def get_required_columns(
    cur, client_id, logical_source_file, source_system, source_type
):
    sql = (
        "SELECT column_name FROM tools.required_columns "
        "WHERE client_id = %s AND logical_source_file = %s AND source_system = %s AND source_type = %s AND is_active = true "
        "ORDER BY required_id"
    )
    cur.execute(sql, (client_id, logical_source_file, source_system, source_type))
    rows = cur.fetchall()
    return [r[0] for r in rows]


def update_file_audit_row_validation_status(
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
        "UPDATE tools.file_audit_log SET row_validation_status = %s "
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


def insert_row_validation_log(
    conn, client_id, file_name, column_name, error_type, error_detail, batch_id
):
    cur = conn.cursor()
    sql = (
        "INSERT INTO tools.row_validation_log "
        "(client_id, file_name, column_name, error_type, error_detail, batch_id) "
        "VALUES (%s, %s, %s, %s, %s, %s)"
    )
    cur.execute(
        sql, (client_id, file_name, column_name, error_type, error_detail, batch_id)
    )
    conn.commit()
    cur.close()


# -----------------------------
# DuckDB helpers
# -----------------------------
def quote_identifier_for_sql(name: str) -> str:
    # double-quote identifier and escape existing double-quotes by doubling
    return '"' + name.replace('"', '""') + '"'


def quote_path_literal(p: str) -> str:
    # put into single-quoted SQL literal, escape single quotes
    return p.replace("'", "''")


def build_null_check_expression(col_identifier: str) -> str:
    return f"({col_identifier} IS NULL OR NULLIF(TRIM(CAST({col_identifier} AS VARCHAR)), '') IS NULL OR {col_identifier} <> {col_identifier})"


def build_normalized_expr(col_identifier: str) -> str:
    return f"COALESCE(NULLIF(LOWER(TRIM(CAST({col_identifier} AS VARCHAR))), ''), '<NULL>')"


# -----------------------------
# Main
# -----------------------------

def main():
    if len(sys.argv) != 3:
        print("Usage: python validate_row.py <client_schema> <physical_file_name>")
        sys.exit(2)

    client_schema = sys.argv[1]
    physical_file_name = sys.argv[2]
    start_time = datetime.now()
    job_name = "Row Validation"

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
            sys.exit(1)

    file_entry = None
    for f in batch_info.get("files", []):
        if f.get("physical_file_name") == physical_file_name:
            file_entry = f
            break

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
            conn.close()
        except Exception:
            pass
        sys.exit(1)

    # prepare resources
    conn = None
    cur = None
    dconn = None
    pf = None

    try:
        # Fetch required columns from DB
        conn = get_connection()
        cur = conn.cursor()
        required_cols = get_required_columns(
            cur, client_id, logical_source_file, source_system, source_type
        )
        cur.close()

        if not required_cols:
            print("❌ Required Columns Not Found")
            insert_row_validation_log(
                conn,
                client_id,
                parquet_name,
                None,
                "Required Columns Not Found",
                "Required Columns Not Found",
                batch_id,
            )
            insert_job_execution_log(
                conn,
                client_id,
                job_name,
                "FAILED",
                "Required Columns Not Found",
                physical_file_name,
                batch_id,
                start_time,
                datetime.now(),
            )
            update_file_audit_row_validation_status(
                conn,
                client_id,
                physical_file_name,
                source_system,
                source_type,
                logical_source_file,
                batch_id,
                "FAILED",
            )
            sys.exit(1)

        # Map normalized required -> actual parquet column names (use pyarrow to list columns)
        try:
            pf = pq.ParquetFile(parquet_path)
            parquet_actual_cols = list(pf.schema.names)
        except Exception as e:
            msg = f"Failed to read parquet schema for mapping: {e}"
            print(f"❌ {msg}")
            insert_job_execution_log(
                conn,
                client_id,
                job_name,
                "FAILED",
                f"parquet_schema_error:{e}",
                physical_file_name,
                batch_id,
                start_time,
                datetime.now(),
            )
            sys.exit(1)

        normalized_parquet_map = {normalize_name(c): c for c in parquet_actual_cols}

        required_to_actual = {}
        missing_required_cols = []
        for req_raw, req_norm in zip(required_cols, [normalize_name(c) for c in required_cols]):
            actual_col = normalized_parquet_map.get(req_norm)
            if actual_col is None:
                missing_required_cols.append(req_raw)
            else:
                required_to_actual[req_norm] = actual_col

        if missing_required_cols:
            missing_norm = [normalize_name(c) for c in missing_required_cols]
            msg = "Required columns missing in parquet: " + ",".join(missing_required_cols)
            print(f"❌ {msg}")
            insert_row_validation_log(
                conn,
                client_id,
                parquet_name,
                ",".join(missing_norm),
                "ROW_VALIDATION_FAILED",
                msg,
                batch_id,
            )
            insert_job_execution_log(
                conn,
                client_id,
                job_name,
                "FAILED",
                msg,
                parquet_name,
                batch_id,
                start_time,
                datetime.now(),
            )
            update_file_audit_row_validation_status(
                conn,
                client_id,
                physical_file_name,
                source_system,
                source_type,
                logical_source_file,
                batch_id,
                "FAILED",
            )
            sys.exit(1)

        actual_required_cols_in_parquet = [required_to_actual[normalize_name(c)] for c in required_cols]

        # Use DuckDB for fast null and duplicate checks
        dconn = duckdb.connect(database=":memory:")

        parquet_path_sql = quote_path_literal(os.path.abspath(parquet_path))

        # NULL detection via DuckDB
        null_found_columns = set()
        for req_raw, req_norm, actual_col in zip(required_cols, [normalize_name(c) for c in required_cols], actual_required_cols_in_parquet):
            qcol = quote_identifier_for_sql(actual_col)
            null_expr = build_null_check_expression(qcol)
            sql = f"SELECT COUNT(*) FROM read_parquet('{parquet_path_sql}') WHERE {null_expr}"
            res = dconn.execute(sql).fetchone()
            cnt = res[0] if res else 0
            if cnt and int(cnt) > 0:
                null_found_columns.add(req_norm)

        # Duplicate detection via DuckDB
        norm_exprs = [build_normalized_expr(quote_identifier_for_sql(c)) for c in actual_required_cols_in_parquet]
        concat_expr = (" || '\\x1f' || ").join(norm_exprs)
        sql_dup = f"SELECT (COUNT(*) - COUNT(DISTINCT {concat_expr})) AS dup_count FROM read_parquet('{parquet_path_sql}')"
        res = dconn.execute(sql_dup).fetchone()
        dup_count = int(res[0]) if res and res[0] is not None else 0
        duplicate_found = dup_count > 0

        # close duckdb connection as soon as possible
        try:
            dconn.close()
            dconn = None
        except Exception:
            pass

        # build result
        issues = []
        if null_found_columns:
            issues.append("Null found in required column")
        if duplicate_found:
            issues.append("Duplicate Found in Required Column")

        if issues:
            status = "FAILED"
            error_type = "ROW_VALIDATION_FAILED"
            error_detail = "; ".join(issues)
            print(f"❌ {error_detail}")
            try:
                insert_row_validation_log(
                    conn,
                    client_id,
                    parquet_name,
                    ",".join([normalize_name(c) for c in required_cols]),
                    error_type,
                    error_detail,
                    batch_id,
                )
            except Exception as e:
                print(f"⚠️ Failed to insert row_validation_log: {e}")
            try:
                affected = update_file_audit_row_validation_status(
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
                    print("⚠️ Warning: file_audit_log update affected 0 rows (no exact match).")
            except Exception as e:
                print(f"⚠️ Failed to update file_audit_log: {e}")
            try:
                insert_job_execution_log(
                    conn,
                    client_id,
                    job_name,
                    status,
                    error_detail,
                    parquet_name,
                    batch_id,
                    start_time,
                    datetime.now(),
                )
            except Exception as e:
                print(f"⚠️ Failed to insert job_execution_log: {e}")
            sys.exit(1)

        # success
        try:
            affected = update_file_audit_row_validation_status(
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

        print("✅ Row validation passed: no nulls or duplicates on required columns.")
        sys.exit(0)

    except Exception as e:
        print(f"❌ Error in validate_row: {e}")
        try:
            if conn:
                insert_job_execution_log(
                    conn,
                    client_id,
                    job_name,
                    "FAILED",
                    f"error:{e}",
                    parquet_name,
                    batch_id,
                    start_time,
                    datetime.now(),
                )
                try:
                    conn.close()
                except Exception:
                    pass
        except Exception:
            pass
        sys.exit(1)

    finally:
        # ensure resources are released — important on Windows where open file handles block moves
        try:
            if dconn:
                try:
                    dconn.close()
                except Exception:
                    pass
        except Exception:
            pass
        try:
            # delete pyarrow object and force GC to ensure file handles are released
            if 'pf' in locals() and pf is not None:
                try:
                    del pf
                except Exception:
                    pass
        except Exception:
            pass
        try:
            gc.collect()
        except Exception:
            pass
        try:
            if cur:
                try:
                    cur.close()
                except Exception:
                    pass
        except Exception:
            pass
        try:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
        except Exception:
            pass


if __name__ == "__main__":
    main()
