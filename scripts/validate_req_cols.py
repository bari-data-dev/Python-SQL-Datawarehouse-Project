import os
import sys
import json
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

# ----------------------#
# 🔧 Config DB          #
# ----------------------#
load_dotenv()

db_port = os.getenv('DB_PORT')
if db_port is None:
    raise ValueError("DB_PORT not set in .env")

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': int(db_port),
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
}

# ----------------------#
# 🧠 Helper Functions   #
# ----------------------#

def get_client_id_by_schema(client_schema):
    """Ambil client_id berdasarkan client_schema dari tabel reference."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        SELECT client_id
        FROM tools.client_reference
        WHERE client_schema = %s
    """, (client_schema,))
    result = cur.fetchone()
    cur.close()
    conn.close()
    if not result:
        raise ValueError(f"❌ client_schema '{client_schema}' tidak ditemukan di client_reference")
    return result[0]

def get_required_column_version(client_id):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        SELECT required_column_version
        FROM tools.client_reference
        WHERE client_id = %s
    """, (client_id,))
    result = cur.fetchone()
    cur.close()
    conn.close()
    return result[0] if result else None

def get_required_columns(client_id, version, logical_source_file):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        SELECT column_name
        FROM tools.required_columns
        WHERE client_id = %s AND required_column_version = %s AND logical_source_file = %s
    """, (client_id, version, logical_source_file))
    result = cur.fetchall()
    cur.close()
    conn.close()
    return set(r[0] for r in result)

def update_file_audit_log(client_id, json_file_name, batch_id, status, valid_rows=None, invalid_rows=None):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    query = """
        UPDATE tools.file_audit_log
        SET row_validation_status = %s
            {valid_clause}
            {invalid_clause}
        WHERE client_id = %s AND json_file_name = %s AND batch_id = %s
    """

    valid_clause = ", valid_rows = %s" if valid_rows is not None else ""
    invalid_clause = ", invalid_rows = %s" if invalid_rows is not None else ""

    full_query = query.format(valid_clause=valid_clause, invalid_clause=invalid_clause)

    params = [status]
    if valid_rows is not None:
        params.append(valid_rows)
    if invalid_rows is not None:
        params.append(invalid_rows)

    params.extend([client_id, json_file_name, batch_id])

    cur.execute(full_query, tuple(params))
    affected = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    return affected > 0

def insert_row_validation_log(client_id, file_name, row_number, column_name, error_type, error_detail, batch_id):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO tools.row_validation_log(
            client_id, file_name, row_number, column_name,
            error_type, error_detail, batch_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        client_id, file_name, row_number, column_name,
        error_type, error_detail, batch_id
    ))
    conn.commit()
    cur.close()
    conn.close()

def insert_job_log(job_name, client_id, json_file_name, status, start_time, end_time, batch_id, error_message=None):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO tools.job_execution_log(
            job_name, client_id, file_name, status,
            start_time, end_time, batch_id, error_message
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        job_name, client_id, json_file_name,
        status, start_time, end_time, batch_id, error_message
    ))
    conn.commit()
    cur.close()
    conn.close()

# ----------------------#
# 🎯 Validation Logic   #
# ----------------------#

def validate_required_columns(client_schema, json_file_name):
    start_time = datetime.now()
    job_name = os.path.basename(__file__)
    status = "SUCCESS"
    error_message = None

    # 🔹 Ambil client_id di awal
    try:
        client_id = get_client_id_by_schema(client_schema)
    except ValueError as e:
        print(e)
        return False

    json_path = os.path.join("data", client_schema, "incoming", json_file_name)

    if not os.path.exists(json_path):
        status = "FAILED"
        error_message = f"File not found: {json_path}"
        insert_job_log(job_name, client_id, json_file_name, status, start_time, datetime.now(), None, error_message)
        print(f"❌ {error_message}")
        return False

    with open(json_path, 'r') as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError as e:
            status = "FAILED"
            error_message = f"Invalid JSON format: {str(e)}"
            insert_job_log(job_name, client_id, json_file_name, status, start_time, datetime.now(), None, error_message)
            print(f"❌ {error_message}")
            return False

    if not isinstance(data, list) or len(data) == 0:
        status = "FAILED"
        error_message = "JSON data is empty or not a list"
        insert_job_log(job_name, client_id, json_file_name, status, start_time, datetime.now(), None, error_message)
        print(f"❌ {error_message}")
        return False

    logical_source_file = data[0].get("logical_source_file")
    batch_id = data[0].get("batch_id")

    if not logical_source_file:
        status = "FAILED"
        error_message = "Missing 'logical_source_file' in JSON metadata"
        insert_job_log(job_name, client_id, json_file_name, status, start_time, datetime.now(), batch_id, error_message)
        print(f"❌ {error_message}")
        return False

    if not batch_id:
        status = "FAILED"
        error_message = "Missing 'batch_id' in JSON metadata"
        insert_job_log(job_name, client_id, json_file_name, status, start_time, datetime.now(), None, error_message)
        print(f"❌ {error_message}")
        return False

    version = get_required_column_version(client_id)
    if not version:
        status = "FAILED"
        error_message = f"No required_column_version found for client_id '{client_id}'"
        insert_job_log(job_name, client_id, json_file_name, status, start_time, datetime.now(), batch_id, error_message)
        print(f"❌ {error_message}")
        return False

    required_columns = get_required_columns(client_id, version, logical_source_file)
    if not required_columns:
        print(f"⚠️ No required columns configured for logical_source_file: '{logical_source_file}'")
        update_file_audit_log(client_id, json_file_name, batch_id, "SUCCESS")
        insert_job_log(job_name, client_id, json_file_name, "SUCCESS", start_time, datetime.now(), batch_id)
        return True

    errors_found = False
    total_rows = len(data)
    invalid_row_indexes = set()

    for idx, row in enumerate(data, start=1):
        for col in required_columns:
            val = row.get(col, None)
            if val is None or (isinstance(val, str) and val.strip() == ""):
                errors_found = True
                invalid_row_indexes.add(idx)
                insert_row_validation_log(
                    client_id=client_id,
                    file_name=json_file_name,
                    row_number=idx,
                    column_name=col,
                    error_type="NULL_REQUIRED_VALUE",
                    error_detail=f"Required column '{col}' is null or empty at row {idx}",
                    batch_id=batch_id
                )

    invalid_rows = len(invalid_row_indexes)
    valid_rows = total_rows - invalid_rows

    if errors_found:
        update_success = update_file_audit_log(client_id, json_file_name, batch_id, "FAILED", valid_rows, invalid_rows)
        if not update_success:
            error_message = "Validation errors found, but file_audit_log not updated (record not found)"
        else:
            error_message = "Validation failed: null or empty values found in required columns"
        
        insert_job_log(job_name, client_id, json_file_name, "FAILED", start_time, datetime.now(), batch_id, error_message)
        print("❌ Validation failed: null values found in required columns. Logged to DB.")
        return False
    else:
        update_file_audit_log(client_id, json_file_name, batch_id, "SUCCESS", valid_rows, 0)
        insert_job_log(job_name, client_id, json_file_name, "SUCCESS", start_time, datetime.now(), batch_id)
        print("✅ Validation passed: all required columns are filled.")
        return True

# ----------------------#
# 🚀 Main Entry Point   #
# ----------------------#

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python validate_req_cols.py <client_schema> <json_file_name>")
        sys.exit(1)

    client_schema = sys.argv[1]
    json_file_name = sys.argv[2]

    print(f"🔍 Validating required columns in {json_file_name} for client '{client_schema}'...\n")
    if not validate_required_columns(client_schema, json_file_name):
        print("🛑 UNCLEAN: Loading to bronze may contains Null values on Required Columns.")
    else:
        print("✅ PROCEED: Ready for next step.")
