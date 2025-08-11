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

def get_client_id(client_schema):
    """Resolve client_schema to client_id."""
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
    return result[0] if result else None

def get_mapping_version(client_id):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        SELECT mapping_version
        FROM tools.client_reference
        WHERE client_id = %s
    """, (client_id,))
    result = cur.fetchone()
    cur.close()
    conn.close()
    return result[0] if result else None

def get_column_mapping(client_id, mapping_version, logical_source_file):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        SELECT source_column
        FROM tools.column_mapping
        WHERE client_id = %s AND mapping_version = %s AND logical_source_file = %s
    """, (client_id, mapping_version, logical_source_file))
    result = cur.fetchall()
    cur.close()
    conn.close()
    return set(r[0] for r in result)

def update_file_audit_log(client_id, json_file_name, status, batch_id):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        UPDATE tools.file_audit_log
        SET mapping_validation_status = %s
        WHERE client_id = %s AND json_file_name = %s AND batch_id = %s
    """, (status, client_id, json_file_name, batch_id))
    affected = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    return affected > 0

def insert_validation_log(client_id, expected, received, missing, extra, json_file_name, batch_id):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO tools.mapping_validation_log(
            client_id, expected_columns, received_columns,
            missing_columns, extra_columns, file_name, batch_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        client_id,
        ", ".join(sorted(expected)),
        ", ".join(sorted(received)),
        ", ".join(sorted(missing)),
        ", ".join(sorted(extra)),
        json_file_name,
        batch_id
    ))
    conn.commit()
    cur.close()
    conn.close()

def insert_job_log(job_name, client_id, json_file_name, status, start_time, end_time, error_message=None, batch_id=None):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO tools.job_execution_log(
            job_name, client_id, file_name, status,
            start_time, end_time, error_message, batch_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        job_name, client_id, json_file_name,
        status, start_time, end_time, error_message, batch_id
    ))
    conn.commit()
    cur.close()
    conn.close()

# ----------------------#
# 🎯 Validation Logic   #
# ----------------------#

def validate_json_keys(client_schema, json_file_name):
    start_time = datetime.now()
    job_name = os.path.basename(__file__)
    status = "SUCCESS"
    error_message = None
    batch_id = None

    # Resolve client_id from client_schema
    client_id = get_client_id(client_schema)
    if not client_id:
        print(f"❌ client_schema '{client_schema}' not found in client_reference")
        return False

    json_path = os.path.join('data', client_schema, 'incoming', json_file_name)

    if not os.path.exists(json_path):
        status = "FAILED"
        error_message = f"File not found: {json_path}"
        insert_job_log(job_name, client_id, json_file_name, status, start_time, datetime.now(), error_message)
        print(f"❌ {error_message}")
        return False

    with open(json_path, 'r') as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError as e:
            status = "FAILED"
            error_message = f"Invalid JSON format: {str(e)}"
            insert_job_log(job_name, client_id, json_file_name, status, start_time, datetime.now(), error_message)
            print(f"❌ {error_message}")
            return False

    # Extract metadata
    if isinstance(data, list) and len(data) > 0:
        record = data[0]
    elif isinstance(data, dict):
        record = data
    else:
        status = "FAILED"
        error_message = "JSON format not recognized (not list/dict)"
        insert_job_log(job_name, client_id, json_file_name, status, start_time, datetime.now(), error_message)
        print(f"❌ {error_message}")
        return False

    batch_id = record.get("batch_id")
    logical_source_file = record.get("logical_source_file")

    if not logical_source_file:
        status = "FAILED"
        error_message = "logical_source_file not found in JSON metadata"
        insert_job_log(job_name, client_id, json_file_name, status, start_time, datetime.now(), error_message, batch_id)
        print(f"❌ {error_message}")
        return False

    if not batch_id:
        status = "FAILED"
        error_message = "batch_id not found in JSON metadata"
        insert_job_log(job_name, client_id, json_file_name, status, start_time, datetime.now(), error_message)
        print(f"❌ {error_message}")
        return False

    # Remove metadata keys before validation
    json_keys = set(record.keys()) - {"csv_row_number", "source_file", "logical_source_file", "batch_id"}

    # Get mapping from DB
    mapping_version = get_mapping_version(client_id)
    if not mapping_version:
        status = "FAILED"
        error_message = f"No mapping_version found for client_id '{client_id}'"
        insert_job_log(job_name, client_id, json_file_name, status, start_time, datetime.now(), error_message, batch_id)
        print(f"❌ {error_message}")
        return False

    mapping_keys = get_column_mapping(client_id, mapping_version, logical_source_file)

    missing = mapping_keys - json_keys
    extra = json_keys - mapping_keys

    if missing or extra:
        status = "FAILED"
        error_message = f"Missing: {missing}, Extra: {extra}"
        insert_validation_log(client_id, mapping_keys, json_keys, missing, extra, json_file_name, batch_id)
        update_success = update_file_audit_log(client_id, json_file_name, "FAILED", batch_id)

        if not update_success:
            error_message += " | file_audit_log record not found"

        insert_job_log(job_name, client_id, json_file_name, status, start_time, datetime.now(), error_message, batch_id)
        print("❌ Validation failed. Logged to DB.")
        return False
    else:
        update_file_audit_log(client_id, json_file_name, "SUCCESS", batch_id)
        insert_job_log(job_name, client_id, json_file_name, status, start_time, datetime.now(), None, batch_id)
        print("✅ Validation passed: all JSON keys match column mapping.")
        return True

# ----------------------#
# 🚀 Main Entry Point   #
# ----------------------#

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python validate_mapping.py <client_schema> <json_file_name>")
        sys.exit(1)

    client_schema = sys.argv[1]
    json_file_name = sys.argv[2]

    print(f"🔍 Validating {json_file_name} for client schema '{client_schema}'...\n")
    if not validate_json_keys(client_schema, json_file_name):
        print("🛑 STOP: Loading to bronze is blocked due to mapping mismatch.")
        sys.exit(1)
    else:
        print("✅ PROCEED: Ready for bronze layer load.")
        sys.exit(0)
