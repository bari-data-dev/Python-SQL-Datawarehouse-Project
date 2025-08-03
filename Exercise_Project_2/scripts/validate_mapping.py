import os
import sys
import json
import psycopg2
from datetime import datetime

# ----------------------#
# 🔧 Config DB          #
# ----------------------#
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'exercise_project_2',
    'user': 'postgres',
    'password': 'asepsigh123',
}

# Folder untuk menyimpan file log error
LOG_FOLDER = 'logs/'

# ----------------------#
# 🧠 Helper Functions   #
# ----------------------#

def extract_logical_source_name(file_name):
    """
    Ambil nama logical source dari nama file.
    Contoh: 'cust_info_20250802.json' → 'cust_info'
    """
    name = os.path.splitext(file_name)[0]
    parts = name.split("_")
    if len(parts) > 1 and parts[-1].isdigit() and len(parts[-1]) == 8:
        return "_".join(parts[:-1])
    else:
        return name

def get_active_schema_version(client_schema):
    """
    Ambil schema_version yang aktif untuk client_schema dari tabel client_reference.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    sql = """
    SELECT active_schema_version
    FROM tools.client_reference
    WHERE client_schema = %s
    """
    cur.execute(sql, (client_schema,))
    result = cur.fetchone()
    cur.close()
    conn.close()
    return result[0] if result else None

def get_column_mapping(client_schema, schema_version, logical_source_file):
    """
    Ambil daftar kolom (source_column) dari tabel column_mapping berdasarkan
    client_schema, schema_version, dan source_file (logical).
    """
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    sql = """
    SELECT source_column
    FROM tools.column_mapping
    WHERE client_schema = %s AND schema_version = %s AND source_file = %s
    """
    cur.execute(sql, (client_schema, schema_version, logical_source_file))
    result = cur.fetchall()
    cur.close()
    conn.close()
    return set(r[0] for r in result)

def log_error(client_schema, file_name, message, details):
    """
    Simpan error:
    - ke dalam tabel tools.error_log
    - ke file log lokal di folder logs/<client_schema>/
    """
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    now = datetime.now()
    insert_sql = """
    INSERT INTO tools.error_log(client_schema, error_type, error_detail, file_name, row_number, timestamp)
    VALUES (%s, %s, %s, %s, NULL, %s)
    """
    cur.execute(insert_sql, (client_schema, "Mapping Validation", details, file_name, now))
    conn.commit()
    cur.close()
    conn.close()

    # Buat subfolder berdasarkan client_schema jika belum ada
    client_log_folder = os.path.join(LOG_FOLDER, client_schema)
    os.makedirs(client_log_folder, exist_ok=True)

    # Simpan ke file log
    log_file = os.path.join(
        client_log_folder,
        f"error_{client_schema}_{now.strftime('%Y%m%d_%H%M%S')}.log"
    )

    with open(log_file, 'w') as f:
        f.write(f"Time: {now}\n")
        f.write(f"Client Schema: {client_schema}\n")
        f.write(f"File: {file_name}\n")
        f.write(f"Error Type: Mapping Validation\n")
        f.write(f"Details: {details}\n")

def validate_json_keys(client_schema, json_file_name):
    """
    Fungsi utama untuk validasi struktur key dari JSON terhadap metadata column_mapping.
    """
    json_path = os.path.join('data', client_schema, 'incoming', json_file_name)

    if not os.path.exists(json_path):
        print(f"❌ File not found: {json_path}")
        return False

    # Baca JSON
    with open(json_path, 'r') as f:
        data = json.load(f)

    # Ambil keys dari JSON
    if isinstance(data, list):
        json_keys = set(data[0].keys()) if data else set()
    elif isinstance(data, dict):
        json_keys = set(data.keys())
    else:
        json_keys = set()

    # Hilangkan kolom tambahan (tidak perlu divalidasi)
    json_keys.discard("csv_row_number")
    json_keys.discard("source_file")

    # Ambil schema version aktif
    schema_version = get_active_schema_version(client_schema)
    if not schema_version:
        print(f"❌ No active schema_version found for client schema '{client_schema}'")
        return False

    # Ambil logical source name dari nama file
    logical_source_file = extract_logical_source_name(json_file_name)

    # Ambil key yang valid dari metadata
    mapping_keys = get_column_mapping(client_schema, schema_version, logical_source_file)

    # Validasi: apakah ada key yang tidak dikenali?
    unknown_keys = json_keys - mapping_keys

    if unknown_keys:
        message = f"Unknown keys in JSON not found in mapping: {unknown_keys}"
        log_error(client_schema, json_file_name, message, str(unknown_keys))
        print("❌ Validation failed. Logged to DB and file.")
        return False
    else:
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
    else:
        print("✅ PROCEED: Ready for bronze layer load.")
