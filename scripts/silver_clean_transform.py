import os
import sys
import json
import psycopg2
import shutil
from datetime import datetime
from dotenv import load_dotenv

def load_single_batch_file(client_schema):
    folder_path = os.path.join("batch_info", client_schema, "incoming")
    json_files = [f for f in os.listdir(folder_path) if f.lower().endswith('.json')]

    if len(json_files) == 0:
        raise FileNotFoundError(f"Tidak ada file JSON batch di folder {folder_path}")
    if len(json_files) > 1:
        raise RuntimeError(f"Lebih dari 1 file JSON batch ditemukan di folder {folder_path}, harap hanya ada 1 file.")

    file_name = json_files[0]
    file_path = os.path.join(folder_path, file_name)
    with open(file_path, 'r') as f:
        data = json.load(f)
    return data, file_name, file_path

def get_client_id(cur, client_schema):
    cur.execute("""
        SELECT client_id FROM tools.client_reference WHERE client_schema = %s
    """, (client_schema,))
    row = cur.fetchone()
    if not row:
        raise Exception(f"client_schema '{client_schema}' tidak ditemukan di client_reference")
    return row[0]

def get_transform_version(client_id, conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT transform_version
            FROM tools.client_reference
            WHERE client_id = %s
            ORDER BY client_id DESC
            LIMIT 1
        """, (client_id,))
        row = cur.fetchone()
        if row:
            return row[0]
        else:
            raise ValueError(f"transform_version tidak ditemukan untuk client_id {client_id}")

def get_active_procs(client_id, transform_version, conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT proc_name
            FROM tools.transformation_config
            WHERE client_id = %s
              AND transform_version = %s
              AND is_active = true
            ORDER BY transform_id
        """, (client_id, transform_version))
        rows = cur.fetchall()
        if not rows:
            raise ValueError(f"Tidak ditemukan procedure aktif untuk client_id {client_id} dan transform_version {transform_version}")
        return [row[0] for row in rows]

def insert_job_execution_log(conn, job_name, client_id, status, start_time, end_time, error_message, file_name, batch_id):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO tools.job_execution_log (
                job_name, client_id, status, start_time, end_time, error_message, file_name, batch_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (job_name, client_id, status, start_time, end_time, error_message, file_name, batch_id))
    conn.commit()


def run_procedure(proc_name, client_schema, batch_id, client_id, conn):
    with conn.cursor() as cur:
        print(f"Menjalankan procedure: {proc_name}({client_schema}, {batch_id}, {client_id})")
        cur.execute(f"CALL {proc_name}(%s, %s, %s, %s, %s);",
                    (client_schema, batch_id, client_id, None, None))
        result = cur.fetchone()
        if result:
            is_success, error_message = result
            print(f"  is_success: {is_success}")
            print(f"  error_message: {error_message}")
            return is_success, error_message
        else:
            print("  Procedure selesai, tapi tidak ada output OUT params")
            return True, None


def move_file(src_path, client_schema, status):
    # status: "SUCCESS" or "FAILED"
    base_folder = os.path.dirname(os.path.dirname(src_path))  # folder batch_info/<client_schema>/
    target_folder = os.path.join(base_folder, status.lower())
    os.makedirs(target_folder, exist_ok=True)

    file_name = os.path.basename(src_path)
    dest_path = os.path.join(target_folder, file_name)
    shutil.move(src_path, dest_path)
    print(f"File {file_name} dipindah ke {target_folder}")

def main():
    load_dotenv()

    if len(sys.argv) < 2:
        print("Usage: python silver_clean_transform.py <client_schema>")
        sys.exit(1)

    client_schema = sys.argv[1]

    DB_PORT = os.getenv("DB_PORT")
    if DB_PORT is None:
        raise ValueError("DB_PORT not set in .env")

    DB_CONFIG = {
        'host': os.getenv('DB_HOST'),
        'port': int(DB_PORT),
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
    }

    try:
        batch_info, file_name, file_path = load_single_batch_file(client_schema)
    except Exception as e:
        print(f"Error membaca batch file: {e}")
        sys.exit(1)

    batch_id = batch_info.get('batch_id')
    if not batch_id:
        print("batch_id tidak ditemukan di file batch info")
        sys.exit(1)

    job_name = "silver_clean_transform.py"
    start_time = datetime.now()

    conn = psycopg2.connect(**DB_CONFIG)

    try:
        with conn.cursor() as cur:
            client_id = get_client_id(cur, client_schema)

        transform_version = get_transform_version(client_id, conn)
        print(f"Transform version untuk client '{client_schema}' (client_id={client_id}): {transform_version}")

        proc_names = get_active_procs(client_id, transform_version, conn)

        for proc_name in proc_names:
            is_success, error_message = run_procedure(proc_name,client_schema, batch_id, client_id, conn)
            if not is_success:
                end_time = datetime.now()
                log_msg = f"Procedure {proc_name} gagal dengan pesan: {error_message}"
                insert_job_execution_log(conn, job_name, client_id, "FAILED", start_time, end_time, log_msg, file_name, batch_id)
                print(log_msg)
                conn.rollback()
                move_file(file_path, client_schema, "FAILED")
                sys.exit(1)

        end_time = datetime.now()
        insert_job_execution_log(conn, job_name, client_id, "SUCCESS", start_time, end_time, None, file_name, batch_id)

        conn.commit()
        move_file(file_path, client_schema, "SUCCESS")

    except Exception as e:
        end_time = datetime.now()
        insert_job_execution_log(conn, job_name, client_id if 'client_id' in locals() else None, "FAILED", start_time, end_time, str(e), file_name, batch_id)
        print(f"Error: {e}")
        conn.rollback()
        move_file(file_path, client_schema, "FAILED")
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
