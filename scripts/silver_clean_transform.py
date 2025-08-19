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
        raise RuntimeError(
            f"Lebih dari 1 file JSON batch ditemukan di folder {folder_path}, harap hanya ada 1 file."
        )

    file_name = json_files[0]
    file_path = os.path.join(folder_path, file_name)
    with open(file_path, "r") as f:
        data = json.load(f)
    return data, file_name, file_path


def get_client_id(cur, client_schema):
    cur.execute(
        """
        SELECT client_id 
        FROM tools.client_reference 
        WHERE client_schema = %s
        """,
        (client_schema,),
    )
    row = cur.fetchone()
    if not row:
        raise Exception(f"client_schema '{client_schema}' tidak ditemukan di client_reference")
    return row[0]


def get_active_procs(client_id, conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT proc_name
            FROM tools.transformation_config
            WHERE client_id = %s
              AND is_active = true
            ORDER BY transform_id
            """,
            (client_id,),
        )
        rows = cur.fetchall()
        if not rows:
            raise ValueError(f"Tidak ditemukan procedure aktif untuk client_id {client_id}")
        return [row[0] for row in rows]


def insert_job_execution_log(
    conn, job_name, client_id, status, start_time, end_time, error_message, file_name, batch_id
):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO tools.job_execution_log (
                job_name, client_id, status, start_time, end_time, error_message, file_name, batch_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (job_name, client_id, status, start_time, end_time, error_message, file_name, batch_id),
        )
    conn.commit()


def run_procedure(proc_name, client_schema, batch_id, client_id):
    proc_conn = None
    try:
        proc_conn = psycopg2.connect(**DB_CONFIG)
        proc_conn.autocommit = True
        with proc_conn.cursor() as cur:
            print(f"Menjalankan procedure: {proc_name}({client_schema}, {batch_id})")
            cur.execute(
                f"CALL {proc_name}(%s, %s, %s, %s);",
                (client_schema, batch_id, None, None),
            )
            result = cur.fetchone()
            if result:
                is_success, error_message = result
            else:
                is_success, error_message = True, None
            print(f"  is_success: {is_success}")
            print(f"  error_message: {error_message}")
            return is_success, error_message
    except Exception as e:
        return False, str(e)
    finally:
        if proc_conn:
            proc_conn.close()


def update_batch_file_with_procs(file_path, proc_names):
    with open(file_path, "r") as f:
        data = json.load(f)

    if "transformation_procedure" not in data:
        data["transformation_procedure"] = proc_names
    else:
        i = 1
        while f"transformation_procedure_rerun{i}" in data:
            i += 1
        data[f"transformation_procedure_rerun{i}"] = proc_names

    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)


def move_file(src_path, client_schema, status):
    base_folder = os.path.dirname(os.path.dirname(src_path))
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

    global DB_CONFIG
    DB_CONFIG = {
        "host": os.getenv("DB_HOST"),
        "port": int(DB_PORT),
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
    }

    try:
        batch_info, file_name, file_path = load_single_batch_file(client_schema)
    except Exception as e:
        print(f"Error membaca batch file: {e}")
        sys.exit(1)

    batch_id = batch_info.get("batch_id")
    if not batch_id:
        print("batch_id tidak ditemukan di file batch info")
        sys.exit(1)

    job_name = "silver_clean_transform.py"
    start_time = datetime.now()

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cur:
            client_id = get_client_id(cur, client_schema)

        proc_names = get_active_procs(client_id, conn)
        print(f"Procedure aktif untuk client '{client_schema}' (client_id={client_id}): {proc_names}")

        all_success = True
        error_messages = []

        for proc_name in proc_names:
            is_success, error_message = run_procedure(proc_name, client_schema, batch_id, client_id)
            if not is_success:
                all_success = False
                error_messages.append(f"{proc_name} gagal: {error_message}")

        end_time = datetime.now()
        final_error_msg = "\n".join(error_messages) if error_messages else None

        update_batch_file_with_procs(file_path, proc_names)

        if all_success:
            insert_job_execution_log(
                conn, job_name, client_id, "SUCCESS", start_time, end_time, None, file_name, batch_id
            )
            conn.commit()
            move_file(file_path, client_schema, "SUCCESS")
        else:
            insert_job_execution_log(
                conn, job_name, client_id, "FAILED", start_time, end_time, final_error_msg, file_name, batch_id
            )
            conn.commit()
            move_file(file_path, client_schema, "FAILED")

    except Exception as e:
        end_time = datetime.now()
        insert_job_execution_log(
            conn,
            job_name,
            client_id if "client_id" in locals() else None,
            "FAILED",
            start_time,
            end_time,
            str(e),
            file_name,
            batch_id,
        )
        print(f"Error: {e}")
        conn.rollback()
        move_file(file_path, client_schema, "FAILED")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
