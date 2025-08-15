import os
import sys
import shutil
import subprocess
import psycopg2
import json
from datetime import datetime
from dotenv import load_dotenv

# =============================
# Load .env
# =============================
load_dotenv()

# =============================
# DB Connection
# =============================
def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT")
    )

# =============================
# Generate next batch ID
# =============================
def increment_batch_id(batch_id):
    prefix = batch_id[:-6]
    number = int(batch_id[-6:])
    new_number = number + 1
    return f"{prefix}{new_number:06d}"

# =============================
# Rename dan tambahkan batch ID ke nama file
# =============================
def add_batch_id_to_files(client_schema, batch_id):
    incoming_dir = f"raw/{client_schema}/incoming"
    updated_files = []

    for filename in os.listdir(incoming_dir):
        full_path = os.path.join(incoming_dir, filename)
        if os.path.isfile(full_path):
            base, ext = os.path.splitext(filename)
            base_std = base.replace(" ", "_").replace("-", "_")
            new_filename = f"{base_std}_{batch_id}{ext}"
            new_path = os.path.join(incoming_dir, new_filename)
            os.rename(full_path, new_path)
            updated_files.append(new_filename)

    return updated_files

# =============================
# Ambil client_id dari client_schema
# =============================
def get_client_id(cur, client_schema):
    cur.execute("""
        SELECT client_id FROM tools.client_reference WHERE client_schema = %s
    """, (client_schema,))
    row = cur.fetchone()
    if not row:
        raise Exception(f"client_schema '{client_schema}' tidak ditemukan di client_reference")
    return row[0]

# =============================
# Insert satu log untuk seluruh batch
# =============================
def log_batch_status(client_id, status, batch_id, error_message=None, start_time=None):
    conn = get_connection()
    cur = conn.cursor()
    now = datetime.now()

    cur.execute("""
        INSERT INTO tools.job_execution_log (job_name, client_id, status, start_time, end_time, error_message, file_name, batch_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        "batch_processing",
        client_id,
        status,
        start_time or now,
        now,
        error_message,
        None,
        batch_id
    ))
    conn.commit()
    cur.close()
    conn.close()

# =============================
# Proses 1 client
# =============================
def process_client(client_schema, mode):
    conn = get_connection()
    cur = conn.cursor()

    try:
        # Ambil client_id
        client_id = get_client_id(cur, client_schema)

        # Pastikan direktori batch_info/<client> lengkap
        os.makedirs(f"batch_info/{client_schema}/incoming", exist_ok=True)
        os.makedirs(f"batch_info/{client_schema}/archive", exist_ok=True)
        os.makedirs(f"batch_info/{client_schema}/failed", exist_ok=True)

        # Ambil last_batch_id
        cur.execute("SELECT last_batch_id FROM tools.client_reference WHERE client_schema = %s", (client_schema,))
        result = cur.fetchone()
        if not result:
            print(f"❌ Client '{client_schema}' tidak ditemukan di client_reference.")
            return
        last_batch_id = result[0]

        # Jika start, generate batch ID dan rename file
        if mode == "start":
            new_batch_id = increment_batch_id(last_batch_id)
            files = add_batch_id_to_files(client_schema, new_batch_id)

            if not files:
                print(f"[{client_schema}] Tidak ada file untuk diproses.")
                return

            # Update batch_id di client_reference
            cur.execute("""
                UPDATE tools.client_reference 
                SET last_batch_id = %s 
                WHERE client_schema = %s
            """, (new_batch_id, client_schema))
            conn.commit()
        else:
            # Mode rerun → gunakan batch_id terakhir, ambil file JSON dari data/<client>/incoming/
            new_batch_id = last_batch_id
            data_dir = f"data/{client_schema}/incoming"
            files = [f.replace(".json", ".csv") for f in os.listdir(data_dir) if f.endswith(".json")]

            if not files:
                print(f"[{client_schema}] Tidak ada file JSON untuk di-rerun.")
                return

        # Tandai waktu mulai batch
        batch_start = datetime.now()
        batch_status = "SUCCESS"
        batch_error_message = None
        success_files = []

        # =============================
        # Proses setiap file
        # =============================
        for file_name in files:
            try:
                json_file = file_name.replace(".csv", ".json")

                if mode == "start":
                    # Step 1: CSV to JSON
                    result = subprocess.run(["python", "handlers/csv_to_json.py", client_schema, file_name])
                    if result.returncode != 0:
                        shutil.move(f"raw/{client_schema}/incoming/{file_name}", f"raw/{client_schema}/failed/{file_name}")
                        raise Exception("FAILED on csv_to_json")

                    # Pindah file CSV ke archive
                    shutil.move(f"raw/{client_schema}/incoming/{file_name}", f"raw/{client_schema}/archive/{file_name}")

                # Step 2: Validate Mapping
                result = subprocess.run(["python", "scripts/validate_mapping.py", client_schema, json_file])
                if result.returncode != 0:
                    shutil.move(f"data/{client_schema}/incoming/{json_file}", f"data/{client_schema}/failed/{json_file}")
                    raise Exception("FAILED on validate_mapping")

                # Step 3: Validate Required Columns
                result = subprocess.run(["python", "scripts/validate_req_cols.py", client_schema, json_file])
                if result.returncode != 0:
                    shutil.move(f"data/{client_schema}/incoming/{json_file}", f"data/{client_schema}/failed/{json_file}")
                    raise Exception("FAILED on validate_req_cols")

                # Step 4: Load to Bronze
                result = subprocess.run(["python", "scripts/load_to_bronze.py", client_schema, json_file])
                if result.returncode != 0:
                    shutil.move(f"data/{client_schema}/incoming/{json_file}", f"data/{client_schema}/failed/{json_file}")
                    raise Exception("FAILED on load_to_bronze")

                # Pindah JSON ke archive
                shutil.move(f"data/{client_schema}/incoming/{json_file}", f"data/{client_schema}/archive/{json_file}")

                # ✅ Tambahkan ke list sukses
                success_files.append(file_name)

            except Exception as e:
                print(f"❌ [{client_schema}] Gagal memproses {file_name} => {e}")
                batch_status = "FAILED"
                batch_error_message = f"{file_name}"

        # ✅ Log ke job_execution_log
        log_batch_status(
            client_id=client_id,
            status=batch_status,
            batch_id=new_batch_id,
            error_message=batch_error_message,
            start_time=batch_start
        )

        # ✅ Simpan file output sukses
        if success_files:
            archive_dir = f"batch_info/{client_schema}/incoming"
            os.makedirs(archive_dir, exist_ok=True)
            output_file = os.path.join(archive_dir, f"batch_output_{client_schema}_{new_batch_id}.json")

            output_data = {
                "client_schema": client_schema,
                "batch_id": new_batch_id,
                "files": []
            }

            # Cek apakah file output sebelumnya sudah ada
            if os.path.exists(output_file):
                with open(output_file, "r") as f:
                    try:
                        existing_data = json.load(f)
                        if isinstance(existing_data, dict) and "files" in existing_data:
                            output_data["files"] = existing_data["files"]
                    except json.JSONDecodeError:
                        pass  # kalau rusak atau kosong, mulai dari nol

            # Tambahkan file baru tanpa duplikat
            for f in success_files:
                if f not in output_data["files"]:
                    output_data["files"].append(f)

            with open(output_file, "w") as f:
                json.dump(output_data, f, indent=2)

    finally:
        cur.close()
        conn.close()

# =============================
# Main
# =============================
def main():
    args = sys.argv[1:]

    if len(args) == 2:
        client, mode = args[0], args[1]
        if mode not in ("start", "rerun"):
            print("Gunakan mode: start atau rerun")
            return
        process_client(client, mode)

    elif len(args) == 0:
        # Mode default: semua client dijalankan dari awal
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT client_schema FROM tools.client_reference")
        clients = cur.fetchall()
        cur.close()
        conn.close()

        for (client_schema,) in clients:
            process_client(client_schema, "start")

    else:
        print("Format: python batch_processing.py [client] [start|rerun]")

if __name__ == "__main__":
    main()
