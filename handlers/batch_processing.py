import os
import sys
import shutil
import subprocess
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

# Load .env
load_dotenv()

# DB Connection
def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT")
    )

# Generate next batch ID
def increment_batch_id(batch_id):
    prefix = batch_id[:-6]
    number = int(batch_id[-6:])
    new_number = number + 1
    return f"{prefix}{new_number:06d}"

# Rename dan tambahkan batch ID ke nama file
def add_batch_id_to_files(client, batch_id):
    incoming_dir = f"raw/{client}/incoming"
    updated_files = []

    for filename in os.listdir(incoming_dir):
        full_path = os.path.join(incoming_dir, filename)
        if os.path.isfile(full_path):
            base, ext = os.path.splitext(filename)
            base_std = base.replace(" ", "_").replace("-", "_")  # standar nama file
            new_filename = f"{base_std}_{batch_id}{ext}"
            new_path = os.path.join(incoming_dir, new_filename)
            os.rename(full_path, new_path)
            updated_files.append(new_filename)
    
    return updated_files

# Insert ke job_execution_log
def log_batch_processing(client, status, file_name, batch_id, error_message=None, start_time=None):
    conn = get_connection()
    cur = conn.cursor()
    now = datetime.now()

    cur.execute("""
        INSERT INTO tools.job_execution_log (job_name, client_schema, status, start_time, end_time, error_message, file_name, batch_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        "batch_processing",
        client,
        status,
        start_time or now,
        now,
        error_message,
        file_name,
        batch_id
    ))
    conn.commit()
    cur.close()
    conn.close()

# Main orchestrator
def main():
    conn = get_connection()
    cur = conn.cursor()
    
    # Ambil semua client
    cur.execute("SELECT client_schema, last_batch_id FROM tools.client_reference")
    clients = cur.fetchall()

    for client_schema, last_batch_id in clients:
        try:
            new_batch_id = increment_batch_id(last_batch_id)
            files = add_batch_id_to_files(client_schema, new_batch_id)

            if not files:
                print(f"[{client_schema}] Tidak ada file untuk diproses.")
                continue

            # Update batch_id
            cur.execute("""
                UPDATE tools.client_reference 
                SET last_batch_id = %s 
                WHERE client_schema = %s
            """, (new_batch_id, client_schema))
            conn.commit()

            for file_name in files:
                final_status = "SUCCESS"
                final_reason = None

                try:
                    # Step 1: CSV to JSON
                    result = subprocess.run(["python", "handlers/csv_to_json.py", client_schema, file_name])
                    if result.returncode != 0:
                        shutil.move(f"raw/{client_schema}/incoming/{file_name}", f"raw/{client_schema}/failed/{file_name}")
                        final_status = "FAILED"
                        final_reason = "FAILED on csv_to_json"
                        raise Exception(final_reason)

                    # ✅ Pindah file CSV ke archive
                    shutil.move(f"raw/{client_schema}/incoming/{file_name}", f"raw/{client_schema}/archive/{file_name}")
                    
                    # Ganti ekstensi ke .json
                    json_file = file_name.replace(".csv", ".json")

                    # Step 2: Validate Mapping
                    result = subprocess.run(["python", "scripts/validate_mapping.py", client_schema, json_file])
                    if result.returncode != 0:
                        shutil.move(f"data/{client_schema}/incoming/{json_file}", f"data/{client_schema}/failed/{json_file}")
                        final_status = "FAILED"
                        final_reason = "FAILED on validate_mapping"
                        raise Exception(final_reason)

                    # Step 3: Validate Required Column
                    result = subprocess.run(["python", "scripts/validate_req_cols.py", client_schema, json_file])
                    if result.returncode != 0:
                        shutil.move(f"data/{client_schema}/incoming/{json_file}", f"data/{client_schema}/failed/{json_file}")
                        final_status = "FAILED"
                        final_reason = "FAILED on validate_req_cols"
                        raise Exception(final_reason)

                    # Step 4: Load to Bronze
                    result = subprocess.run(["python", "scripts/load_to_bronze.py", client_schema, json_file])
                    if result.returncode != 0:
                        shutil.move(f"data/{client_schema}/incoming/{json_file}", f"data/{client_schema}/failed/{json_file}")
                        final_status = "FAILED"
                        final_reason = "FAILED on load_to_bronze"
                        raise Exception(final_reason)

                    # ✅ Semua berhasil, pindahkan JSON ke archive
                    shutil.move(f"data/{client_schema}/incoming/{json_file}", f"data/{client_schema}/archive/{json_file}")

                except Exception as e:
                    # Semua kesalahan terperangkap di sini
                    print(f"❌ [{client_schema}] Gagal memproses {file_name} => {e}")
                    final_status = "FAILED"
                    if not final_reason:
                        final_reason = f"FAILED on batch_cycle: {str(e)}"

                # ✅ Simpan ke job_execution_log (hanya 1 record per file)
                log_batch_processing(client_schema, final_status, file_name, new_batch_id, error_message=final_reason)

        except Exception as e:
            print(f"❌ Error besar di client {client_schema}: {e}")
            continue

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
