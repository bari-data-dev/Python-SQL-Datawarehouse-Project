import os
import sys
import shutil
import subprocess
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
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

# Logging ke job_execution_log
def log_job(client_schema, file_name, status, job_name, batch_id=None, error_message=None, start_time=None):
    conn = get_connection()
    cur = conn.cursor()
    now = datetime.now()
    
    cur.execute("""
        INSERT INTO tools.job_execution_log (
            job_name, client_schema, status,
            start_time, end_time, error_message,
            file_name, batch_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        job_name,
        client_schema,
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

def main():
    if len(sys.argv) < 2:
        print("Usage: python rerun_batch.py <client_schema>")
        sys.exit(1)

    client_schema = sys.argv[1]
    incoming_dir = f"data/{client_schema}/incoming"
    failed_dir = f"data/{client_schema}/failed"
    archive_dir = f"data/{client_schema}/archive"

    os.makedirs(failed_dir, exist_ok=True)
    os.makedirs(archive_dir, exist_ok=True)

    files = [f for f in os.listdir(incoming_dir) if f.endswith(".json")]
    if not files:
        print(f"[{client_schema}] Tidak ada file untuk diproses.")
        return

    for json_file in files:
        start_time = datetime.now()
        final_status = "SUCCESS"
        final_reason = None
        batch_id = None

        try:
            # Step 1: Validate Mapping
            result = subprocess.run(["python", "scripts/validate_mapping.py", client_schema, json_file])
            if result.returncode != 0:
                shutil.move(f"{incoming_dir}/{json_file}", f"{failed_dir}/{json_file}")
                final_status = "FAILED"
                final_reason = "FAILED on validate_mapping"
                raise Exception(final_reason)

            # Step 2: Validate Required Columns
            result = subprocess.run(["python", "scripts/validate_req_cols.py", client_schema, json_file])
            if result.returncode != 0:
                shutil.move(f"{incoming_dir}/{json_file}", f"{failed_dir}/{json_file}")
                final_status = "FAILED"
                final_reason = "FAILED on validate_req_cols"
                raise Exception(final_reason)

            # Step 3: Load to Bronze
            result = subprocess.run(["python", "scripts/load_to_bronze.py", client_schema, json_file])
            if result.returncode != 0:
                shutil.move(f"{incoming_dir}/{json_file}", f"{failed_dir}/{json_file}")
                final_status = "FAILED"
                final_reason = "FAILED on load_to_bronze"
                raise Exception(final_reason)

            # ✅ Jika berhasil semua, pindah ke archive
            shutil.move(f"{incoming_dir}/{json_file}", f"{archive_dir}/{json_file}")

        except Exception as e:
            print(f"❌ [{client_schema}] Gagal memproses {json_file} => {e}")

        # Ambil batch_id dari nama file kalau ada
        if "_BATCH" in json_file:
            batch_id = json_file.split("_BATCH")[-1].replace(".json", "")
            batch_id = "BATCH" + batch_id

        # Log untuk setiap file
        log_job(
            client_schema=client_schema,
            file_name=json_file,
            status=final_status,
            job_name="rerun_batch",
            batch_id=batch_id,
            error_message=final_reason,
            start_time=start_time
        )

if __name__ == "__main__":
    main()
