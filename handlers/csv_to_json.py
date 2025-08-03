import os
import pandas as pd
import argparse
import json
import shutil

def standardize_file_name(filename: str) -> str:
    """
    Convert 'cust info 20250804.csv' to 'cust_info_20250804.json'
    """
    name, _ = os.path.splitext(filename)
    name = name.strip().lower().replace(" ", "_")
    return name + ".json"

def convert_csv_to_json(client):
    client = client.lower()
    input_dir = os.path.join("raw", client, "incoming")
    archive_dir = os.path.join("raw", client, "archive")
    output_dir = os.path.join("data", client, "incoming")

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(archive_dir, exist_ok=True)

    if not os.path.isdir(input_dir):
        print(f"❌ Folder tidak ditemukan: {input_dir}")
        return

    files = [f for f in os.listdir(input_dir) if f.lower().endswith(".csv")]

    if not files:
        print(f"⚠️ Tidak ada file CSV di {input_dir}")
        return

    for filename in files:
        file_path = os.path.join(input_dir, filename)
        print(f"🔄 Mengubah {filename} ke format JSON...")

        try:
            df = pd.read_csv(file_path, dtype=str).fillna("")
            df.insert(0, "csv_row_number", range(1, len(df) + 1))
            df["source_file"] = filename

            records = df.to_dict(orient="records")

            output_file = standardize_file_name(filename)
            output_path = os.path.join(output_dir, output_file)

            if os.path.exists(output_path):
                print(f"⚠️ File {output_path} sudah ada. Lewati...")
                continue

            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(records, f, ensure_ascii=False, indent=2)

            print(f"✅ Disimpan: {output_path}")

            # Pindahkan file ke arsip
            archive_path = os.path.join(archive_dir, filename)
            shutil.move(file_path, archive_path)
            print(f"📦 Dipindahkan ke arsip: {archive_path}")

        except Exception as e:
            print(f"❌ Gagal memproses {filename}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--client", required=True, help="Nama client, contoh: client1")
    args = parser.parse_args()

    convert_csv_to_json(args.client)
