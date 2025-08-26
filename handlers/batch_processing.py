import os
import sys
import shutil
import subprocess
import psycopg2
import json
import getpass
import tempfile
import time
from datetime import datetime
from dotenv import load_dotenv

# -----------------------------
# DB connection
# -----------------------------
load_dotenv()

# validate DB_PORT presence and cast to int
db_port = os.getenv("DB_PORT")
if db_port is None:
    raise ValueError("DB_PORT not set in .env")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(db_port),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}


def get_connection():
    """
    Return a new psycopg2 connection using DB_CONFIG.
    Caller is responsible to close the connection (conn.close()).
    """
    missing = [k for k, v in DB_CONFIG.items() if v in (None, "")]
    if missing:
        raise ValueError(f"Missing DB config values: {missing}")
    return psycopg2.connect(**DB_CONFIG)


# -----------------------------
# Utilities
# -----------------------------
BATCH_SUFFIX_PREFIX = "_BATCH"


def increment_batch_id(batch_id):
    prefix = batch_id[:-6] if batch_id and len(batch_id) > 6 else "BATCH"
    try:
        number = int(batch_id[-6:]) if batch_id and len(batch_id) >= 6 else 0
    except Exception:
        number = 0
    new_number = number + 1
    return f"{prefix}{new_number:06d}"


def normalize_name(s: str):
    if s is None:
        return ""
    base = os.path.splitext(s)[0]
    return base.strip().lower().replace(" ", "_").replace("-", "_")


def strip_batch_suffix(filename: str) -> str:
    """Remove trailing _BATCH###### before extension, if present."""
    if not filename:
        return filename
    name, ext = os.path.splitext(filename)
    up = name.upper()
    # exact _BATCH (unlikely) or _BATCHNNNNNN
    if up.endswith(BATCH_SUFFIX_PREFIX) and len(name) >= len(BATCH_SUFFIX_PREFIX) + 6:
        return name[: -len(BATCH_SUFFIX_PREFIX)] + ext
    if "_BATCH" in up:
        base, suf = name.rsplit("_", 1)
        if suf.upper().startswith("BATCH") and len(suf) >= 11 and suf[5:].isdigit():
            return base + ext
    return filename


def ensure_raw_dirs(client_schema, source_system):
    base = f"raw/{client_schema}/{source_system}"
    for kind in ("incoming", "success", "failed", "archive"):
        os.makedirs(os.path.join(base, kind), exist_ok=True)
    # data dir for parquet (handlers manage content)
    os.makedirs(f"data/{client_schema}/{source_system}/incoming", exist_ok=True)
    os.makedirs(f"data/{client_schema}/incoming", exist_ok=True)


def write_json_atomic(path, data):
    dirn = os.path.dirname(path)
    os.makedirs(dirn, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp_batchinfo_", dir=dirn, text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except Exception:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass
        raise


def read_json_retry(path, retries: int = 10, delay: float = 0.15):
    """
    Read JSON file with a few retries to handle transient OS-level locks (especially on Windows).
    Returns the parsed JSON (could be any JSON type). Caller should validate type if needed.
    """
    last_exc = None
    for i in range(retries):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            last_exc = e
            if i < retries - 1:
                time.sleep(delay)
                continue
            raise last_exc


def wait_for_parquet_name(
    batch_info_path, physical_file_name, timeout=30.0, poll_interval=0.25
):
    """
    Poll batch_info until file entry has 'parquet_name' key populated for given physical_file_name.
    Returns True if found within timeout, False otherwise.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            bi = read_json_retry(batch_info_path, retries=3, delay=0.05)
        except Exception:
            # wait and retry polling
            time.sleep(poll_interval)
            continue
        # ensure bi is a dict
        if not isinstance(bi, dict):
            time.sleep(poll_interval)
            continue
        files = bi.get("files", []) or []
        for f in files:
            if f.get("physical_file_name") == physical_file_name:
                if f.get("parquet_name"):
                    return True
                break
        time.sleep(poll_interval)
    return False


# -----------------------------
# Merge-on-read helper (fix for parquet_name overwrite)
# -----------------------------
def upsert_file_entry_batch_info(
    batch_info_path, new_entry, max_attempts=6, delay=0.08
):
    """
    Merge-on-read upsert that:
     - does NOT inject batch-level metadata into each file entry,
     - preserves existing parquet_name if present,
     - only updates allowed file-level keys.
    """
    allowed_keys = {
        "physical_file_name",
        "logical_source_file",
        "source_system",
        "source_type",
        "target_schema",
        "target_table",
        "source_config",
        "parquet_name",
    }

    # ensure new_entry contains only allowed keys (defensive)
    clean_entry = {k: v for k, v in new_entry.items() if k in allowed_keys}

    for attempt in range(max_attempts):
        try:
            if os.path.exists(batch_info_path):
                current = read_json_retry(batch_info_path)
            else:
                # create top-level skeleton, but DO NOT include batch fields in file entries
                current = {
                    "client_schema": None,
                    "client_id": None,
                    "batch_id": None,
                    "files": [],
                }
        except Exception:
            current = {
                "client_schema": None,
                "client_id": None,
                "batch_id": None,
                "files": [],
            }

        if not isinstance(current, dict):
            current = {
                "client_schema": None,
                "client_id": None,
                "batch_id": None,
                "files": [],
            }

        files = current.setdefault("files", [])
        found = False
        for f in files:
            if str(f.get("physical_file_name")) == str(
                clean_entry.get("physical_file_name")
            ):
                existing_parquet = f.get("parquet_name", None)
                # update only allowed keys
                for k, v in clean_entry.items():
                    # If parquet_name is None in clean_entry, don't overwrite existing non-null value
                    if k == "parquet_name" and (v is None) and existing_parquet:
                        continue
                    f[k] = v
                found = True
                break

        if not found:
            files.append(clean_entry)

        # Do NOT overwrite top-level client_schema/client_id/batch_id if present in current.
        # (we only update files section here)
        try:
            write_json_atomic(batch_info_path, current)
            return True
        except Exception:
            time.sleep(delay * (1 + attempt * 0.3))
            continue

    return False


# -----------------------------
# DB helpers
# -----------------------------


def get_client_info(cur, client_schema):
    cur.execute(
        "SELECT client_id, last_batch_id FROM tools.client_reference WHERE client_schema = %s",
        (client_schema,),
    )
    row = cur.fetchone()
    if not row:
        raise Exception(
            f"client_schema '{client_schema}' tidak ditemukan di client_reference"
        )
    return {"client_id": row[0], "last_batch_id": row[1]}


def find_client_configs(cur, client_id):
    cur.execute(
        """
        SELECT config_id, source_type, target_schema, target_table, source_config, logical_source_file, source_system
        FROM tools.client_config
        WHERE client_id = %s AND is_active = true
        """,
        (client_id,),
    )
    rows = cur.fetchall()
    configs = []
    for r in rows:
        configs.append(
            {
                "config_id": r[0],
                "source_type": (r[1] or "").lower(),
                "target_schema": r[2],
                "target_table": r[3],
                "source_config": r[4],
                "logical_source_file": r[5],
                "source_system": r[6],
                "logical_norm": normalize_name(r[5]) if r[5] else None,
            }
        )
    return configs


def insert_file_audit(cur, conn, rec):
    """
    Write into tools.file_audit_log.
    The table has many columns; we explicitly set relevant columns and leave others NULL.
    """
    cur.execute(
        """
        INSERT INTO tools.file_audit_log
        (convert_status, mapping_validation_status, row_validation_status, load_status, total_rows, valid_rows, invalid_rows,
         processed_by, logical_source_file, physical_file_name, batch_id, file_received_time, source_type, source_system, config_validation_status, client_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            rec.get("convert_status"),
            rec.get("mapping_validation_status"),
            rec.get("row_validation_status"),
            rec.get("load_status"),
            rec.get("total_rows"),
            rec.get("valid_rows"),
            rec.get("invalid_rows"),
            rec.get("processed_by"),
            rec.get("logical_source_file"),
            rec.get("physical_file_name"),
            rec.get("batch_id"),
            rec.get("file_received_time"),
            rec.get("source_type"),
            rec.get("source_system"),
            rec.get("config_validation_status"),
            rec.get("client_id"),
        ),
    )
    conn.commit()


def log_batch_status(
    client_id, status, batch_id, job_name, error_message=None, start_time=None
):
    conn = get_connection()
    cur = conn.cursor()
    now = datetime.now()

    cur.execute(
        """
        INSERT INTO tools.job_execution_log 
            (job_name, client_id, status, start_time, end_time, error_message, file_name, batch_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            job_name,
            client_id,
            status,
            start_time or now,
            now,
            error_message,
            None,
            batch_id,
        ),
    )
    conn.commit()
    cur.close()
    conn.close()


# -----------------------------
# Core process
# -----------------------------


def process_client(client_schema, mode):
    conn = get_connection()
    cur = conn.cursor()
    try:
        info = get_client_info(cur, client_schema)
        client_id = info["client_id"]
        last_batch = info["last_batch_id"] or "BATCH000000"
        configs = find_client_configs(cur, client_id)
        source_systems = ["crm", "erp", "api", "db"]
        for ss in source_systems:
            ensure_raw_dirs(client_schema, ss)

        new_batch_id = last_batch
        files_to_handle = []

        # Determine job_name early
        if mode == "start":
            job_name = "Batch Processing Start"
        elif mode == "restart":
            job_name = "Batch Processing Restart"
        elif mode == "reprocessing":
            job_name = "Batch Reprocessing"
        else:
            print("Mode tidak dikenali. Gunakan start | restart | reprocessing")
            return

        # START: increment batch_id and update
        if mode == "start":
            new_batch_id = increment_batch_id(last_batch)
            cur.execute(
                "UPDATE tools.client_reference SET last_batch_id = %s WHERE client_schema = %s",
                (new_batch_id, client_schema),
            )
            conn.commit()

            # scan incoming in raw for all source_systems
            for ss in source_systems:
                incoming = f"raw/{client_schema}/{ss}/incoming"
                if not os.path.isdir(incoming):
                    continue
                for fn in os.listdir(incoming):
                    path = os.path.join(incoming, fn)
                    if not os.path.isfile(path):
                        continue
                    ext = os.path.splitext(fn)[1].lower().lstrip(".")
                    logical_norm = normalize_name(strip_batch_suffix(fn))
                    matched = None
                    for cfg in configs:
                        if (
                            (cfg["source_system"] or "").lower() == ss.lower()
                            and cfg["source_type"] == ext
                            and cfg["logical_norm"] == logical_norm
                        ):
                            matched = cfg
                            break
                    if matched:
                        files_to_handle.append(
                            {
                                "orig_path": path,
                                "orig_name": fn,
                                "ss": ss,
                                "ext": ext,
                                "cfg": matched,
                            }
                        )
                    else:
                        # record audit for non-matching file (config failed) — rename with batch suffix then move to failed
                        base, e = os.path.splitext(fn)
                        base_std = base.strip().replace(" ", "_").replace("-", "_")
                        new_name = f"{base_std}_{new_batch_id}{e}"
                        raw_failed = f"raw/{client_schema}/{ss}/failed"
                        os.makedirs(raw_failed, exist_ok=True)
                        new_full = os.path.join(incoming, new_name)
                        try:
                            if os.path.exists(new_full):
                                os.remove(new_full)
                            os.rename(path, new_full)
                            shutil.move(new_full, os.path.join(raw_failed, new_name))
                            phys_to_record = new_name
                        except Exception:
                            phys_to_record = fn
                        audit_rec = {
                            "client_id": client_id,
                            "processed_by": os.getenv("PROCESS_USER")
                            or getpass.getuser()
                            or "autoloader",
                            "logical_source_file": None,
                            "physical_file_name": phys_to_record,
                            "batch_id": new_batch_id,
                            "file_received_time": datetime.now(),
                            "source_type": ext if ext else None,
                            "source_system": ss,
                            "config_validation_status": "FAILED",
                        }
                        try:
                            insert_file_audit(cur, conn, audit_rec)
                        except Exception as e:
                            print(
                                f"[{client_schema}] Gagal insert audit untuk no-match file {fn}: {e}"
                            )
                        print(
                            f"[{client_schema}][{ss}] SKIP no config match: {fn} -> recorded as {audit_rec['physical_file_name']}"
                        )

            if not files_to_handle:
                # Always create batch_info top-level even if there is no matching file
                batch_info_dir = f"batch_info/{client_schema}/incoming"
                os.makedirs(batch_info_dir, exist_ok=True)
                batch_info_path = os.path.join(
                    batch_info_dir, f"batch_output_{client_schema}_{new_batch_id}.json"
                )
                skeleton = {
                    "client_schema": client_schema,
                    "client_id": client_id,
                    "batch_id": new_batch_id,
                    "files": [],
                }
                try:
                    write_json_atomic(batch_info_path, skeleton)
                except Exception as e:
                    batch_start = datetime.now()
                    log_batch_status(
                        client_id=client_id,
                        status="FAILED",
                        batch_id=new_batch_id,
                        job_name=job_name,
                        error_message=f"BATCHINFO_WRITE_FAILED: {e}",
                        start_time=batch_start,
                    )
                    print(f"[{client_schema}] Gagal menulis batch_info: {e}. Batal.")
                    return

                batch_start = datetime.now()
                log_batch_status(
                    client_id=client_id,
                    status="FAILED",
                    batch_id=new_batch_id,
                    job_name=job_name,
                    error_message="FILE CONFIG NOT FOUND",
                    start_time=batch_start,
                )
                print(
                    f"[{client_schema}] Tidak ada file yang match config. Batch info dibuat: {batch_info_path}"
                )
                return

        # RESTART: update-only flow (strictly update existing audit rows; do not insert new ones)
        elif mode == "restart":
            new_batch_id = last_batch

            # ensure batch_info exists in incoming for this batch
            batch_info_path = f"batch_info/{client_schema}/incoming/batch_output_{client_schema}_{new_batch_id}.json"
            if not os.path.exists(batch_info_path):
                batch_start = datetime.now()
                log_batch_status(
                    client_id=client_id,
                    status="FAILED",
                    batch_id=new_batch_id,
                    job_name=job_name,
                    error_message=f"NO_BATCH_INFO_FOR_{new_batch_id}",
                    start_time=batch_start,
                )
                print(
                    f"[{client_schema}] Tidak ada batch_info untuk batch {new_batch_id}. Pindahkan batch_info ke batch_info/{client_schema}/incoming lalu jalankan ulang restart."
                )
                return

            try:
                batch_info = read_json_retry(batch_info_path)
                if not isinstance(batch_info, dict):
                    raise ValueError("batch_info is not an object")
            except Exception:
                batch_start = datetime.now()
                log_batch_status(
                    client_id=client_id,
                    status="FAILED",
                    batch_id=new_batch_id,
                    job_name=job_name,
                    error_message=f"INVALID_BATCH_INFO_{new_batch_id}",
                    start_time=batch_start,
                )
                print(f"[{client_schema}] batch_info tidak valid. Batal restart.")
                return

            # Prepare a helper to match config using physical file name WITHOUT batch suffix
            def match_config_for_physical(
                physical_file_name, source_system, source_type
            ):
                phys_norm = normalize_name(strip_batch_suffix(physical_file_name))
                for cfg_item in configs:
                    if (
                        (cfg_item.get("logical_norm") or "") == phys_norm
                        and (cfg_item.get("source_system") or "").lower()
                        == (source_system or "").lower()
                        and (cfg_item.get("source_type") or "") == (source_type or "")
                    ):
                        return cfg_item
                return None

            # helper: fetch existing audit row (by client_id, batch_id, physical_file_name ONLY) and update only config_validation_status + logical_source_file
            def fetch_and_update_file_audit(
                client_id, batch_id, physical_file_name, source_system, source_type
            ):
                try:
                    cur.execute(
                        """
                        SELECT logical_source_file, source_system, source_type
                        FROM tools.file_audit_log
                        WHERE client_id = %s AND batch_id = %s AND physical_file_name = %s
                        ORDER BY file_received_time DESC NULLS LAST, ctid DESC
                        LIMIT 1
                        """,
                        (client_id, batch_id, physical_file_name),
                    )
                    row = cur.fetchone()
                    if not row:
                        return (False, None, None)
                    logical_sf_db, ss_db, st_db = row

                    # decide config match
                    matched_cfg = match_config_for_physical(
                        physical_file_name,
                        source_system or ss_db,
                        source_type or (st_db or ""),
                    )
                    new_status = "SUCCESS" if matched_cfg else "FAILED"

                    if matched_cfg and not logical_sf_db:
                        cur.execute(
                            """
                            UPDATE tools.file_audit_log
                            SET logical_source_file = %s, config_validation_status = %s
                            WHERE client_id = %s AND batch_id = %s AND physical_file_name = %s
                            """,
                            (
                                matched_cfg.get("logical_source_file"),
                                new_status,
                                client_id,
                                batch_id,
                                physical_file_name,
                            ),
                        )
                    else:
                        cur.execute(
                            """
                            UPDATE tools.file_audit_log
                            SET config_validation_status = %s
                            WHERE client_id = %s AND batch_id = %s AND physical_file_name = %s
                            """,
                            (
                                new_status,
                                client_id,
                                batch_id,
                                physical_file_name,
                            ),
                        )
                    conn.commit()

                    # logical_sf after update
                    logical_sf_new = logical_sf_db or (
                        matched_cfg.get("logical_source_file") if matched_cfg else None
                    )
                    return (True, logical_sf_new, matched_cfg)
                except Exception:
                    conn.rollback()
                    raise

            # Build candidate list from batch_info.files (preferred)
            candidates = []
            for f in batch_info.get("files") or []:
                if not isinstance(f, dict):
                    continue
                phys = f.get("physical_file_name")
                ss = f.get("source_system")
                st = f.get("source_type")
                if phys:
                    candidates.append(
                        {
                            "physical_file_name": phys,
                            "source_system": ss,
                            "source_type": st,
                        }
                    )

            # fallback: scan raw incoming for files (these files should already have batch suffix from start)
            if not candidates:
                for ss in source_systems:
                    incoming = f"raw/{client_schema}/{ss}/incoming"
                    if not os.path.isdir(incoming):
                        continue
                    for fn in os.listdir(incoming):
                        if not os.path.isfile(os.path.join(incoming, fn)):
                            continue
                        ext = os.path.splitext(fn)[1].lower().lstrip(".")
                        candidates.append(
                            {
                                "physical_file_name": fn,
                                "source_system": ss,
                                "source_type": ext,
                            }
                        )

            missing_records = []
            processed_candidates = []
            for c in candidates:
                phys = c.get("physical_file_name")
                ss = c.get("source_system") or None
                st = c.get("source_type") or None

                try:
                    updated, logical_sf, matched_cfg = fetch_and_update_file_audit(
                        client_id, new_batch_id, phys, ss, st
                    )
                except Exception as e:
                    print(
                        f"[{client_schema}] ERROR saat update file_audit untuk {phys}: {e}"
                    )
                    batch_start = datetime.now()
                    log_batch_status(
                        client_id=client_id,
                        status="FAILED",
                        batch_id=new_batch_id,
                        job_name=job_name,
                        error_message=f"UPDATE_FILE_AUDIT_ERROR_{phys}: {e}",
                        start_time=batch_start,
                    )
                    return

                if not updated:
                    missing_records.append((phys, ss, st))
                else:
                    # find file on disk (may be in incoming/failed/success)
                    found_path = None
                    if ss:
                        for kind in ("incoming", "failed", "success"):
                            p = os.path.join(f"raw/{client_schema}/{ss}", kind, phys)
                            if os.path.exists(p):
                                found_path = p
                                break
                    if not found_path:
                        for root_ss in ("crm", "erp", "api", "db"):
                            for kind in ("incoming", "failed", "success"):
                                p = os.path.join(
                                    f"raw/{client_schema}/{root_ss}", kind, phys
                                )
                                if os.path.exists(p):
                                    found_path = p
                                    ss = root_ss
                                    break
                            if found_path:
                                break

                    # pass along matched metadata for batch_info upsert
                    cfg_meta = {
                        "physical_file_name": phys,
                        "source_type": st,
                        "source_system": ss,
                        "logical_source_file": logical_sf,
                        "existing_audit": True,
                    }
                    if matched_cfg:
                        cfg_meta.update(
                            {
                                "target_schema": matched_cfg.get("target_schema"),
                                "target_table": matched_cfg.get("target_table"),
                                "source_config": matched_cfg.get("source_config"),
                                "logical_source_file": matched_cfg.get(
                                    "logical_source_file"
                                ),
                            }
                        )

                    if found_path:
                        processed_candidates.append(
                            {
                                "orig_path": found_path,
                                "orig_name": phys,
                                "ss": ss,
                                "ext": st,
                                "cfg": cfg_meta,
                            }
                        )
                    else:
                        print(
                            f"[{client_schema}] WARNING: audit updated but file not found on disk for {phys} (batch {new_batch_id})."
                        )

            if missing_records:
                batch_start = datetime.now()
                log_batch_status(
                    client_id=client_id,
                    status="FAILED",
                    batch_id=new_batch_id,
                    job_name=job_name,
                    error_message=f"FILE_AUDIT_RECORD_NOT_FOUND for {len(missing_records)} files",
                    start_time=batch_start,
                )
                print(
                    f"[{client_schema}] Gagal restart: beberapa file tidak punya record file_audit (belum pernah dijalankan mode start). Detail contoh: {missing_records[:5]}"
                )
                # Important: abort here and do NOT proceed to any subprocess
                return

            files_to_handle.extend(processed_candidates)

            if not files_to_handle:
                batch_start = datetime.now()
                log_batch_status(
                    client_id=client_id,
                    status="FAILED",
                    batch_id=new_batch_id,
                    job_name=job_name,
                    error_message="NO_FILES_TO_PROCESS_ON_RESTART",
                    start_time=batch_start,
                )
                print(
                    f"[{client_schema}] Tidak ada berkas yang dapat diproses pada restart untuk batch {new_batch_id}."
                )
                return

        # REPROCESSING: scan data/...incoming for parquet and correlate with batch_info
        elif mode == "reprocessing":
            new_batch_id = last_batch
            batch_info_path = f"batch_info/{client_schema}/incoming/batch_output_{client_schema}_{new_batch_id}.json"
            if not os.path.exists(batch_info_path):
                batch_start = datetime.now()
                log_batch_status(
                    client_id=client_id,
                    status="FAILED",
                    batch_id=new_batch_id,
                    job_name=job_name,
                    error_message=f"NO_BATCH_INFO_FOR_{new_batch_id}",
                    start_time=batch_start,
                )
                print(
                    f"[{client_schema}] Tidak ada batch_info untuk batch {new_batch_id}. Batal reprocessing."
                )
                return

            try:
                batch_info = read_json_retry(batch_info_path)
                if not isinstance(batch_info, dict):
                    raise ValueError(
                        f"batch_info parsed but is not an object (type={type(batch_info).__name__})"
                    )
            except Exception:
                batch_start = datetime.now()
                log_batch_status(
                    client_id=client_id,
                    status="FAILED",
                    batch_id=new_batch_id,
                    job_name=job_name,
                    error_message=f"INVALID_BATCH_INFO_{new_batch_id}",
                    start_time=batch_start,
                )
                print(
                    f"[{client_schema}] Gagal baca batch_info {batch_info_path}. Batal."
                )
                return

            parquet_map = {}
            for f in batch_info.get("files") or []:
                if not isinstance(f, dict):
                    continue
                pn = f.get("parquet_name")
                if pn:
                    parquet_map[str(pn)] = f

            data_root = f"data/{client_schema}"
            candidates = []
            if os.path.isdir(data_root):
                for entry in os.listdir(data_root):
                    p = os.path.join(data_root, entry)
                    inc = os.path.join(p, "incoming")
                    if os.path.isdir(inc):
                        candidates.append(inc)
                fallback = os.path.join(data_root, "incoming")
                if os.path.isdir(fallback):
                    candidates.append(fallback)

            for d in candidates:
                for fn in os.listdir(d):
                    if not fn.lower().endswith(".parquet"):
                        continue
                    parquet_filename = fn
                    parquet_path = os.path.join(d, fn)
                    matched_entry = None

                    if parquet_filename in parquet_map:
                        matched_entry = parquet_map[parquet_filename]
                    else:
                        lower_map = {k.lower(): v for k, v in parquet_map.items()}
                        if parquet_filename.lower() in lower_map:
                            matched_entry = lower_map[parquet_filename.lower()]

                    if not matched_entry:
                        base = os.path.splitext(parquet_filename)[0]
                        base_norm = base.lower().replace("_batch", "").strip()
                        for k, v in parquet_map.items():
                            kb = os.path.splitext(k)[0].lower()
                            if kb.replace("_batch", "").strip() == base_norm:
                                matched_entry = v
                                break

                    if not matched_entry:
                        print(
                            f"[{client_schema}] SKIP parquet without manifest entry: {parquet_path}"
                        )
                        continue

                    physical = matched_entry.get("physical_file_name")
                    ss = matched_entry.get("source_system") or None
                    if not physical:
                        print(
                            f"[{client_schema}] SKIP parquet {parquet_filename} because manifest missing physical_file_name"
                        )
                        continue

                    files_to_handle.append(
                        {
                            "orig_path": parquet_path,
                            "orig_name": physical,
                            "ss": ss,
                            "ext": "parquet",
                            "cfg": matched_entry,
                        }
                    )

            if not files_to_handle:
                batch_start = datetime.now()
                log_batch_status(
                    client_id=client_id,
                    status="FAILED",
                    batch_id=new_batch_id,
                    job_name=job_name,
                    error_message="FILES NOT FOUND TO REPROCESS",
                    start_time=batch_start,
                )
                print(
                    f"[{client_schema}] Tidak ditemukan parquet untuk direprocessing pada batch {new_batch_id}."
                )
                return

        # prepare batch_info path (atomic writes when updating)
        batch_info_dir = f"batch_info/{client_schema}/incoming"
        os.makedirs(batch_info_dir, exist_ok=True)
        batch_info_path = os.path.join(
            batch_info_dir, f"batch_output_{client_schema}_{new_batch_id}.json"
        )

        # processing loop (shared)
        batch_start = datetime.now()
        batch_status = "SUCCESS"
        batch_error_message = None
        success_files = []

        for item in files_to_handle:
            orig_path = item["orig_path"]
            orig_name = item[
                "orig_name"
            ]  # for restart this already includes batch suffix
            ss = item["ss"] or "unknown"
            ext = item["ext"]
            cfg = item["cfg"]

            # NEW GUARD:
            # If we're in restart mode, ensure this file came from an existing audit row.
            # If not, abort the whole run immediately (no subprocesses).
            if mode == "restart" and not cfg.get("existing_audit"):
                batch_status = "FAILED"
                batch_error_message = f"{orig_name} - FILE_AUDIT_RECORD_NOT_FOUND"
                print(
                    f"[{client_schema}] Abort restart: found file without existing audit record: {orig_name}. Tidak akan menjalankan subprocess."
                )
                log_batch_status(
                    client_id=client_id,
                    status="FAILED",
                    batch_id=new_batch_id,
                    job_name=job_name,
                    error_message=batch_error_message,
                    start_time=batch_start,
                )
                # close DB cursor/conn in finally and return
                cur.close()
                conn.close()
                return

            try:
                if mode == "reprocessing":
                    r = subprocess.run(
                        [
                            sys.executable,
                            "scripts/validate_mapping.py",
                            client_schema,
                            orig_name,
                        ]
                    )
                    if r.returncode != 0:
                        print(
                            f"[{client_schema}] validate_mapping FAILED for {orig_name}"
                        )
                        batch_status = "FAILED"
                        batch_error_message = f"{orig_name} - validate_mapping failed"
                        continue

                    r = subprocess.run(
                        [
                            sys.executable,
                            "scripts/validate_row.py",
                            client_schema,
                            orig_name,
                        ]
                    )
                    if r.returncode != 0:
                        print(
                            f"[{client_schema}] WARNING validate_row failed for {orig_name} (non-fatal)"
                        )

                    r = subprocess.run(
                        [
                            sys.executable,
                            "scripts/load_to_bronze.py",
                            client_schema,
                            orig_name,
                        ]
                    )
                    if r.returncode != 0:
                        print(
                            f"[{client_schema}] load_to_bronze FAILED for {orig_name}"
                        )
                        batch_status = "FAILED"
                        batch_error_message = f"{orig_name} - load_to_bronze failed"
                        continue

                    success_files.append(orig_name)

                    if ss and ss != "unknown":
                        failed_path = f"raw/{client_schema}/{ss}/failed/{orig_name}"
                        archive_path = f"raw/{client_schema}/{ss}/archive/{orig_name}"
                        if os.path.exists(failed_path):
                            os.makedirs(os.path.dirname(archive_path), exist_ok=True)
                            shutil.move(failed_path, archive_path)

                elif mode == "restart" and cfg.get("existing_audit"):
                    # DO NOT rename. Just move (if necessary) to success, upsert batch_info with matched config, then run pipeline.
                    raw_success = f"raw/{client_schema}/{ss}/success"
                    raw_failed = f"raw/{client_schema}/{ss}/failed"
                    raw_archive = f"raw/{client_schema}/{ss}/archive"
                    os.makedirs(raw_success, exist_ok=True)
                    os.makedirs(raw_failed, exist_ok=True)
                    os.makedirs(raw_archive, exist_ok=True)

                    dest_success = os.path.join(raw_success, orig_name)
                    try:
                        if os.path.abspath(orig_path) != os.path.abspath(dest_success):
                            if os.path.exists(dest_success):
                                os.remove(dest_success)
                            shutil.move(orig_path, dest_success)
                    except Exception:
                        try:
                            shutil.copy2(orig_path, dest_success)
                        except Exception:
                            pass

                    new_entry = {
                        "physical_file_name": orig_name,
                        "logical_source_file": cfg.get("logical_source_file"),
                        "source_system": ss,
                        "source_type": ext,
                        "target_schema": cfg.get("target_schema"),
                        "target_table": cfg.get("target_table"),
                        "source_config": cfg.get("source_config"),
                        "parquet_name": None,
                    }
                    ok = upsert_file_entry_batch_info(batch_info_path, new_entry)
                    if not ok:
                        print(
                            f"[{client_schema}] WARNING: gagal upsert batch_info untuk {orig_name}; continuing"
                        )

                    if ext.lower() == "csv":
                        r = subprocess.run(
                            [
                                sys.executable,
                                "handlers/convert_to_parquet.py",
                                client_schema,
                                orig_name,
                            ]
                        )
                        if r.returncode != 0:
                            src = os.path.join(raw_success, orig_name)
                            if os.path.exists(src):
                                shutil.move(src, os.path.join(raw_failed, orig_name))
                            raise Exception("FAILED on convert_to_parquet")

                        ok = wait_for_parquet_name(
                            batch_info_path, orig_name, timeout=30.0, poll_interval=0.25
                        )
                        if not ok:
                            src = os.path.join(raw_success, orig_name)
                            if os.path.exists(src):
                                shutil.move(src, os.path.join(raw_failed, orig_name))
                            raise Exception(
                                "FAILED: convert_to_parquet did not update batch_info with parquet_name within timeout"
                            )

                    r = subprocess.run(
                        [
                            sys.executable,
                            "scripts/validate_mapping.py",
                            client_schema,
                            orig_name,
                        ]
                    )
                    if r.returncode != 0:
                        src = os.path.join(raw_success, orig_name)
                        if os.path.exists(src):
                            shutil.move(src, os.path.join(raw_failed, orig_name))
                        raise Exception("FAILED on validate_mapping")

                    r = subprocess.run(
                        [
                            sys.executable,
                            "scripts/validate_row.py",
                            client_schema,
                            orig_name,
                        ]
                    )
                    if r.returncode != 0:
                        print(
                            f"[{client_schema}] WARNING validate_row failed for {orig_name} (non-fatal)"
                        )

                    r = subprocess.run(
                        [
                            sys.executable,
                            "scripts/load_to_bronze.py",
                            client_schema,
                            orig_name,
                        ]
                    )
                    if r.returncode != 0:
                        src = os.path.join(raw_success, orig_name)
                        if os.path.exists(src):
                            shutil.move(src, os.path.join(raw_failed, orig_name))
                        raise Exception("FAILED on load_to_bronze")

                    src = os.path.join(raw_success, orig_name)
                    if os.path.exists(src):
                        shutil.move(src, os.path.join(raw_archive, orig_name))

                    success_files.append(orig_name)

                else:
                    # START flow (or unexpected path). Rename, insert audit, then pipeline.
                    base, e = os.path.splitext(orig_name)
                    base_std = base.strip().replace(" ", "_").replace("-", "_")
                    new_name = f"{base_std}_{new_batch_id}{e}"
                    raw_in = f"raw/{client_schema}/{ss}/incoming"
                    raw_success = f"raw/{client_schema}/{ss}/success"
                    raw_failed = f"raw/{client_schema}/{ss}/failed"
                    raw_archive = f"raw/{client_schema}/{ss}/archive"
                    os.makedirs(raw_success, exist_ok=True)
                    os.makedirs(raw_failed, exist_ok=True)
                    os.makedirs(raw_archive, exist_ok=True)

                    new_full = os.path.join(raw_in, new_name)
                    os.rename(orig_path, new_full)

                    audit_rec = {
                        "client_id": client_id,
                        "processed_by": os.getenv("PROCESS_USER")
                        or getpass.getuser()
                        or "batch_processing",
                        "logical_source_file": cfg.get("logical_source_file"),
                        "physical_file_name": new_name,
                        "batch_id": new_batch_id,
                        "file_received_time": datetime.now(),
                        "source_type": ext,
                        "source_system": ss,
                        "config_validation_status": (
                            "SUCCESS" if cfg.get("logical_source_file") else "FAILED"
                        ),
                    }

                    try:
                        insert_file_audit(cur, conn, audit_rec)
                    except Exception:
                        try:
                            if os.path.exists(new_full):
                                os.rename(new_full, orig_path)
                        except Exception:
                            pass
                        raise

                    shutil.move(new_full, os.path.join(raw_success, new_name))

                    new_entry = {
                        "physical_file_name": new_name,
                        "logical_source_file": cfg.get("logical_source_file"),
                        "source_system": ss,
                        "source_type": ext,
                        "target_schema": cfg.get("target_schema"),
                        "target_table": cfg.get("target_table"),
                        "source_config": cfg.get("source_config"),
                        "parquet_name": None,
                    }

                    ok = upsert_file_entry_batch_info(batch_info_path, new_entry)
                    if not ok:
                        print(
                            f"[{client_schema}] WARNING: gagal upsert batch_info untuk {new_name}; continuing"
                        )

                    if ext.lower() == "csv":
                        r = subprocess.run(
                            [
                                sys.executable,
                                "handlers/convert_to_parquet.py",
                                client_schema,
                                new_name,
                            ]
                        )
                        if r.returncode != 0:
                            src = os.path.join(raw_success, new_name)
                            if os.path.exists(src):
                                shutil.move(src, os.path.join(raw_failed, new_name))
                            raise Exception("FAILED on convert_to_parquet")

                        ok = wait_for_parquet_name(
                            batch_info_path, new_name, timeout=30.0, poll_interval=0.25
                        )
                        if not ok:
                            src = os.path.join(raw_success, new_name)
                            if os.path.exists(src):
                                shutil.move(src, os.path.join(raw_failed, new_name))
                            raise Exception(
                                "FAILED: convert_to_parquet did not update batch_info with parquet_name within timeout"
                            )

                    r = subprocess.run(
                        [
                            sys.executable,
                            "scripts/validate_mapping.py",
                            client_schema,
                            new_name,
                        ]
                    )
                    if r.returncode != 0:
                        src = os.path.join(raw_success, new_name)
                        if os.path.exists(src):
                            shutil.move(src, os.path.join(raw_failed, new_name))
                        raise Exception("FAILED on validate_mapping")

                    r = subprocess.run(
                        [
                            sys.executable,
                            "scripts/validate_row.py",
                            client_schema,
                            new_name,
                        ]
                    )
                    if r.returncode != 0:
                        print(
                            f"[{client_schema}] WARNING validate_row failed for {new_name} (non-fatal)"
                        )

                    r = subprocess.run(
                        [
                            sys.executable,
                            "scripts/load_to_bronze.py",
                            client_schema,
                            new_name,
                        ]
                    )
                    if r.returncode != 0:
                        src = os.path.join(raw_success, new_name)
                        if os.path.exists(src):
                            shutil.move(src, os.path.join(raw_failed, new_name))
                        raise Exception("FAILED on load_to_bronze")

                    src = os.path.join(raw_success, new_name)
                    if os.path.exists(src):
                        shutil.move(src, os.path.join(raw_archive, new_name))

                    success_files.append(new_name)

            except Exception as e:
                print(f"❌ [{client_schema}] Gagal memproses {orig_name} => {e}")
                batch_status = "FAILED"
                batch_error_message = f"{orig_name} - {str(e)}"

        log_batch_status(
            client_id=client_id,
            status=batch_status,
            batch_id=new_batch_id,
            job_name=job_name,
            error_message=batch_error_message,
            start_time=batch_start,
        )
        print(
            f"[{client_schema}] Done. batch_id={new_batch_id} status={batch_status} files_success={len(success_files)}"
        )

    finally:
        # make sure cursor/conn are closed if not closed already
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


# -----------------------------
# Main
# -----------------------------


def main():
    args = sys.argv[1:]
    if len(args) == 2:
        client, mode = args[0], args[1]
        if mode not in ("start", "restart", "reprocessing"):
            print("Gunakan mode: start | restart | reprocessing")
            return
        process_client(client, mode)
    elif len(args) == 0:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT client_schema FROM tools.client_reference")
        clients = cur.fetchall()
        cur.close()
        conn.close()
        for (client_schema,) in clients:
            process_client(client_schema, "start")
    else:
        print(
            "Format: python batch_processing.py [client] [start|restart|reprocessing]"
        )


if __name__ == "__main__":
    main()
