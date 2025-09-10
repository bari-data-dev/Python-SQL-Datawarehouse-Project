# Alur Kerja & Deskripsi Proyek — Manifest-Driven On-Prem ETL

**Versi dokumen:** 2.0 · On‑prem · Python-based

---

## Ringkasan Eksekutif

Dokumen ini menyajikan: tujuan & value proposition, arsitektur high‑level, artefak & peran file, struktur filesystem kontrak, lifecycle batch step‑by‑step, mode operasi, model retry/idempotency/recovery, mapping ke Prefect (peta migrasi), runbook troubleshooting, DDL contoh, checklist kesiapan produksi, dan roadmap prioritas.

---

## 1. Tujuan & Value Proposition

Platform ini adalah pipeline on‑prem, manifest‑driven, multi‑tenant per‑client yang:

* Menjamin traceability tiap run melalui `batch_id` dan `batch_info.json` (manifest).
* Menyediakan idempotent transforms (stored procedures) sehingga re‑run aman dan reproducible.
* Menghadirkan operator UI (webapp) yang memanage metadata driver dan membaca log langsung dari PostgreSQL untuk monitoring, audit, dan recovery manual.
* Memungkinkan migrasi ke orkestrator/engine yang lebih scalable (Prefect, PySpark) karena desain modular dan kontrak artefak (manifest, `parquet_name`, `batch_id`).

**Value:** reproducibility, reduced operator toil, per‑client isolation, auditability, dan migrasi bertahap ke orkestrasi terpusat.

---

## 2. Ringkasan Arsitektur (High Level)

* **Orchestrator saat ini:** Pure Python scripts (`batch_processing.py` sebagai entrypoint).
* **Storage:** Local filesystem terstruktur: `raw/`, `data/`, `batch_info/`.
* **Metadata & Logs:** PostgreSQL (schema `tools`) — metadata, mappings, configs, dan tabel log untuk tiap tahap.
* **Transform/Integration/MV:** Stored procedures di Postgres (schema `tools`) memindahkan data Bronze → Silver → Gold.
* **Webapp:** operator UI untuk client/config/mapping/required‑columns/transformation/integration/mv refresh & dashboards.

Komponen modular memungkinkan penggantian orchestrator (Prefect) tanpa mengubah kontrak file/DB.

---

## 3. Artefak Kunci & Peran File (Fast Index)

* `batch_info` (JSON manifest) — single source of truth untuk run.
* `handlers/batch_processing.py` — entrypoint; modes: `start | restart | reprocessing`. Scan incoming → create/rename → upsert manifest → spawn handlers → record logs.
* `handlers/convert_to_parquet.py` — convert CSV/XLSX/JSON → Parquet (pandas → pyarrow/snappy); update `batch_info.parquet_name`.
* `scripts/validate_mapping.py` — baca Parquet (pyarrow), compare columns vs `tools.column_mapping`; mismatch → move Parquet ke `failed`.
* `scripts/validate_row.py` — DuckDB untuk null/duplicate checks berdasarkan `tools.required_columns`.
* `scripts/load_to_bronze.py` — DuckDB → CSV → COPY ke Postgres bronze table; idempotent: `DELETE WHERE dwh_batch_id = <batch_id>` sebelum `COPY`.
* `scripts/silver_clean_transform.py` — panggil stored procedures transformation (Bronze→Silver) sesuai `tools.transformation_config`.
* `scripts/gold_integration.py` — panggil procedures integration (Silver→Gold) sesuai `tools.integration_config` + dependency checks.
* `scripts/refresh_mv.py` — panggil refresh MV procedures (nama di `tools.mv_refresh_config`).
* Stored procedures contoh: `tools.load_crm_cust_info_v1`, `tools.load_fact_sales_v1`, `tools.refresh_mv_customer_churn`.

---

## 4. Struktur Filesystem (Kontrak)

```
raw/{client_schema}/{source_system}/{incoming,success,failed,archive}
data/{client_schema}/{source_system}/{incoming,failed,archive}
batch_info/{client_schema}/{incoming,success,failed,archive,refreshed}
handlers/
scripts/
sql/ (berisi dokumentasi procedure, SELECT transformasi, dan DDL kalau migrasi pakai Prefect sudah tidak perlu procedure langsung run INSERT INTO SELECT transformasi/integrasi/refresh mv)
webapp/
```

* Saat `start`, `physical_file_name` diberi suffix `_BATCH######`.
* Parquet ditulis ke `data/{client_schema}/{source_system}/incoming/{parquet_name}`.
* `batch_info` JSON tercatat di `batch_info/{client_schema}/incoming/batch_output_{client}_{BATCH}.json`.

---

## 5. Lifecycle Batch — Operasional Step‑by‑Step

1. Operator menaruh file sumber ke `raw/{client_schema}/{source_system}/incoming`.
2. Trigger: `python batch_processing.py <client_schema> start` (atau via webapp button fitur ini belum ada tapi akan ditambahkan).
3. `batch_processing`:

   * Ambil `last_batch_id` dari `tools.client_reference`.
   * Jika mode `start` → generate `new_batch_id` (increment), rename file menjadi `{base}_BATCH######`.
   * Insert/record ke `tools.file_audit_log`.
   * Upsert/Write `batch_info` manifest di `batch_info/{client}/incoming` (atomic).
4. Convert: `convert_to_parquet.py` membaca `raw/.../success/{file}` → tulis Parquet ke `data/.../incoming/` → update `batch_info.parquet_name` (atomic write + retry).
5. Validate Mapping: `validate_mapping.py` baca Parquet schema → bandingkan dengan `tools.column_mapping`; mismatch → move Parquet → `data/.../failed` dan log ke `tools.mapping_validation_log`.
6. Validate Row: `validate_row.py` (DuckDB) cek null pada required columns & duplicate rules → hasil ke `tools.row_validation_log`. Policy: ada per‑file/per‑client policy untuk fatal vs warning.
7. Load to Bronze: `load_to_bronze.py` materialize CSV via DuckDB → `DELETE FROM bronze_table WHERE dwh_batch_id = <batch_id>` → `COPY` → on success move Parquet → `archive`.
8. Transform (Silver): `silver_clean_transform.py` panggil procedures sesuai `tools.transformation_config`; log ke `tools.transformation_log`.
9. Integrate (Gold): `gold_integration.py` jalankan procedures sesuai `tools.integration_config` — group dimens & facts, dependency checks via `tools.integration_dependencies`; log results.
10. Refresh MV: `refresh_mv.py` panggil refresh procedures, log ke `tools.mv_refresh_log`.
11. Finalize: update `tools.job_execution_log` (status akhir), move `batch_info` ke `success`/`failed`/`refreshed` sesuai outcome. Webapp menampilkan aggregasi KPI dari logs.

---

## 6. Mode Operasi & Semantics

* **start:** increment `batch_id`, rename files, create `batch_info`, jalankan full pipeline.
* **restart:** gunakan **sama** `batch_id` (tidak increment), update existing audit rows — digunakan setelah perbaikan mapping/konfigurasi. File di `failed` harus dipindahkan ke `incoming` secara manual bila perlu.
* **reprocessing:** jalankan ulang downstream (validate → load → transform → integrate) menggunakan existing Parquet; tidak increment `batch_id`.
* **Checkpointing:** granular via `tools.file_audit_log` (per-file), `tools.job_execution_log` (per-job), dan `batch_info` manifest (atomic updates).

---

## 7. Retry, Idempotency & Recovery Model

* **I/O retries:** helper functions `read_json_retry`, `write_atomic` dan `wait_for_parquet_name` dengan limited polling.
* **Downstream retry policy:** saat ini manual — jika convert/validate/load fail, file dipindah ke `failed`. Recovery dilakukan via `restart`/`reprocessing`.
* **Idempotency:** loads & stored procedures menggunakan `DELETE WHERE dwh_batch_id = ...` atau upsert logic; stored procedures dirancang agar re‑run tidak duplikasi.
* **Concurrency:** aman bila instances handling berbeda `{client_schema, source_system}`. Tidak aman untuk multiple instances menulis ke folder/manifest yang sama (no distributed lock present).

---

## 8. Database Artifacts (Penting untuk Migrasi)

Daftar ringkas tabel & kegunaan:

* `tools.client_reference` — client\_id, client\_schema, last\_batch\_id.
* `tools.client_config` — mapping file configs → logical\_source\_file, target\_schema/table.
* `tools.column_mapping` — mapping kolom sumber → target.
* `tools.required_columns` — required cols per file.
* `tools.file_audit_log` — per-file audit (convert\_status, mapping\_validation\_status, load\_status, total\_rows, physical\_file\_name, batch\_id).
* `tools.job_execution_log` — per-job execution.
* `tools.mapping_validation_log`, `tools.row_validation_log`, `tools.load_error_log`, `tools.transformation_log`, `tools.integration_log`, `tools.mv_refresh_log`.
* `tools.transformation_config`, `tools.integration_config`, `tools.mv_refresh_config`, `tools.integration_dependencies`.

Stored procedures menulis ke masing‑masing log table sebagai bagian dari kontrak.

---

## 9. Contoh Manifest (`batch_info.json`)

```json
{
  "client_schema": "client1",
  "client_id": 2,
  "batch_id": "BATCH000014",
  "files": [
    {
      "physical_file_name": "cust_info_BATCH000014.csv",
      "logical_source_file": "cust_info",
      "source_system": "crm",
      "source_type": "csv",
      "target_schema": "bronze_client1",
      "target_table": "crm_cust_info",
      "source_config": null,
      "parquet_name": "cust_info_BATCH000014.parquet",
      "client_schema": "client1",
      "client_id": 2,
      "batch_id": "BATCH000014"
    }
  ],
  "transformation_procedure": [
    "tools.load_crm_cust_info_v1",
    "tools.load_crm_prd_info_v1"
  ],
  "integration_procedure": [
    "tools.load_dim_customers_v1",
    "tools.load_fact_sales_v1"
  ],
  "refresh_procedure": [
    "mv_customer_lifetime_value",
    "mv_customer_order_gap"
  ]
}
```

---

## 10. README — Key Operational Notes (Singkat)

* **Storage paths:** `raw/...`, `data/...` (Parquet), `batch_info/...` (manifest).
* **Manifest contract:** `logical_source_file`, `source_type`, `parquet_name`, `batch_id` harus ada.
* **CLI examples:**

```bash
python batch_processing.py client1 start
python batch_processing.py client1 restart
python batch_processing.py client1 reprocessing
```

---

## 11. Operational Runbook (Common Failures & Actions)

**A. Convert fail**

* Symptom: `job_execution_log` FAILED, `parquet_name` kosong dalam `batch_info`.
* Action: inspect raw file encoding/format; fix file or config; reingest and `start`/`restart` as appropriate.

**B. Mapping mismatch (validate\_mapping FAILED)**

* Symptom: `mapping_validation_log` entry, Parquet → `data/.../failed`.
* Action: perbaiki mapping via webapp (`tools.column_mapping`), pindahkan Parquet ke `data/.../incoming`, jalankan `python batch_processing.py <client> reprocessing`.

**C. Load to Bronze fail**

* Symptom: `load_error_log` entry; type cast or missing column.
* Action: check `tools.column_mapping` vs target table schema; add missing columns or update mapping; re-run `reprocessing` if Parquet exists.

**D. Transform/Integration fail**

* Symptom: `transformation_log` or `integration_log` FAILED.
* Action: inspect proc error, fix proc or upstream data, re-run transform/integration steps.

**E. MV refresh fail**

* Symptom: `mv_refresh_log` FAILED.
* Action: inspect underlying table availability, proc dependencies, re-run `refresh_mv.py`.

---

## 12. Limitasi / Technical Debt (Catatan untuk Supervisor)

* Local FS single‑node → durability & concurrency risk.
* Secrets di `.env` (plaintext) — butuh vault atau K/V secure.
* Downstream auto‑retry & alerting belum ada — MTTR manual.
* Stored procedures kadang hardcoded pada client schemas — butuh parametrization.
* Caller/proc contract inconsistency (CALL vs FUNCTION return) — standardize.

---

## 13. Peta Migrasi ke Prefect (Praktis)

**Strategi:** pertahankan kontrak artefak (manifest JSON, `parquet_name`, `batch_id`, logs). Implementasikan DAG templated per client atau single DAG dengan dynamic task mapping.

**Task mapping (contoh):**

* sensor: `wait_for_files` (FileSensor / custom sensor)
* task: `start_batch` (PythonOperator) — increment batch\_id, write `batch_info`
* dynamic TaskGroup per file: `convert` → `validate_mapping` → `validate_row` → `load_to_bronze`
* taskgroup: `call_transformations` (SubDag / TaskGroup)
* taskgroup: `call_integrations` (dependency‑aware)
* task: `refresh_mv`
* finalize: update logs + archive

Manfaat: gunakan Prefect retries, SLA, XCom untuk menyimpan `parquet_name`/artefak; sensors + XComs untuk ketahanan. Pastikan idempotency contract dipertahankan (DELETE by `dwh_batch_id`).

---

## 14. Risiko Utama & Mitigasi

1. **Schema drift** — Mitigasi: `schema_registry` per `logical_source_file` + pre-load validation.
2. **Scale & durability** — Mitigasi: storage abstraction (NFS / S3 adapter).
3. **Operational MTTR** — Mitigasi: limited automatic retries + alerting + metrics.
4. **Security** — Mitigasi: migrate secrets to on‑prem vault; webapp auth & RBAC.

---

## 15. Rekomendasi Roadmap (Prioritas)

**Quick wins**

* Implement `schema_registry` per `logical_source_file`.
* Add limited automatic retry (3 attempts, exponential backoff) pada convert/validate/load.
* Implement retention/purge job (default 90 hari).

**Mid-term**

* Secrets vault (HashiCorp or on‑prem K/V).
* LDAP/AD + RBAC for webapp.
* Metrics → Prometheus + Grafana.

**Strategic**

* Abstraction layer for storage (NFS/S3 adapter).
* Orchestrator: migrate ke refect dan adapt stored proc invocation to idempotent SQL tasks.

---
