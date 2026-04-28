# ETL Platform PTIT — Tích hợp dữ liệu học vụ đa nguồn

> Đồ án Ngành 2 — Bộ môn Kỹ thuật Dữ liệu, Khoa Viễn Thông I
> Học viện Công nghệ Bưu chính Viễn thông
> **Nhóm 8**: Vũ Hoàng Phúc (B23DCKD053), Đỗ Minh Hoàng (B23DCKD024)
> **GVHD**: PGS.TS. Lê Hải Châu

## 📌 Mục tiêu

Xây dựng hệ thống ETL tự động tích hợp dữ liệu từ **3 nguồn dị thể** (PostgreSQL, CSV, REST API) vào một **Data Warehouse Star Schema**, kèm kiểm soát chất lượng dữ liệu tự động và giám sát thời gian thực, phục vụ:

- Phát hiện sinh viên có nguy cơ bỏ học (cảnh báo học vụ).
- Hỗ trợ xét học bổng dựa trên GPA + điểm rèn luyện + tình trạng tài chính.
- Báo cáo tổng hợp theo học kỳ cho Ban Giám hiệu.

## 🏗️ Kiến trúc hệ thống
┌─────────────────┐    ┌─────────────┐    ┌─────────────────┐
│ PostgreSQL      │    │ CSV files   │    │ REST API JSON   │
│ (Phòng Đào tạo) │    │ (Phòng CTSV)│    │ (Tài chính)     │
└────────┬────────┘    └──────┬──────┘    └────────┬────────┘
│                    │                    │
└──────────┬─────────┴────────────────────┘
▼
┌──────────────────┐
│  EXTRACT (ELT)   │
└────────┬─────────┘
▼
┌───────────────────────┐
│ MinIO bucket raw-data │  ◄── parquet, run_id={timestamp}
└───────────┬───────────┘
▼
┌──────────────────┐
│ Great Expectations│  5 suites: students, grades,
│   VALIDATE        │  ctsv, tai_chinh, warehouse
└────────┬─────────┘
│ FAIL → pipeline dừng
▼
┌──────────────────┐
│  TRANSFORM        │  GPA hệ 4/10, xếp loại,
│                   │  dedup 2 tầng, SCD2 detect
└────────┬─────────┘
▼
┌───────────────────────────┐
│ MinIO bucket staging-data │  ◄── checkpoint, hỗ trợ resume
└───────────┬───────────────┘
▼
┌──────────────────┐
│  LOAD             │  Star Schema + SCD2 cascade
└────────┬─────────┘
▼
┌───────────────────────────┐
│ PostgreSQL Warehouse      │
│  4 dim + 4 fact + 1 agg   │
└───────────────────────────┘
▼
┌──────────────────────────┐
│ Grafana Business Dashboard│
└──────────────────────────┘

## 🛠️ Stack công nghệ

| Lớp | Công nghệ |
|-----|-----------|
| Orchestration | Apache Airflow 2.8.1 |
| Storage | MinIO (S3-compatible) |
| Database | PostgreSQL 15 (source + warehouse + airflow metadata) |
| Validation | Great Expectations 0.18 |
| Monitoring | Prometheus + Grafana + Alertmanager |
| ETL | Python 3.11 + Pandas + SQLAlchemy 2.0 |
| Container | Docker + docker-compose |


## 📂 Cấu trúc thư mục
.
├── airflow/                 # Dockerfile + entrypoint Airflow
├── dags/
│   └── daily_student_pipeline.py    # DAG ETL hằng ngày
├── data/
│   ├── csv/                 # Nguồn 2: CSV CTSV
│   └── api_json/            # Nguồn 3: API JSON fallback
├── grafana/
│   ├── pipeline_health.json     # Dashboard kỹ thuật
│   └── business_metrics.json    # Dashboard nghiệp vụ
├── great_expectations/
│   └── expectations/        # 5 suite JSON
├── monitoring/
│   ├── prometheus.yml
│   └── alert_rules.yml
├── sql/
│   ├── source/              # DDL DB nguồn
│   └── warehouse/           # DDL DB đích (4 dim + 4 fact + 1 agg)
├── src/
│   ├── config/              # settings, database
│   ├── etl/                 # extract, transform, load, aggregation
│   ├── models/              # SQLAlchemy ORM
│   ├── utils/               # logger, minio_client, metrics
│   └── validation/          # ge_validation
├── scripts/
│   ├── generate_sample_data.py    # Tạo data demo
│   ├── inject_errors.py           # Bơm lỗi để test GE
│   ├── validate_generated_data.py # Cross-source check
│   ├── mock_api_server.py         # Mock REST API
│   └── run_etl.py                 # CLI runner (3 modes)
├── docker-compose.yml
├── .env.example
└── README.md

## 🚀 Hướng dẫn chạy

### 1. Clone & cấu hình

```bash
git clone <repo>
cd <repo>
cp .env.example .env
# Sửa .env, thay CHANGE_ME bằng password thật
```

### 2. Khởi động toàn bộ stack

```bash
docker-compose up -d
```

Đợi ~30s để Airflow init metadata. Kiểm tra:

```bash
docker-compose ps
# Tất cả services phải ở state "Up" hoặc "healthy"
```

### 3. Generate dữ liệu mẫu (lần đầu)

```bash
docker exec -it airflow-webserver python /opt/airflow/scripts/generate_sample_data.py
```

→ Tạo ~1.700 sinh viên, ~55.000 điểm, ~9.500 bản ghi rèn luyện, ~9.500 bản ghi tài chính.

### 4. (Tuỳ chọn) Bơm lỗi để demo Great Expectations

```bash
docker exec -it airflow-webserver python /opt/airflow/scripts/inject_errors.py
```

→ Lỗi được bơm theo tỷ lệ tương ứng với `mostly` threshold trong GE suites.

### 5. Truy cập các UI

| Service | URL | Tài khoản |
|---------|-----|-----------|
| Airflow UI | http://localhost:8080 | admin / admin |
| Grafana | http://localhost:3000 | admin / admin |
| pgAdmin | http://localhost:5050 | admin@school.edu / admin |
| MinIO Console | http://localhost:9001 | minio_admin / minio_pass |
| Prometheus | http://localhost:9090 | (no auth) |

### 6. Trigger DAG

Vào Airflow UI → bật DAG `daily_student_pipeline` → Trigger.

DAG chạy ~3-5 phút trên data mẫu, đi qua: `extract → validate → transform → load → alert`.

### 7. Xem kết quả

- **Grafana Business**: http://localhost:3000/d/business-metrics-v8
- **MinIO**: http://localhost:9001 → bucket `raw-data` và `staging-data`
- **Warehouse**: pgAdmin → DB `school_warehouse` → `SELECT * FROM agg_student_summary LIMIT 10;`

## 🔄 Chế độ chạy CLI

Ngoài DAG, hệ thống hỗ trợ 3 chế độ qua `scripts/run_etl.py`:

```bash
# Full extract → transform → load
python scripts/run_etl.py --mode full

# Incremental theo học kỳ
python scripts/run_etl.py --mode incremental --hoc-ky HK1-2024-25

# Resume từ MinIO checkpoint (skip extract)
python scripts/run_etl.py --mode resume --run-id 2026-04-26_02-30
```

## 📊 Kết quả thực tế

| Chỉ số | Giá trị |
|--------|---------|
| Số nguồn dữ liệu | 3 (PostgreSQL, CSV, REST API) |
| Số bảng warehouse | 9 (4 dim + 4 fact + 1 agg) |
| Số records xử lý/lần chạy | ~140.000 |
| Thời gian pipeline (full) | ~3-5 phút |
| Số GE expectations | 38 (5 suites) |
| Tỷ lệ phát hiện inject errors | >95% |

## 📚 Tài liệu

- `docs/ARCHITECTURE.md` — chi tiết thiết kế từng layer
- `docs/DATA_DICTIONARY.md` — schema warehouse từng cột
- `docs/TROUBLESHOOTING.md` — fix các lỗi thường gặp

## 📝 License

Mã nguồn phục vụ mục đích học thuật.