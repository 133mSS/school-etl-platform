# School ETL Platform

> **ETL Platform PTIT — Tích hợp dữ liệu học vụ đa nguồn**
>
> Đồ án Ngành 2 — Bộ môn Kỹ thuật Dữ liệu, Khoa Viễn Thông I  
> Học viện Công nghệ Bưu chính Viễn thông (PTIT)

[![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Apache Airflow](https://img.shields.io/badge/Airflow-2.8.1-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![Great Expectations](https://img.shields.io/badge/Great%20Expectations-0.18.8-6B4FBB)](https://greatexpectations.io/)

## 📌 Tổng quan

**School ETL Platform** là một hệ thống ETL phục vụ tích hợp và phân tích dữ liệu học vụ. Hệ thống thu thập dữ liệu từ **3 nguồn dị thể**:

1. **PostgreSQL** — dữ liệu từ Phòng Đào tạo.
2. **CSV** — dữ liệu từ Phòng Công tác Sinh viên.
3. **REST API / JSON** — dữ liệu tài chính.

Dữ liệu được đưa qua quy trình:

**Extract → Raw Storage → Data Quality Validation → Transform → Staging → Load → Aggregate → Monitoring**

Mục tiêu của project là xây dựng một pipeline ETL có khả năng:

- Tích hợp dữ liệu từ nhiều nguồn và nhiều định dạng.
- Kiểm tra chất lượng dữ liệu trước khi nạp vào Data Warehouse.
- Chuẩn hóa và biến đổi dữ liệu phục vụ phân tích.
- Xây dựng **Star Schema** cho dữ liệu học vụ.
- Hỗ trợ **SCD Type 2** cho thông tin sinh viên.
- Theo dõi trạng thái pipeline và các chỉ số ETL bằng Prometheus/Grafana.
- Có thể chạy tự động theo lịch thông qua Apache Airflow.

## 🏗️ Kiến trúc

```text
                  DATA SOURCES
 ┌──────────────────┐  ┌──────────────┐  ┌──────────────────┐
 │ PostgreSQL       │  │ CSV          │  │ REST API / JSON  │
 │ Phòng Đào tạo    │  │ Phòng CTSV   │  │ Tài chính        │
 └────────┬─────────┘  └──────┬───────┘  └────────┬─────────┘
          │                   │                   │
          └───────────────────┼───────────────────┘
                              ▼
                    ┌──────────────────┐
                    │     EXTRACT      │
                    │ Airflow + Python │
                    └────────┬─────────┘
                             ▼
                    ┌──────────────────┐
                    │ MinIO - Raw Data │
                    │     Parquet      │
                    └────────┬─────────┘
                             ▼
                ┌─────────────────────────┐
                │   DATA QUALITY CHECK    │
                │  Great Expectations     │
                └────────────┬────────────┘
                             │
                    Validation passed
                             ▼
                    ┌──────────────────┐
                    │    TRANSFORM     │
                    │ Pandas + Python  │
                    └────────┬─────────┘
                             ▼
                  ┌──────────────────────┐
                  │ MinIO - Staging Data │
                  │      Checkpoint       │
                  └──────────┬───────────┘
                             ▼
                    ┌──────────────────┐
                    │       LOAD       │
                    │   PostgreSQL DW  │
                    └────────┬─────────┘
                             ▼
              ┌────────────────────────────┐
              │ Star Schema + Aggregate    │
              │ 4 Dimensions + 4 Facts     │
              │ + Student Summary          │
              └──────────────┬─────────────┘
                             ▼
                  ┌─────────────────────┐
                  │ Grafana Dashboard   │
                  │ Prometheus Metrics  │
                  └─────────────────────┘
```

### Pipeline orchestration

DAG chính `daily_student_pipeline` chạy theo lịch **02:00 hằng ngày**:

```text
start
  │
  ▼
init_run_id
  │
  ├──────────────┬──────────────┐
  ▼              ▼              ▼
PostgreSQL      CSV          REST API
  │              │              │
  └──────────────┴──────────────┘
                 │
                 ▼
             Validate
                 │
                 ▼
             Transform
                 │
                 ▼
                Load
                 │
                 ▼
          Alert / Metrics
                 │
                 ▼
                end
```

Ba nguồn được **extract song song**, sau đó mới thực hiện validation, transformation và loading.

## 🛠️ Công nghệ

| Thành phần | Công nghệ |
|---|---|
| Language | Python 3.11 |
| Orchestration | Apache Airflow 2.8.1 |
| Data Processing | Pandas 2.1.4, NumPy 1.26.3 |
| Source Database | PostgreSQL 15 |
| Data Warehouse | PostgreSQL 15 |
| Object Storage | MinIO |
| Data Quality | Great Expectations 0.18.8 |
| ORM / Database Access | SQLAlchemy, psycopg2 |
| HTTP / API | Requests |
| Monitoring | Prometheus + Grafana |
| Alerting | Alertmanager |
| Containerization | Docker + Docker Compose |
| Test | Pytest |

## 🗃️ Data Warehouse

Warehouse được tổ chức theo mô hình **Star Schema**, gồm:

### Dimensions

- `dim_sinh_vien` — thông tin sinh viên, hỗ trợ **SCD Type 2**.
- `dim_hoc_phan` — thông tin học phần.
- `dim_giang_vien` — thông tin giảng viên.
- `dim_hoc_ky` — thông tin học kỳ.

### Facts

- `fact_hoc_tap` — kết quả học tập.
- `fact_dang_ky` — đăng ký học phần.
- `fact_ctsv` — điểm rèn luyện, học bổng và kỷ luật.
- `fact_tai_chinh` — học phí, miễn giảm và công nợ.

### Aggregate

- `agg_student_summary` — tổng hợp thông tin học tập, rèn luyện và tài chính của sinh viên, phục vụ dashboard và các truy vấn phân tích.

Một số phép tính được xây dựng trong aggregate gồm:

- GPA hệ 4 và hệ 10.
- Số tín chỉ đăng ký / đạt / không đạt.
- Tỷ lệ đạt.
- Điểm rèn luyện trung bình.
- Tổng công nợ học phí.
- Mức độ rủi ro và cảnh báo học vụ.
- Một số chỉ số liên quan đến chất lượng bằng tốt nghiệp.

## 🔍 Data Quality

Great Expectations được tích hợp trực tiếp vào pipeline.

Quy trình:

```text
Extract
   ↓
Raw data
   ↓
Great Expectations
   ├── PASS → Transform → Load
   └── FAIL → Pipeline dừng
```

Khi validation thất bại, task validation sẽ raise exception và ngăn dữ liệu không đạt yêu cầu tiếp tục đi vào bước transform/load.

Hệ thống cũng hỗ trợ tạo **Great Expectations Data Docs** để kiểm tra kết quả validation.

## 📂 Cấu trúc thư mục

```text
school-etl-platform/
│
├── airflow/
│   ├── Dockerfile
│   └── requirements.txt
│
├── dags/
│   ├── daily_student_pipeline.py
│   ├── weekly_summary_pipeline.py
│   └── test_dag.py
│
├── data/
│   ├── csv/
│   ├── api_json/
│   └── ...
│
├── grafana/
│   ├── dashboards/
│   └── datasources/
│
├── great_expectations/
│   └── expectations/
│
├── monitoring/
│   ├── prometheus.yml
│   ├── alert_rules.yml
│   └── alertmanager.yml
│
├── scripts/
│   ├── generate_sample_data.py
│   ├── inject_errors.py
│   ├── validate_generated_data.py
│   ├── mock_api_server.py
│   └── run_etl.py
│
├── sql/
│   ├── source/
│   └── warehouse/
│       ├── create_dimension.sql
│       ├── create_facts.sql
│       └── create_views.sql
│
├── src/
│   ├── config/
│   ├── etl/
│   │   ├── extract.py
│   │   ├── transform.py
│   │   ├── load.py
│   │   └── aggregation.py
│   ├── models/
│   ├── utils/
│   └── validation/
│
├── .env.example
├── Dockerfile
├── docker-compose.yml
└── README.md
```

## 🚀 Cài đặt và chạy

### 1. Yêu cầu

Cần cài đặt:

- Docker Desktop
- Docker Compose
- Git

Khuyến nghị:

- RAM tối thiểu 8 GB dành cho Docker.
- Windows / Linux / macOS đều có thể sử dụng Docker Compose.

### 2. Clone repository

```bash
git clone https://github.com/133mSS/school-etl-platform.git
cd school-etl-platform
```

### 3. Tạo file môi trường

```bash
cp .env.example .env
```

Trên Windows PowerShell nếu cần:

```powershell
Copy-Item .env.example .env
```

Kiểm tra và cập nhật các biến môi trường cần thiết, đặc biệt là:

```text
DISCORD_WEBHOOK_URL
```

Không commit file `.env` chứa secret thật lên GitHub.

### 4. Khởi động hệ thống

```bash
docker compose up -d --build
```

Kiểm tra trạng thái:

```bash
docker compose ps
```

Chờ các service khởi động và chuyển sang trạng thái `healthy` khi có healthcheck.

### 5. Kiểm tra Airflow

Mở:

```text
http://localhost:8080
```

Thông tin mặc định của môi trường demo:

```text
Username: admin
Password: admin
```

Bật DAG:

```text
daily_student_pipeline
```

Sau đó có thể trigger DAG thủ công bằng nút **Trigger DAG**.

## 🌐 Các service

| Service | URL | Mục đích |
|---|---|---|
| Airflow | http://localhost:8080 | Orchestrate ETL |
| Grafana | http://localhost:3000 | Dashboard & monitoring |
| pgAdmin | http://localhost:5050 | Quản trị PostgreSQL |
| MinIO Console | http://localhost:9001 | Quản lý object storage |
| Prometheus | http://localhost:9090 | Metrics |
| Alertmanager | http://localhost:9093 | Alert management |
| Mock API | http://localhost:5055 | REST API mô phỏng nguồn tài chính |

Thông tin đăng nhập mặc định cho môi trường demo được cấu hình trong `docker-compose.yml`.

## 🧪 Dữ liệu mẫu

Project cung cấp script để tạo dữ liệu mô phỏng cho các nguồn:

```bash
docker exec -it airflow-webserver \
  python /opt/airflow/scripts/generate_sample_data.py
```

Có thể sử dụng script inject lỗi để kiểm thử cơ chế Data Quality:

```bash
docker exec -it airflow-webserver \
  python /opt/airflow/scripts/inject_errors.py
```

Sau khi inject lỗi, trigger lại DAG để quan sát việc Great Expectations phát hiện dữ liệu không hợp lệ.

## ▶️ Chạy ETL bằng CLI

Ngoài Airflow DAG, project có CLI runner:

```bash
python scripts/run_etl.py --mode full
```

### Full run

Chạy toàn bộ quy trình:

```text
Extract → Validate → Transform → Load
```

### Incremental run

```bash
python scripts/run_etl.py --mode incremental --hoc-ky HK1-2024-25
```

### Resume

Khôi phục pipeline từ một run đã lưu trong MinIO:

```bash
python scripts/run_etl.py --mode resume --run-id <RUN_ID>
```

## 📊 Monitoring

Hệ thống sử dụng:

**Prometheus**

- Thu thập metrics từ Airflow và ETL.
- Theo dõi số lượng record extract/load.
- Theo dõi validation result.
- Theo dõi trạng thái pipeline.

**Grafana**

- Dashboard kỹ thuật cho pipeline.
- Dashboard nghiệp vụ từ dữ liệu warehouse.

**Alertmanager**

- Quản lý cảnh báo từ Prometheus.
- Có thể chuyển cảnh báo tới Discord thông qua service proxy.

## 🔄 ETL flow chi tiết

### Extract

Dữ liệu được lấy từ ba nguồn:

```text
PostgreSQL ──┐
CSV ─────────┼──→ Extract → MinIO Raw
REST API ────┘
```

Mỗi lần chạy được gắn một `run_id` để theo dõi dữ liệu theo từng pipeline run.

### Validate

Great Expectations kiểm tra dữ liệu trước khi transform.

Các nhóm dữ liệu được kiểm tra bao gồm dữ liệu sinh viên, học tập, CTSV, tài chính và warehouse tùy theo suite được cấu hình.

### Transform

Các bước chính gồm:

- Chuẩn hóa dữ liệu.
- Xử lý dữ liệu trùng lặp.
- Tính toán GPA.
- Phân loại kết quả học tập.
- Chuẩn hóa dữ liệu từ nhiều nguồn.
- Phát hiện thay đổi cho SCD Type 2.
- Chuẩn bị dữ liệu cho warehouse.

### Load

Dữ liệu được nạp vào PostgreSQL Data Warehouse theo Star Schema.

Quá trình load bao gồm:

- Upsert dimension.
- Xử lý SCD Type 2 cho sinh viên.
- Load fact tables.
- Tạo/cập nhật aggregate.
- Ghi nhận metrics của quá trình load.

## 📈 Use cases

Dữ liệu warehouse có thể được sử dụng để hỗ trợ:

### Cảnh báo học vụ

Kết hợp kết quả học tập, tín chỉ không đạt và các chỉ số liên quan để xác định sinh viên cần được theo dõi.

### Hỗ trợ xét học bổng

Kết hợp:

```text
GPA
+ Điểm rèn luyện
+ Tình trạng tài chính
+ Thông tin học tập
```

### Báo cáo học vụ

Phân tích dữ liệu theo:

- Sinh viên.
- Học phần.
- Giảng viên.
- Học kỳ.
- Ngành / khoa.
- Kết quả học tập.
- Tình trạng tài chính.

## 🔧 Một số lệnh hữu ích

Xem log:

```bash
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-webserver
```

Xem toàn bộ container:

```bash
docker compose ps
```

Dừng hệ thống:

```bash
docker compose down
```

Dừng và xóa cả volume dữ liệu:

```bash
docker compose down -v
```

> ⚠️ `docker compose down -v` sẽ xóa các volume PostgreSQL, MinIO, Grafana, Prometheus và pgAdmin của project. Chỉ sử dụng khi muốn reset môi trường.

## 📚 Tài liệu liên quan

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [MinIO Documentation](https://min.io/docs/minio/linux/index.html)
- [Great Expectations Documentation](https://docs.greatexpectations.io/)
- [Prometheus Documentation](https://prometheus.io/docs/)
- [Grafana Documentation](https://grafana.com/docs/)

## 👥 Thành viên

**Nhóm 8 — Học viện Công nghệ Bưu chính Viễn thông**

- Vũ Hoàng Phúc
- Đỗ Minh Hoàng

## 📄 License

Project được xây dựng cho mục đích **học tập và nghiên cứu**.
