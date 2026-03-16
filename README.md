# SkyStream Analytics 🛫

**Real-time Flight Tracking & Business Intelligence Platform**

Streaming data pipeline xây dựng trên **AWS + Databricks** theo kiến trúc **Medallion (Bronze → Silver → Gold)**, sử dụng dữ liệu ADS-B thật từ OpenSky Network. Triển khai CI/CD hoàn toàn tự động qua **Databricks Asset Bundle (DAB)**.

---

## 🏗️ Kiến Trúc Tổng Quan

```
OpenSky Network API (real-time, 10s refresh)
        │
        ▼
AWS Lambda (poll mỗi phút)
        │
        ▼
Amazon Kinesis Data Streams
        │
        ▼
Kinesis Firehose → S3 /bronze/raw_states/ (GZIP, 60s buffer)
        │
        ▼
Databricks Delta Live Tables (CONTINUOUS mode)
  🥉 Bronze  →  🥈 Silver  →  🥇 Gold (11 bảng analytics)
        │
        ▼
Databricks SQL Dashboard (auto-refresh 30s)
```

---

## 📁 Cấu Trúc Project

```
databrick/
├── databricks.yml              ← Databricks Asset Bundle (DAB) config
├── .github/
│   └── workflows/
│       └── deploy.yml          ← CI/CD GitHub Actions
├── infra/
│   ├── main.tf                 ← Terraform: S3, Kinesis, Lambda, IAM, Firehose
│   ├── variables.tf
│   └── outputs.tf
├── ingestion/
│   └── lambda_producer/
│       ├── handler.py          ← Lambda: poll OpenSky → push Kinesis
│       └── requirements.txt
├── notebooks/
│   ├── 01_bronze_autoloader.py ← Auto Loader → bronze.raw_flight_states
│   ├── 02_silver_transform.py  ← Cleaning & enrichment → silver.flights
│   ├── 03_gold_analytics.py    ← 11 Gold streaming queries
│   └── 04_dlt_pipeline.py      ← Delta Live Tables (all-in-one, dùng cho DAB)
├── reference/
│   └── airports_loader.py      ← One-time: load airports CSV → reference.airports
├── dashboards/
│   └── skystream_dashboard.json← Dashboard widget definitions (12 widgets, 3 alerts)
├── tests/
│   ├── conftest.py
│   ├── test_silver_transform.py
│   ├── test_gold_aggregation.py
│   └── requirements-test.txt
└── plan.md
```

---

## ✅ Yêu Cầu Trước Khi Bắt Đầu

| Công cụ | Phiên bản | Link cài đặt |
|---------|-----------|--------------|
| Python | ≥ 3.9 | https://python.org |
| Java | 11 hoặc 17 | `brew install openjdk@11` |
| Terraform | ≥ 1.0 | https://developer.hashicorp.com/terraform/install |
| Databricks CLI | ≥ 0.200 | Xem bên dưới |
| AWS CLI | ≥ 2.0 | https://aws.amazon.com/cli |
| Git | bất kỳ | https://git-scm.com |

---

## 🚀 Hướng Dẫn Triển Khai Từng Bước

### BƯỚC 0 — Clone & Cài đặt môi trường local

```bash
git clone <your-repo-url>
cd databrick

# Tạo virtual environment
python3 -m venv .venv
source .venv/bin/activate       # macOS/Linux
# .venv\Scripts\activate        # Windows

# Cài test dependencies
pip install -r tests/requirements-test.txt

# Cài Databricks CLI
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
databricks --version            # kiểm tra: v0.200+
```

---

### BƯỚC 1 — Chuẩn Bị Tài Khoản

#### 1a. AWS Account
- Tạo tại https://aws.amazon.com/free (có Free Tier 12 tháng)
- Cài AWS CLI: `brew install awscli`
- Cấu hình credentials:
  ```bash
  aws configure
  # AWS Access Key ID: <nhập key>
  # AWS Secret Access Key: <nhập secret>
  # Default region name: us-east-1
  ```

#### 1b. OpenSky Network (nguồn dữ liệu thật)
- Đăng ký miễn phí: https://opensky-network.org
- Sau khi có account: rate limit tăng từ 400 → 4,000 requests/day
- **Test ngay** (không cần account):
  ```bash
  curl -s "https://opensky-network.org/api/states/all?lamin=8&lomin=100&lamax=24&lomax=109" \
    | python3 -c "
  import json, sys
  d = json.load(sys.stdin)
  print(f'Máy bay qua Việt Nam lúc này: {len(d[\"states\"])}')
  for s in d['states'][:5]:
      print(f'  {s[1] or \"N/A\":<12} | {s[2]:<20} | Lat:{s[6]:.2f} Lon:{s[5]:.2f} | Alt:{s[7]}m')
  "
  ```

#### 1c. Databricks Workspace
- Free trial 14 ngày: https://www.databricks.com/try-databricks
- Chọn **AWS** khi được hỏi cloud provider
- Sau khi tạo xong, lấy:
  - **Workspace URL**: Settings → ở thanh địa chỉ trình duyệt (VD: `https://dbc-xxx.cloud.databricks.com`)
  - **Personal Access Token (PAT)**: User Settings → Access Tokens → Generate New Token

---

### BƯỚC 2 — Deploy Hạ Tầng AWS (Terraform)

```bash
cd infra

# Khởi tạo Terraform
terraform init

# Tạo file biến (KHÔNG commit file này lên git)
cat > terraform.tfvars << 'EOF'
aws_region             = "us-east-1"
s3_bucket_name         = "skystream-datalake-dev"   # phải globally unique
opensky_username       = ""       # optional: OpenSky username
opensky_password       = ""       # optional: OpenSky password
databricks_external_id = ""       # lấy từ Databricks workspace Settings > AWS
EOF

# Xem trước những gì sẽ được tạo
terraform plan

# Tạo tất cả resources (~2-3 phút)
terraform apply
```

Sau khi apply xong, lưu lại output này:
```
Outputs:
  databricks_iam_role_arn = "arn:aws:iam::123456789:role/skystream-databricks-role"
  s3_bucket_name          = "skystream-datalake-dev"
  kinesis_stream_name     = "flights-stream"
  bronze_s3_path          = "s3://skystream-datalake-dev/bronze/raw_states/"
```

```bash
cd ..  # quay về root
```

---

### BƯỚC 3 — Cấu Hình Databricks Workspace

#### 3a. Gắn IAM Role vào Databricks
1. Đăng nhập Databricks workspace
2. Vào **Settings → Security → IAM Role** (hoặc **Admin Console → AWS → Instance Profiles**)
3. Nhập ARN từ Terraform output: `databricks_iam_role_arn`
4. Nhấn **Add**

#### 3b. Xác thực Databricks CLI
```bash
# Cách 1: OAuth (khuyến nghị)
databricks auth login --host https://your-workspace.cloud.databricks.com

# Cách 2: PAT token
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi_xxxxxxxxxxxxx"

# Kiểm tra kết nối
databricks clusters list
```

#### 3c. Cập nhật databricks.yml
Mở file `databricks.yml`, điền các giá trị còn trống:

```yaml
targets:
  dev:
    workspace:
      host: "https://your-workspace.cloud.databricks.com"   # ← điền vào đây
    ...

resources:
  pipelines:
    skystream_dlt_pipeline:
      clusters:
        - label: default
          aws_attributes:
            instance_profile_arn: "arn:aws:iam::123456789:role/skystream-databricks-role"  # ← từ terraform output
```

---

### BƯỚC 4 — Chạy Unit Tests (Local)

> Yêu cầu: Java 11+ đã cài (`java -version`)

```bash
# macOS: cài Java nếu chưa có
brew install openjdk@11
export JAVA_HOME=$(brew --prefix openjdk@11)

# Chạy tests
source .venv/bin/activate
cd tests
pytest -v

# Kết quả mong đợi:
# test_silver_transform.py::TestDataQualityFilters::test_null_latitude_dropped PASSED
# test_silver_transform.py::TestFlightPhaseClassification::test_climbing_phase PASSED
# ... (29 tests total, all PASSED)
```

---

### BƯỚC 5 — Deploy Databricks Asset Bundle

```bash
# Validate cấu hình (không cần connect Databricks)
databricks bundle validate --target dev

# Deploy lên môi trường dev (upload notebooks + tạo DLT pipeline + tạo Jobs)
databricks bundle deploy --target dev

# Sau deploy, chạy job load airports reference (chỉ cần 1 lần)
databricks bundle run airports_loader_job --target dev

# Khởi động DLT pipeline streaming (CONTINUOUS mode)
databricks bundle run skystream_dlt_pipeline --target dev
```

Kiểm tra pipeline đang chạy:
1. Vào Databricks workspace → **Workflows → Delta Live Tables**
2. Tìm pipeline **"SkyStream Analytics — DLT Pipeline [dev]"**
3. Status phải là **Running** (màu xanh)
4. Bạn sẽ thấy data flow: `raw_flight_states` → `flights` → 11 Gold tables

---

### BƯỚC 6 — Tạo Dashboard Real-time

#### 6a. Tạo SQL Warehouse
1. Databricks → **SQL → SQL Warehouses → Create**
2. Chọn **Serverless** (tự động scale, không cần quản lý)
3. Size: `2X-Small` là đủ cho dashboard

#### 6b. Tạo Dashboard từ file định nghĩa
1. Databricks → **SQL → Dashboards → Create Dashboard**
2. Đặt tên: `SkyStream Analytics`
3. Thêm lần lượt 12 queries từ file `dashboards/skystream_dashboard.json`

**12 widgets trong dashboard:**

| Widget | Loại | Refresh |
|--------|------|---------|
| ✈️ Aircraft in the Air Right Now | Counter | 30s |
| 🌍 Top 15 Countries by Active Flights | Bar chart | 60s |
| 🔴 Flight Alerts (Rapid Descent) | Table | 30s |
| 📊 Flight Phase Distribution | Pie chart | 60s |
| 📈 Hourly Traffic Trend (24h) | Line chart | 5 phút |
| 🏢 Live Airline Market Share | Bar chart | 60s |
| 📦 Cargo Flow by Carrier | Bar chart | 60s |
| 🛬 Airport Congestion Scores | Table | 60s |
| 🏖️ Tourism Demand by Destination | Bar chart | 2 phút |
| ⛽ Estimated Fuel Burn by Country | Bar chart | 2 phút |
| 📉 Economic Activity Index | Table | 5 phút |
| 🗺️ Route Demand Surge Zones | Table | 2 phút |

#### 6c. Bật Auto-Refresh
1. Trong Dashboard, nhấn nút **Schedule**
2. Set **Auto-refresh: 30 seconds**
3. Dashboard sẽ tự cập nhật mà không cần F5

#### 6d. Tạo Alerts (Cảnh báo tự động)
1. Databricks SQL → **Alerts → Create Alert**
2. Tạo 3 alerts theo file `dashboards/skystream_dashboard.json`:
   - **Rapid Descent Spike**: > 5 sự kiện hạ độ cao đột ngột / 5 phút → gửi email
   - **Airport Critical Congestion**: congestion_level = CRITICAL → gửi email
   - **Tourism Surge**: inbound > 20 flights / 15 phút → gửi email

---

### BƯỚC 7 — Cài Đặt CI/CD với GitHub Actions

#### 7a. Tạo GitHub Repository
```bash
git init
git add .
git commit -m "feat: initial SkyStream Analytics implementation"
git remote add origin https://github.com/your-username/skystream-analytics.git
git push -u origin main
```

#### 7b. Thêm Secrets vào GitHub
Vào GitHub repo → **Settings → Secrets and variables → Actions → New repository secret**:

| Secret Name | Giá trị |
|-------------|---------|
| `DATABRICKS_HOST` | `https://your-workspace.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | PAT token từ Databricks User Settings |
| `AWS_ACCESS_KEY_ID` | AWS IAM User access key |
| `AWS_SECRET_ACCESS_KEY` | AWS IAM User secret key |
| `TF_VAR_opensky_username` | OpenSky username (optional) |
| `TF_VAR_opensky_password` | OpenSky password (optional) |
| `TF_VAR_databricks_external_id` | External ID từ Databricks workspace |

#### 7c. Tạo Branch Strategy
```bash
# Branch develop → deploy môi trường dev
git checkout -b develop
git push origin develop

# Branch main → deploy môi trường prod
# (chỉ merge vào main sau khi test kỹ trên develop)
```

#### 7d. CI/CD Flow hoạt động như sau:

```
Developer push code
        │
        ▼
GitHub Actions trigger
        │
        ├─ [Mọi PR/push] Validate bundle (databricks bundle validate)
        │
        ├─ [Mọi PR/push] Unit Tests (pytest với PySpark local)
        │
        ├─ [Push vào develop] → Terraform apply → Bundle deploy → dev
        │
        └─ [Push vào main]   → Terraform apply → Bundle deploy → prod
                                                        │
                                                        ▼
                                              DLT Pipeline restart
                                              (CONTINUOUS mode, auto-recover)
```

---

## 📊 Kiểm Tra Pipeline Đang Chạy

### Kiểm tra data đang flow
```bash
# Kiểm tra S3 đang nhận data
aws s3 ls s3://skystream-datalake-dev/bronze/raw_states/ --recursive | tail -5

# Kiểm tra Lambda đang chạy
aws logs tail /aws/lambda/skystream-opensky-poller --follow

# Kiểm tra Kinesis metrics
aws cloudwatch get-metric-statistics \
  --namespace AWS/Kinesis \
  --metric-name IncomingRecords \
  --dimensions Name=StreamName,Value=flights-stream \
  --start-time $(date -u -v-10M +%Y-%m-%dT%H:%M:%SZ) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%SZ) \
  --period 60 \
  --statistics Sum
```

### Kiểm tra Delta Tables trong Databricks
Chạy trong Databricks notebook:
```sql
-- Kiểm tra Bronze đang tăng
SELECT COUNT(*) as total, MAX(_bronze_ingested_at) as latest FROM bronze.raw_flight_states;

-- Kiểm tra Silver
SELECT flight_phase, COUNT(*) as cnt
FROM silver.flights
WHERE snapshot_time_ts >= current_timestamp() - INTERVAL 2 MINUTES
GROUP BY flight_phase;

-- Kiểm tra Gold alerts
SELECT * FROM gold.flight_alerts
ORDER BY alert_generated_at DESC
LIMIT 10;

-- Kiểm tra market share
SELECT airline_icao, active_aircraft, market_share_pct
FROM gold.airline_market_share
WHERE window_start >= current_timestamp() - INTERVAL 10 MINUTES
ORDER BY active_aircraft DESC
LIMIT 10;
```

---

## ⚡ End-to-End Latency

| Stage | Latency |
|-------|---------|
| OpenSky cập nhật dữ liệu | Mỗi 10 giây |
| Lambda poll → Kinesis | +10–15 giây |
| Kinesis Firehose → S3 | +60 giây (buffer) |
| Auto Loader → Bronze table | +30 giây |
| Bronze → Silver (DLT) | +10–15 giây |
| Silver → Gold (DLT) | +10–15 giây |
| **Total end-to-end** | **~2–3 phút** |

Dashboard refresh 30 giây → người dùng thấy dữ liệu **cũ nhất 3–4 phút**.

---

## 💰 Chi Phí Ước Tính

| Dịch vụ | Free Tier | Chi phí/tháng (dev) |
|---------|-----------|---------------------|
| Kinesis Data Streams (2 shards) | 1 shard free/12 tháng | ~$22 |
| Kinesis Firehose | 5GB free | ~$0.50 |
| S3 (~10GB) | 5GB free | ~$0.50 |
| AWS Lambda | 1M invocations free | ~$0 |
| Databricks (free trial) | 14 ngày | $0 → ~$40/tháng sau |
| **Tổng** | | **~$0 (trong free tier)** |

> 💡 **Tiết kiệm chi phí**: Chỉ chạy Lambda + Kinesis khi muốn collect data. DLT cluster có auto-terminate. S3 lifecycle rule tự xóa file Bronze sau 30 ngày.

---

## 🛠️ Tech Stack

```
DATA SOURCE    : OpenSky Network REST API (ADS-B, miễn phí, 10s refresh)
INGESTION      : AWS Lambda + Amazon Kinesis Data Streams + Kinesis Firehose
STORAGE        : Amazon S3 + Delta Lake (Parquet + transaction log)
PROCESSING     : Databricks Structured Streaming + Delta Live Tables (DLT)
CI/CD          : Databricks Asset Bundle (DAB) + GitHub Actions
ORCHESTRATION  : Databricks Workflows
SERVING        : Databricks SQL Dashboard (auto-refresh 30s) + Databricks Alerts
MONITORING     : AWS CloudWatch + Databricks DLT UI + Databricks Lakehouse Monitoring
INFRA AS CODE  : Terraform
TESTING        : pytest + PySpark local mode
ARCHITECTURE   : Medallion (Bronze → Silver → Gold)
```

---

## 🔧 Troubleshooting

### Lambda không gửi data vào Kinesis
```bash
# Xem logs Lambda
aws logs tail /aws/lambda/skystream-opensky-poller --follow
# Nếu thấy "rate limit reached" → thêm OpenSky username/password vào Lambda env vars
```

### DLT Pipeline bị lỗi "Schema evolution"
```python
# Trong DLT notebook, thêm option:
.option("cloudFiles.schemaEvolutionMode", "addNewColumns")
```

### Auto Loader không nhận file mới
```bash
# Kiểm tra Firehose delivery status
aws firehose describe-delivery-stream --delivery-stream-name skystream-flights-to-s3
# Nếu thấy "ACTIVE" nhưng không có file mới → kiểm tra Kinesis có records không
```

### databricks bundle deploy thất bại
```bash
# Kiểm tra authentication
databricks auth env --host https://your-workspace.cloud.databricks.com

# Kiểm tra bundle syntax
databricks bundle validate --target dev

# Deploy với verbose logs
databricks bundle deploy --target dev --debug
```

---

## 📚 Tài Liệu Tham Khảo

- [Databricks Asset Bundle](https://docs.databricks.com/en/dev-tools/bundles/index.html)
- [Delta Live Tables](https://docs.databricks.com/en/delta-live-tables/index.html)
- [Auto Loader](https://docs.databricks.com/en/ingestion/auto-loader/index.html)
- [OpenSky Network API](https://openskynetwork.github.io/opensky-api/)
- [Kinesis Firehose → S3](https://docs.aws.amazon.com/firehose/latest/dev/basic-deliver.html)
- [Terraform AWS Provider](https://registry.terraform.io/providers/hashicorp/aws/latest/docs)
