# SkyStream Analytics 🛫

**Real-time Flight Tracking & Business Intelligence Platform**

Streaming data pipeline theo kiến trúc **Medallion (Bronze → Silver → Gold)** xây dựng trên **AWS + Databricks**, sử dụng dữ liệu ADS-B thật từ OpenSky Network. CI/CD hoàn toàn tự động qua **Databricks Asset Bundle (DAB) + GitHub Actions**.

---

## 🏗️ Kiến Trúc

```
OpenSky Network API (real-time ADS-B, cập nhật mỗi 10 giây)
        │ poll mỗi phút (AWS Lambda)
        ▼
Amazon Kinesis Data Streams (2 shards)
        │
        ▼
Kinesis Firehose → S3 /bronze/raw_states/ (GZIP, buffer 60s)
        │
        ▼
Databricks Delta Live Tables — CONTINUOUS mode
  🥉 Bronze: raw_flight_states
  🥈 Silver: flights (cleaned, enriched, typed)
  🥇 Gold:   11 bảng analytics (market share, cargo, tourism, congestion...)
        │
        ▼
Databricks SQL Dashboard (auto-refresh 30s) + Alerts
```

---

## 📁 Cấu Trúc Project

```
databrick/
├── databricks.yml                  ← Databricks Asset Bundle config (CI/CD entry point)
├── .github/workflows/deploy.yml    ← GitHub Actions CI/CD pipeline
├── infra/
│   ├── main.tf                     ← Terraform: S3, Kinesis, Lambda, IAM, Firehose
│   ├── variables.tf                ← Khai báo biến
│   └── outputs.tf                  ← Output sau khi terraform apply
├── ingestion/lambda_producer/
│   ├── handler.py                  ← Poll OpenSky → gửi Kinesis
│   └── requirements.txt
├── notebooks/
│   ├── 01_bronze_autoloader.py     ← Auto Loader → bronze.raw_flight_states
│   ├── 02_silver_transform.py      ← Cleaning & enrichment → silver.flights
│   ├── 03_gold_analytics.py        ← 11 Gold streaming queries
│   └── 04_dlt_pipeline.py          ← Delta Live Tables (dùng cho DAB deploy)
├── reference/airports_loader.py    ← One-time: load airports CSV → Delta table
├── dashboards/skystream_dashboard.json
├── tests/
│   ├── conftest.py
│   ├── test_silver_transform.py
│   ├── test_gold_aggregation.py
│   └── requirements-test.txt
└── plan.md
```

---

## ✅ Yêu Cầu Đã Cài Trên Máy

| Công cụ | Kiểm tra | Phiên bản cần |
|---------|----------|---------------|
| Python | `python3 --version` | ≥ 3.9 |
| Java | `java -version` | **17** (bắt buộc cho PySpark 4.x) |
| Terraform | `terraform -version` | ≥ 1.5 |
| Databricks CLI | `databricks -version` | ≥ 0.200 |
| AWS CLI | `aws --version` | ≥ 2.0 |
| Git | `git --version` | bất kỳ |

---

## 🚀 Hướng Dẫn Deploy — Từng Bước

---

### BƯỚC 1 — Kiểm Tra Môi Trường

Mở **terminal mới** (để `.zshrc` load JAVA_HOME) rồi chạy:

```bash
java -version    # → openjdk version "17.x.x"
terraform -version  # → Terraform v1.5.x
databricks -version # → Databricks CLI v0.2xx
aws --version    # → aws-cli/2.x.x
```

Nếu `java` chưa nhận:
```bash
source ~/.zshrc
java -version
```

---

### BƯỚC 2 — Tạo AWS Account & Cấu Hình Credentials

#### 2a. Tạo AWS Account
- Vào https://aws.amazon.com/free → đăng ký (có Free Tier 12 tháng)

#### 2b. Tạo IAM User
1. Đăng nhập **AWS Console** → gõ `IAM` trên thanh tìm kiếm → **Users** → **Create user**
2. **User name**: `skystream-terraform`
3. Nhấn **Next** → chọn **Attach policies directly** → tick **AdministratorAccess** → **Next** → **Create user**
4. Click vào user vừa tạo → tab **Security credentials** → **Create access key**
5. Chọn **Command Line Interface (CLI)** → tick xác nhận → **Next** → **Create access key**
6. **Copy và lưu lại** `Access key ID` và `Secret access key` (chỉ hiện 1 lần)

#### 2c. Cấu Hình AWS CLI
```bash
aws configure
```
Điền lần lượt:
```
AWS Access Key ID [None]:     <dán Access key ID vừa copy>
AWS Secret Access Key [None]: <dán Secret access key vừa copy>
Default region name [None]:   us-east-1
Default output format [None]: json
```

Kiểm tra:
```bash
aws sts get-caller-identity
# Kết quả mong đợi:
# {
#     "UserId": "AIDA...",
#     "Account": "123456789012",   ← ghi nhớ Account ID này
#     "Arn": "arn:aws:iam::123456789012:user/skystream-terraform"
# }
```

---

### BƯỚC 3 — Đăng Ký OpenSky Network

1. Vào https://opensky-network.org → **Register** ở góc trên phải
2. Điền **Username**, **Email**, **Password** → **Register**
3. Kiểm tra email → click link xác nhận
4. **Lưu lại** username và password (dùng ở Bước 5)

Test ngay (không cần đăng nhập):
```bash
curl -s "https://opensky-network.org/api/states/all?lamin=8&lomin=100&lamax=24&lomax=109" \
  | python3 -c "
import json, sys
d = json.load(sys.stdin)
print(f'Số máy bay qua Việt Nam lúc này: {len(d[\"states\"])}')
for s in d['states'][:3]:
    print(f'  {str(s[1]).strip():<12} | {s[2]:<20} | Alt: {s[7]}m')
"
```

---

### BƯỚC 4 — Tạo Databricks Workspace

1. Vào https://www.databricks.com/try-databricks
2. Điền email → **Get Started Free**
3. Chọn **AWS** khi hỏi cloud provider
4. Chọn region **US East (N. Virginia) — us-east-1** (cùng region với S3)
5. Chờ workspace được tạo (~5 phút) → nhấn **Open Workspace**

#### 4a. Lấy Workspace URL
- Nhìn vào thanh địa chỉ trình duyệt: `https://dbc-eb803386-bec5.cloud.databricks.com/`
- **Copy và lưu lại** toàn bộ URL này

#### 4b. Tạo Personal Access Token (PAT)
1. Databricks workspace → click avatar góc **trên phải** → **Settings**
2. Chọn **Developer** → **Access tokens** → **Manage** → **Generate new token**
3. Điền:
   - **Comment**: `skystream-cli`
   - **Lifetime (days)**: `90`
   - **Scope**: chọn **Other APIs**
   - **API scope(s)**: tick các scope: `clusters`, `jobs`, `pipelines`, `dbfs`, `sql`, `files`
     *(hoặc tick `all APIs` nếu là workspace cá nhân)*
4. Click **Generate** → **copy token ngay** — dạng `dapi_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx` (chỉ hiện 1 lần)

5. Lưu vào nơi an toàn

> ℹ️ **External ID không bắt buộc** — project này không yêu cầu. Terraform sẽ tạo IAM role mà không cần External ID.

---

### BƯỚC 5 — Deploy AWS Infrastructure (Terraform)

```bash
cd /Users/admin/databrick/infra
terraform init
```

Tạo file biến cá nhân **(file này đã có trong `.gitignore`, KHÔNG bị commit lên git)**:
```bash
cat > terraform.tfvars << 'EOF'
aws_region       = "us-east-1"
s3_bucket_name   = "skystream-datalake-dev"
opensky_username = "bhuy71"
opensky_password = "@Huy123456"
EOF
```

> ⚠️ `s3_bucket_name` phải **globally unique** trên toàn AWS. Nếu bị lỗi "bucket already exists" → đổi thành `skystream-datalake-dev-<tên bạn>` VD: `skystream-datalake-dev-john`

Xem trước và deploy:
```bash
terraform plan    # xem những gì sẽ được tạo, không tạo gì cả

terraform apply   # gõ "yes" khi được hỏi — chờ ~2-3 phút
```

**Sau khi apply xong, copy toàn bộ output và lưu lại:**
```
Outputs:

databricks_iam_role_arn             = "arn:aws:iam::123456789012:role/skystream-databricks-role"
databricks_instance_profile_arn     = "arn:aws:iam::123456789012:instance-profile/skystream-databricks-role"
s3_bucket_name                      = "skystream-datalake-dev"
kinesis_stream_name                 = "flights-stream"
lambda_function_name                = "skystream-opensky-poller"
firehose_stream_name                = "skystream-flights-to-s3"
bronze_s3_path                      = "s3://skystream-datalake-dev/bronze/raw_states/"
```

```bash
cd ..  # quay về thư mục root
```

---

### BƯỚC 6 — Gắn IAM Role vào Databricks (Instance Profile)

1. Vào Databricks workspace → **⚙️ Settings** (góc trái dưới) → **Security**
2. Tìm mục **Instance profiles** → click **Add instance profile**
3. Trong ô **Instance profile ARN**, dán giá trị `databricks_instance_profile_arn` từ Terraform output
   - Dạng: `arn:aws:iam::020426224060:instance-profile/skystream-databricks-role`
4. Trong ô **IAM role ARN**, dán giá trị `databricks_iam_role_arn` từ Terraform output
   - Dạng: `arn:aws:iam::020426224060:role/skystream-databricks-role`
5. Bỏ tick **Skip validation** (để Databricks tự kiểm tra)
6. Nhấn **Add** → Instance profile xuất hiện trong danh sách là thành công ✅

---

### BƯỚC 7 — Xác Thực Databricks CLI

```bash
databricks configure
```
Điền:
```
Databricks host: https://dbc-xxxxxxxx-xxxx.cloud.databricks.com  ← URL từ Bước 4a
Token:           dapi_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx             ← PAT từ Bước 4b
```

Kiểm tra kết nối:
```bash
databricks clusters list
# Nếu trả về danh sách (kể cả rỗng) là đã kết nối thành công ✅
```

---

### BƯỚC 8 — Cập Nhật `databricks.yml` Với Thông Tin Cá Nhân

Mở file:
```bash
open -e /Users/admin/databrick/databricks.yml
# hoặc nếu dùng VS Code:
# code /Users/admin/databrick/databricks.yml
```

**Tìm và thay thế 5 chỗ trống** (tìm bằng Cmd+F với chuỗi `""`):

**Chỗ 1 — Workspace URL (target dev):**
```yaml
targets:
  dev:
    workspace:
      host: "https://dbc-xxxxxxxx-xxxx.cloud.databricks.com"   # ← dán URL Bước 4a
```

**Chỗ 2 — Workspace URL (target prod, có thể dùng cùng workspace):**
```yaml
  prod:
    workspace:
      host: "https://dbc-xxxxxxxx-xxxx.cloud.databricks.com"   # ← cùng URL hoặc workspace khác
```

**Chỗ 3 — Instance Profile ARN cho DLT pipeline cluster:**
```yaml
      clusters:
        - label: default
          aws_attributes:
            instance_profile_arn: "arn:aws:iam::123456789012:instance-profile/skystream-databricks-role"  # ← databricks_instance_profile_arn từ Terraform output
```

**Chỗ 4 — Instance Profile ARN cho airports_loader_job:**
```yaml
    airports_loader_job:
      tasks:
        - task_key: load_airports
          new_cluster:
            aws_attributes:
              instance_profile_arn: "arn:aws:iam::123456789012:instance-profile/skystream-databricks-role"  # ← databricks_instance_profile_arn từ Terraform output
```

**Chỗ 5 — Email nhận thông báo khi pipeline lỗi:**
```yaml
      notifications:
        - email_recipients:
            - "your@email.com"   # ← email của bạn
```

Lưu file lại.

---

### BƯỚC 9 — Chạy Unit Tests

```bash
cd /Users/admin/databrick
source .venv/bin/activate

cd tests
pytest -v
```

Kết quả mong đợi:
```
test_silver_transform.py::TestDataQualityFilters::test_null_latitude_dropped PASSED
test_silver_transform.py::TestFlightPhaseClassification::test_climbing_phase PASSED
...
========================= 29 passed in 6.49s =========================
```

```bash
cd ..   # quay về root
```

---

### BƯỚC 10 — Deploy Databricks Asset Bundle

```bash
# Validate — kiểm tra cú pháp và config (không cần kết nối Databricks)
databricks bundle validate --target dev

# Deploy — upload notebooks + tạo DLT pipeline + tạo Jobs trên Databricks
databricks bundle deploy --target dev
```

Kết quả mong đợi:
```
Uploading bundle files to /Shared/skystream-analytics/dev...
Deploying resources...
  Updating pipeline skystream_dlt_pipeline...
  Updating job airports_loader_job...
  Updating job skystream_streaming_job...
Successfully deployed!
```

Kiểm tra trên Databricks UI:
- **Workflows → Delta Live Tables** → thấy `SkyStream Analytics — DLT Pipeline [dev]` ✅
- **Workflows → Jobs** → thấy `SkyStream — Load Airports Reference [dev]` ✅

---

### BƯỚC 11 — Chạy Pipeline Lần Đầu

```bash
# Bước 11a: Load bảng airports reference (chỉ chạy 1 lần duy nhất)
databricks bundle run airports_loader_job --target dev
# Chờ job complete (~3-5 phút)

# Bước 11b: Khởi động DLT streaming pipeline (CONTINUOUS — chạy mãi mãi)
databricks bundle run skystream_dlt_pipeline --target dev
```

Theo dõi trên Databricks UI:
1. **Workflows → Delta Live Tables** → click vào pipeline
2. Thấy graph: `raw_flight_states` → `flights` → 11 Gold tables
3. Status từng node chuyển sang **Running** (xanh) ✅
4. Sau ~3 phút, số records bắt đầu tăng trên mỗi node

---

### BƯỚC 12 — Tạo SQL Dashboard

#### 12a. Tạo SQL Warehouse
1. Databricks → **SQL** (sidebar trái) → **SQL Warehouses** → **Create SQL Warehouse**
2. Điền:
   - **Name**: `skystream-warehouse`
   - **Type**: Serverless
   - **Size**: 2X-Small
3. **Create** → chờ warehouse start (~1 phút)

#### 12b. Tạo Dashboard
1. Databricks → **SQL** → **Dashboards** → **Create Dashboard**
2. Đặt tên: `SkyStream Analytics — Real-time`
3. Với mỗi widget trong `dashboards/skystream_dashboard.json`, nhấn **Add visualization** → chọn đúng SQL Warehouse → dán query vào

**12 queries chính cần tạo (copy từ `dashboards/skystream_dashboard.json`):**

| # | Tên Widget | Loại | Refresh |
|---|-----------|------|---------|
| 1 | ✈️ Aircraft in the Air | Counter | 30s |
| 2 | 🌍 Top Countries by Flights | Bar chart | 60s |
| 3 | 🔴 Flight Alerts | Table | 30s |
| 4 | 📊 Flight Phase Distribution | Pie chart | 60s |
| 5 | 📈 Hourly Traffic Trend | Line chart | 5 phút |
| 6 | 🏢 Airline Market Share | Bar chart | 60s |
| 7 | 📦 Cargo Flow by Carrier | Bar chart | 60s |
| 8 | 🛬 Airport Congestion | Table | 60s |
| 9 | 🏖️ Tourism Demand Signal | Bar chart | 2 phút |
| 10 | ⛽ Fuel Burn Estimate | Bar chart | 2 phút |
| 11 | 📉 Economic Activity Index | Table | 5 phút |
| 12 | 🗺️ Route Demand Surge Zones | Table | 2 phút |

#### 12c. Bật Auto-Refresh
1. Trên Dashboard → nhấn **⋮ (3 chấm)** → **Schedule**
2. Chọn **Refresh every 30 seconds**
3. **Save** → Dashboard tự cập nhật liên tục ✅

---

### BƯỚC 13 — Setup CI/CD GitHub Actions

#### 13a. Tạo GitHub Repository và Push Code

```bash
cd /Users/admin/databrick

# Cách 1: Dùng GitHub CLI (nếu đã cài)
gh auth login
gh repo create skystream-analytics --public --source=. --push

# Cách 2: Thủ công
# 1. Vào https://github.com/new
# 2. Repository name: skystream-analytics
# 3. KHÔNG tick "Add README", "Add .gitignore", "Add license"
# 4. Create repository → copy lệnh hiển thị
git remote add origin https://github.com/<your-username>/skystream-analytics.git
git branch -M main
git push -u origin main
```

#### 13b. Tạo Branch `develop`

```bash
git checkout -b develop
git push -u origin develop
```

> Từ đây: dev hằng ngày trên branch `develop`, merge vào `main` khi muốn deploy production.

#### 13c. Thêm Secrets vào GitHub

Vào GitHub repo → **Settings** (tab trên cùng) → **Secrets and variables** → **Actions** → **New repository secret**

Thêm lần lượt **7 secrets** sau:

| Secret Name | Giá trị | Lấy từ đâu |
|-------------|---------|------------|
| `DATABRICKS_HOST` | `https://dbc-xxxxxxxx-xxxx.cloud.databricks.com` | Bước 4a |
| `DATABRICKS_TOKEN` | `dapi_xxxxxxxxxxxxxxxx` | Bước 4b |
| `AWS_ACCESS_KEY_ID` | `AKIA...` | Bước 2b |
| `AWS_SECRET_ACCESS_KEY` | `xxxxxxxx` | Bước 2b |
| `TF_VAR_opensky_username` | username OpenSky | Bước 3 |
| `TF_VAR_opensky_password` | password OpenSky | Bước 3 |

> Cách thêm từng secret: **New repository secret** → điền **Name** → điền **Secret** → **Add secret**

#### 13d. Kiểm Tra CI/CD Chạy

Push 1 commit nhỏ để trigger workflow:
```bash
cd /Users/admin/databrick
git checkout develop
echo "# trigger ci" >> .trigger
git add .trigger && git commit -m "ci: trigger first workflow run"
git push origin develop
```

Vào GitHub repo → **Actions** tab → thấy workflow đang chạy:
```
✅ Validate Bundle        (~30 giây)
✅ Unit Tests             (~2 phút)
✅ Deploy AWS Infra       (~3 phút)  — chỉ chạy khi push, không chạy khi PR
✅ Deploy Databricks Bundle (~2 phút)
```

Xóa file trigger tạm:
```bash
git rm .trigger && git commit -m "chore: remove trigger file" && git push
```

---

## 🔄 Workflow Hằng Ngày (Sau Khi Setup Xong)

```bash
# Làm việc trên develop
git checkout develop

# Sửa code...

git add .
git commit -m "feat: mô tả thay đổi"
git push origin develop
# → GitHub Actions tự chạy: validate → test → deploy dev

# Khi muốn lên production
git checkout main
git merge develop
git push origin main
# → GitHub Actions tự chạy: validate → test → deploy prod
```

---

## 🔍 Kiểm Tra Pipeline Đang Chạy

### Kiểm tra data đang flow vào S3
```bash
aws s3 ls s3://skystream-datalake-dev/bronze/raw_states/ --recursive | tail -5
# Phải thấy file .gz mới trong vòng 60-90 giây gần nhất
```

### Kiểm tra Lambda đang hoạt động
```bash
aws logs tail /aws/lambda/skystream-opensky-poller --follow
# Phải thấy log dạng:
# [Poll 1/6] Fetching OpenSky states...
# [Poll 1/6] total=12500, valid=12300, published=12300
```

### Kiểm tra Kinesis có records
```bash
aws cloudwatch get-metric-statistics \
  --namespace AWS/Kinesis \
  --metric-name IncomingRecords \
  --dimensions Name=StreamName,Value=flights-stream \
  --start-time $(date -u -v-5M +%Y-%m-%dT%H:%M:%SZ) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%SZ) \
  --period 60 --statistics Sum
```

### Kiểm tra Delta tables trong Databricks
Chạy trong notebook hoặc SQL Editor:
```sql
-- Bronze đang tăng?
SELECT COUNT(*) AS total, MAX(_bronze_ingested_at) AS latest_record
FROM bronze.raw_flight_states;

-- Silver đang update real-time?
SELECT flight_phase, COUNT(*) AS cnt
FROM silver.flights
WHERE snapshot_time_ts >= current_timestamp() - INTERVAL 2 MINUTES
GROUP BY flight_phase ORDER BY cnt DESC;

-- Gold có insight?
SELECT airline_icao, active_aircraft, market_share_pct
FROM gold.airline_market_share
ORDER BY active_aircraft DESC LIMIT 10;

-- Cảnh báo nào đang active?
SELECT alert_type, callsign, origin_country, altitude_m, vertical_rate, alert_generated_at
FROM gold.flight_alerts
ORDER BY alert_generated_at DESC LIMIT 10;
```

---

## ⚡ End-to-End Latency

| Giai đoạn | Thời gian |
|-----------|-----------|
| OpenSky cập nhật → Lambda nhận | ~10 giây |
| Lambda → Kinesis | ~1-2 giây |
| Kinesis → S3 (Firehose buffer) | **tối đa 60 giây** ← bottleneck chính |
| S3 → Bronze (Auto Loader) | ~30 giây |
| Bronze → Silver → Gold (DLT) | ~20-30 giây |
| **Tổng end-to-end** | **~2-3 phút** |

---

## 🔧 Xử Lý Lỗi Thường Gặp

### Lambda không gửi được data
```bash
# Xem log Lambda
aws logs tail /aws/lambda/skystream-opensky-poller --follow

# Lỗi thường gặp:
# "rate limit reached" → thêm OpenSky credentials vào Lambda env vars trong AWS Console
# "AccessDeniedException" → kiểm tra IAM policy của Lambda role
```

### `terraform apply` lỗi "BucketAlreadyExists"
```
# Đổi tên bucket trong terraform.tfvars thành tên unique hơn
s3_bucket_name = "skystream-datalake-dev-yourname-2024"
```

### `databricks bundle deploy` lỗi "Host not configured"
```bash
# Kiểm tra host đã được điền trong databricks.yml chưa
grep "host:" /Users/admin/databrick/databricks.yml

# Hoặc set qua env var
export DATABRICKS_HOST="https://dbc-xxx.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi_xxx"
databricks bundle deploy --target dev
```

### DLT Pipeline lỗi "InstanceProfileArn not found"
- Kiểm tra `instance_profile_arn` trong `databricks.yml` đã được điền đúng ARN từ Terraform output chưa
- Kiểm tra IAM Role đã được add vào Databricks workspace ở Bước 6 chưa

### Auto Loader không nhận file mới
```bash
# Kiểm tra Firehose có đang ghi xuống S3 không
aws firehose describe-delivery-stream \
  --delivery-stream-name skystream-flights-to-s3 \
  --query 'DeliveryStreamDescription.DeliveryStreamStatus'
# Phải trả về "ACTIVE"
```

---

## 💰 Chi Phí Ước Tính

| Dịch vụ | Free Tier | Chi phí dev/tháng |
|---------|-----------|-------------------|
| Kinesis Data Streams (2 shards) | 1 shard free/12 tháng | ~$22 |
| Kinesis Firehose | 5GB/tháng free | ~$0.50 |
| S3 (~10GB) | 5GB free | ~$0.50 |
| AWS Lambda | 1M invocations free | ~$0 |
| Databricks (trial) | 14 ngày free | ~$0 → ~$40/tháng sau |
| **Tổng** | **~$0 trong free tier** | **~$20-60/tháng** |

> 💡 Để tiết kiệm: tắt Lambda trigger khi không dùng (`aws events disable-rule --name skystream-poll-every-minute`), DLT cluster tự terminate khi không có data.

---

## 📚 Tài Liệu Tham Khảo

- [Databricks Asset Bundle docs](https://docs.databricks.com/en/dev-tools/bundles/index.html)
- [Delta Live Tables](https://docs.databricks.com/en/delta-live-tables/index.html)
- [Auto Loader](https://docs.databricks.com/en/ingestion/auto-loader/index.html)
- [OpenSky Network API](https://openskynetwork.github.io/opensky-api/)
- [Terraform AWS Provider](https://registry.terraform.io/providers/hashicorp/aws/latest/docs)
