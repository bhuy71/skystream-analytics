# 🛫 KẾ HOẠCH DỰ ÁN: Real-time Flight Tracking & Delay Analytics Platform

## 📌 Tổng Quan Dự Án

### Tên dự án
**SkyStream Analytics** — Nền tảng theo dõi chuyến bay theo thời gian thực + Bộ công cụ phân tích kinh doanh & đầu tư hàng không

### Lý do chọn dự án này
- ✅ **Dữ liệu thật, miễn phí**: OpenSky Network cung cấp vị trí máy bay toàn cầu 24/7 (ADS-B data)
- ✅ **Ai cũng hiểu giá trị ngay lập tức**: Trễ chuyến, sân bay tắc nghẽn, hàng hóa đang ở đâu — mọi người đều thấy lợi ích rõ ràng
- ✅ **Latency thấp**: Dữ liệu cập nhật mỗi 10 giây
- ✅ **Ứng dụng kinh doanh cực cao**: Từ cá nhân (đặt vé) đến tổ chức (quỹ đầu tư, hãng bay, sân bay)
- ✅ **Medallion Architecture hoàn chỉnh**: Bronze → Silver → Gold rõ ràng từng tầng

---

## 🏗️ Kiến Trúc Hệ Thống (Medallion Architecture trên AWS + Databricks)

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCE (REAL-TIME)                      │
│   OpenSky Network REST API (https://opensky-network.org/api)        │
│   • State vectors: vị trí, tốc độ, độ cao tất cả máy bay           │
│   • ~10,000–15,000 máy bay online cùng lúc, cập nhật mỗi 10 giây  │
│   • Miễn phí hoàn toàn, không cần API key (anonymous OK)           │
└──────────────────────────┬──────────────────────────────────────────┘
                           │ HTTP polling mỗi 10 giây (Lambda)
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     INGESTION LAYER (AWS)                           │
│  AWS Lambda (opensky-poller) → Kinesis Data Streams (2 shards)     │
│  Kinesis Firehose → S3 /bronze/raw_states/ (GZIP, 60s buffer)      │
│  EventBridge Scheduler → trigger Lambda mỗi 1 phút                 │
└─────────────────────────────────────────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   PROCESSING LAYER (DATABRICKS)                     │
│                                                                     │
│  🥉 BRONZE — bronze.raw_flight_states                               │
│     Auto Loader (cloudFiles) đọc GZIP JSON từ S3, append-only      │
│                                                                     │
│  🥈 SILVER — silver.flights                                         │
│     Parse 17 fields, cast types, classify flight_phase             │
│     Extract airline_icao, flag cargo, convert units                 │
│     Filter null/invalid positions, dedup by icao24+snapshot_time   │
│                                                                     │
│  🥇 GOLD — 11 bảng analytics (xem chi tiết bên dưới)              │
│                                                                     │
│  ⚙️  Delta Live Tables (DLT) — CONTINUOUS mode                     │
└──────────────────────────┬──────────────────────────────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────────────┐
│              SERVING — Databricks SQL Dashboard                     │
│  11 widgets: live counter, bar charts, line charts, alert tables   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 🥇 Gold Layer — 11 Bảng Analytics

| # | Bảng | Window | Mô tả |
|---|------|--------|-------|
| 1 | `gold.airspace_density` | 1 phút | Số máy bay theo ô lưới 1°×1° |
| 2 | `gold.flight_phase_stats` | 5 phút | Phân phối ON_GROUND/CLIMBING/CRUISING/DESCENDING |
| 3 | `gold.flight_alerts` | Realtime | Cảnh báo hạ độ cao đột ngột (>15 m/s) |
| 4 | `gold.hourly_traffic` | 1 giờ | Tổng chuyến bay theo giờ/quốc gia |
| 5 | `gold.route_demand_index` | 15 phút | Chỉ số nhu cầu theo hành lang bay |
| 6 | `gold.airline_market_share` | 5 phút | Market share của từng hãng bay theo thời gian thực |
| 7 | `gold.cargo_flow` | 1 phút | Luồng máy bay hàng hóa (FedEx/UPS/DHL) theo khu vực |
| 8 | `gold.airport_congestion` | 5 phút | Điểm tắc nghẽn sân bay (máy bay đang tiếp cận/khởi hành) |
| 9 | `gold.tourism_demand_signal` | 15 phút | Tín hiệu nhu cầu du lịch theo điểm đến |
| 10 | `gold.fuel_burn_estimate` | 1 phút | Ước tính tiêu thụ nhiên liệu & phát thải CO₂ |
| 11 | `gold.economic_activity_index` | 1 giờ | Chỉ số hoạt động kinh tế dựa trên lưu lượng bay |

---

## 💼 Business & Investment Insights (Streaming)

Đây là phần mang lại **giá trị kinh tế thực sự** — các insight được tính toán real-time từ luồng dữ liệu bay:

### 1. 📈 Route Demand Index (`gold.route_demand_index`)
**Bài toán:** Nhà đầu tư muốn biết hành lang bay nào đang tăng trưởng mạnh để:
- Đầu tư vào cổ phiếu hãng bay đang mở rộng tuyến bay đó
- Mở khách sạn/nhà hàng tại các điểm đến đang có sóng du khách
- Xây dựng trung tâm logistics tại các hub đang tăng trưởng

**Cách tính:** Đếm số chuyến bay theo ô lưới 5°×5° trong cửa sổ 15 phút, so sánh với trung bình 7 ngày. Phân loại: `SURGE` (>150% avg), `NORMAL`, `DIP` (<70% avg)

---

### 2. ✈️ Airline Market Share (`gold.airline_market_share`)
**Bài toán:** Nhà đầu tư chứng khoán cần theo dõi:
- Hãng bay nào đang tăng/giảm số lượng khai thác thực tế so với kế hoạch
- Phát hiện sớm khi hãng bay cắt giảm đột ngột (dấu hiệu khủng hoảng tài chính)
- So sánh năng lực khai thác giữa các hãng cùng nhóm (full-service vs low-cost)

**Cách tính:** Trích xuất airline ICAO code từ callsign (3 ký tự đầu), đếm số máy bay đang bay của từng hãng trong cửa sổ 5 phút. Tính % thị phần và tốc độ thay đổi.

---

### 3. 📦 Cargo Flow Tracker (`gold.cargo_flow`)
**Bài toán:** Doanh nghiệp logistics & nhà đầu tư supply chain cần:
- Theo dõi lưu lượng hàng hóa qua các hub chính (Memphis/FedEx, Louisville/UPS)
- Phát hiện gián đoạn chuỗi cung ứng sớm hơn 24-48 tiếng so với báo cáo chính thức
- Ước tính nhu cầu e-commerce/retail dựa trên mật độ chuyến bay cargo

**Cách tính:** Lọc callsign của các hãng cargo lớn (FDX=FedEx, UPS=UPS, GTI=Atlas Air, DHL...), đếm theo khu vực/giờ, phân loại `SURGE`/`NORMAL`/`DIP`.

---

### 4. 🏨 Tourism Demand Signal (`gold.tourism_demand_signal`)
**Bài toán:** Khách sạn, công ty du lịch, nhà đầu tư bất động sản nghỉ dưỡng:
- Biết được sóng du khách đến Bali, Phuket, Dubai, v.v. TRƯỚC KHI khách sạn cảm nhận
- Điều chỉnh giá phòng động theo thời gian thực
- Nhà đầu tư phát hiện điểm đến mới nổi (emerging destinations) qua dữ liệu bay

**Cách tính:** Đếm máy bay đang hạ độ cao (DESCENDING, altitude <3000m) trong bounding box của 10 điểm đến du lịch nổi tiếng toàn cầu. Window 15 phút.

---

### 5. 🛬 Airport Congestion Score (`gold.airport_congestion`)
**Bài toán:** Doanh nghiệp phụ trợ sân bay (catering, ground handling, parking, F&B):
- Biết sân bay sắp bùng nổ để tăng ca nhân viên, chuẩn bị thực phẩm
- Dự đoán hàng đợi taxi/xe đón để tối ưu hóa đội xe
- Sân bay tính phí dynamic landing slot dựa trên congestion score thực tế

**Cách tính:** Đếm máy bay đang hạ cánh (DESCENDING) và cất cánh (CLIMBING) trong bán kính 100km quanh 20 sân bay lớn nhất thế giới. Congestion score = (tiếp cận + khởi hành) / capacity_baseline × 100

---

### 6. ⛽ Fuel Burn & CO₂ Estimate (`gold.fuel_burn_estimate`)
**Bài toán:** 
- **Airlines:** Theo dõi chi phí nhiên liệu của toàn bộ fleet theo thời gian thực
- **Fuel traders:** Ước tính nhu cầu Jet-A trên toàn cầu theo giờ để giao dịch hợp đồng tương lai
- **ESG investors:** Tính carbon footprint của ngành hàng không theo thời gian thực

**Cách tính:** Áp dụng mô hình đơn giản hóa: Cruising=8kg/phút/máy bay, Climbing=14kg/phút, Descending=4kg/phút, Taxi=3kg/phút. CO₂ = fuel × 3.16. Aggregate theo quốc gia và phase.

---

### 7. 📊 Economic Activity Index (`gold.economic_activity_index`)
**Bài toán:** Quỹ đầu tư macro, ngân hàng, chính phủ cần:
- Chỉ số leading indicator của nền kinh tế (lưu lượng bay thường đi trước GDP 1-2 tháng)
- So sánh phục hồi kinh tế post-COVID giữa các quốc gia
- Phát hiện sớm dấu hiệu suy thoái (giảm lưu lượng bay nội địa)

**Cách tính:** Tổng số chuyến bay active theo quốc gia trong cửa sổ 1 giờ, chuẩn hóa theo baseline (trung bình cùng giờ 7 ngày trước). Index = (current / baseline) × 100. >110 = tăng trưởng, <90 = suy giảm.

---

## 📋 WORKPLAN — Các Bước Triển Khai

### PHASE 1: Chuẩn Bị Hạ Tầng AWS (Terraform)
- [ ] **1.1** Cài đặt Terraform >= 1.0
- [ ] **1.2** Cấu hình AWS credentials (`aws configure`)
- [ ] **1.3** `cd infra && terraform init`
- [ ] **1.4** Tạo file `infra/terraform.tfvars` với các biến thực tế:
  ```hcl
  aws_region          = "us-east-1"
  s3_bucket_name      = "skystream-datalake-dev"
  opensky_username    = "your_opensky_username"  # optional
  opensky_password    = "your_opensky_password"  # optional
  databricks_external_id = "your_external_id"   # từ Databricks workspace
  ```
- [ ] **1.5** `terraform plan` → kiểm tra resources sẽ tạo
- [ ] **1.6** `terraform apply` → deploy toàn bộ infrastructure

### PHASE 2: Kiểm Tra Data Source (OpenSky)
- [ ] **2.1** Đăng ký tài khoản miễn phí: https://opensky-network.org
- [ ] **2.2** Test API ngay:
  ```bash
  curl -s "https://opensky-network.org/api/states/all?lamin=8&lomin=100&lamax=24&lomax=109" | python3 -c "
  import json,sys; d=json.load(sys.stdin)
  print(f'Máy bay qua Việt Nam: {len(d[\"states\"])}')
  [print(f'  {s[1].strip():<10} | {s[2]:<20} | {s[6]:.1f}°N {s[5]:.1f}°E | {s[7]}m') for s in d['states'][:5]]"
  ```
- [ ] **2.3** Verify Lambda đang publish records vào Kinesis (AWS Console > Kinesis > Monitoring)
- [ ] **2.4** Verify data đang landing vào S3 `/bronze/raw_states/` (sau 60-90 giây)

### PHASE 3: Cài Đặt Databricks
- [ ] **3.1** Tạo Databricks Workspace trên AWS (free trial 14 ngày: https://www.databricks.com/try-databricks)
- [ ] **3.2** Trong Databricks: Settings > AWS > Instance Profile > nhập IAM Role ARN từ Terraform output `databricks_iam_role_arn`
- [ ] **3.3** Tạo Cluster: Runtime 14.x LTS, `i3.xlarge`, 1 driver + 1 worker, auto-terminate 2h
- [ ] **3.4** Upload notebooks từ thư mục `notebooks/` vào Databricks Workspace
- [ ] **3.5** Upload `reference/airports_loader.py` và chạy một lần để tạo `reference.airports` table

### PHASE 4: Chạy Bronze Layer
- [ ] **4.1** Mở notebook `01_bronze_autoloader.py` trong Databricks
- [ ] **4.2** Cập nhật `S3_BRONZE_PATH` với đúng bucket name
- [ ] **4.3** Run notebook — streaming query sẽ start và chạy liên tục
- [ ] **4.4** Verify: `SELECT COUNT(*) FROM bronze.raw_flight_states` tăng dần theo thời gian

### PHASE 5: Chạy Silver Layer
- [ ] **5.1** Mở notebook `02_silver_transform.py`
- [ ] **5.2** Run notebook — Silver streaming query sẽ start
- [ ] **5.3** Verify: `SELECT callsign, flight_phase, velocity_kmh, is_cargo FROM silver.flights LIMIT 10`

### PHASE 6: Chạy Gold Layer Analytics
- [ ] **6.1** Mở notebook `03_gold_analytics.py`
- [ ] **6.2** Run notebook — 11 Gold streaming queries sẽ start đồng thời
- [ ] **6.3** Verify từng table:
  - `SELECT * FROM gold.airspace_density ORDER BY window_start DESC LIMIT 5`
  - `SELECT * FROM gold.airline_market_share ORDER BY window_start DESC LIMIT 10`
  - `SELECT * FROM gold.flight_alerts ORDER BY snapshot_time_ts DESC LIMIT 5`
  - `SELECT * FROM gold.tourism_demand_signal ORDER BY window_start DESC LIMIT 10`

### PHASE 7: Chuyển sang Delta Live Tables (Production)
- [ ] **7.1** Trong Databricks: Workflows > Delta Live Tables > Create Pipeline
- [ ] **7.2** Source code: `notebooks/04_dlt_pipeline.py`
- [ ] **7.3** Pipeline mode: `CONTINUOUS`
- [ ] **7.4** Storage location: `s3://skystream-datalake-dev/dlt_storage/`
- [ ] **7.5** Start pipeline và monitor qua DLT UI

### PHASE 8: Databricks SQL Dashboard
- [ ] **8.1** Tạo Databricks SQL Warehouse (Serverless)
- [ ] **8.2** Tạo Dashboard mới, thêm các queries:
  - Counter: Tổng số máy bay đang bay (refresh 30s)
  - Bar chart: Top 10 hãng bay theo market share
  - Line chart: Lưu lượng bay theo giờ (hôm nay vs hôm qua)
  - Table: Real-time flight alerts
  - Bar chart: Top 10 sân bay theo congestion score
  - Line chart: Tourism demand signal cho 5 điểm đến chính
  - Area chart: Fuel burn estimate theo giờ
- [ ] **8.3** Set auto-refresh 30 giây cho toàn bộ dashboard
- [ ] **8.4** Tạo Databricks Alert: khi `flight_alerts` count > 5 trong 5 phút → gửi email

### PHASE 9: Kiểm Thử & Tối Ưu
- [ ] **9.1** Chạy unit tests: `cd tests && pip install -r requirements-test.txt && pytest -v`
- [ ] **9.2** Đo end-to-end latency (mục tiêu < 3 phút)
- [ ] **9.3** Kiểm tra data quality trong DLT UI (expectation pass/fail rates)
- [ ] **9.4** Tối ưu: partition Gold tables theo `date`, Z-order theo `origin_country`
- [ ] **9.5** Cấu hình AWS Budget alert ngưỡng $30/tháng

---

## 🔑 Nguồn Dữ Liệu Chi Tiết

### OpenSky Network API (MIỄN PHÍ)

| Endpoint | Mô tả | Rate limit |
|----------|--------|------------|
| `GET /api/states/all` | Tất cả máy bay toàn cầu | Anonymous: 400 req/day |
| `GET /api/states/all?lamin=&lomin=&lamax=&lomax=` | Máy bay trong bbox | Anonymous: 400 req/day |
| — | — | Free account: 4,000 req/day |

**17 fields của State Vector:**
```
[0]  icao24         → Mã ICAO24 duy nhất của máy bay (transponder ID)
[1]  callsign       → Số hiệu chuyến bay (VD: VN123, UAL456)
[2]  origin_country → Quốc gia đăng ký transponder
[3]  time_position  → Unix timestamp lần cuối cập nhật vị trí
[4]  last_contact   → Unix timestamp lần cuối nhận tín hiệu
[5]  longitude      → Kinh độ (WGS-84)
[6]  latitude       → Vĩ độ (WGS-84)
[7]  baro_altitude  → Độ cao barometric (mét)
[8]  on_ground      → Boolean: đang ở mặt đất
[9]  velocity       → Tốc độ mặt đất (m/s)
[10] true_track     → Hướng bay (độ, 0=Bắc, 90=Đông)
[11] vertical_rate  → Tốc độ lên/xuống (m/s, âm = đang hạ)
[12] sensors        → Danh sách sensor IDs (thường null)
[13] geo_altitude   → Độ cao geometric (mét)
[14] squawk         → Mã transponder 4 số
[15] spi            → Special purpose indicator
[16] position_source→ 0=ADS-B, 1=ASTERIX, 2=MLAT, 3=FLARM
```

---

## 💰 Ước Tính Chi Phí AWS

| Dịch vụ | Free Tier | Chi phí dev/tháng |
|---------|-----------|-------------------|
| Kinesis Data Streams (2 shards) | 1 shard free 12 tháng | ~$22 |
| Kinesis Firehose | 5GB free | ~$0.50 |
| S3 (~10GB/tháng) | 5GB free | ~$0.50 |
| Lambda | 1M invocations free | ~$0 |
| EventBridge | 14M events free | ~$0 |
| Databricks (free trial) | 14 ngày | $0 → ~$40 sau trial |
| **Tổng/tháng** | **Trong free tier: ~$0** | **~$20-60** |

---

## 🛠️ Tech Stack

```
DATA SOURCE    : OpenSky Network REST API (ADS-B, FREE, 10s refresh)
INGESTION      : AWS Lambda + Kinesis Data Streams + Kinesis Firehose
STORAGE        : Amazon S3 + Delta Lake
PROCESSING     : Databricks Structured Streaming + Delta Live Tables
ORCHESTRATION  : Databricks Workflows
SERVING        : Databricks SQL Dashboard + Databricks Alerts
MONITORING     : AWS CloudWatch + Databricks DLT UI
ARCHITECTURE   : Medallion (Bronze → Silver → Gold)
INFRA AS CODE  : Terraform
TESTING        : pytest + pyspark
```

---

## 📁 Cấu Trúc Thư Mục

```
databrick/
├── infra/
│   ├── main.tf              # Terraform: S3, Kinesis, Lambda, IAM, Firehose
│   ├── variables.tf
│   └── outputs.tf
├── ingestion/
│   └── lambda_producer/
│       ├── handler.py       # Poll OpenSky → Kinesis (urllib3, boto3)
│       └── requirements.txt
├── notebooks/
│   ├── 01_bronze_autoloader.py    # Auto Loader → bronze.raw_flight_states
│   ├── 02_silver_transform.py     # Cleaning & enrichment → silver.flights
│   ├── 03_gold_analytics.py       # 11 Gold streaming queries
│   └── 04_dlt_pipeline.py         # Delta Live Tables (all-in-one)
├── reference/
│   └── airports_loader.py   # Load OurAirports CSV → reference.airports
├── dashboards/
│   └── skystream_dashboard.json
├── tests/
│   ├── conftest.py
│   ├── test_silver_transform.py
│   ├── test_gold_aggregation.py
│   └── requirements-test.txt
└── plan.md
```
