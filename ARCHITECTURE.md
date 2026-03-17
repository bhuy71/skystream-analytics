# SkyStream Analytics — Kiến Trúc Hệ Thống

## Tổng Quan

SkyStream Analytics là hệ thống **streaming data pipeline** kết hợp **Lambda Architecture**, xây dựng trên **AWS + Databricks**. Hệ thống xử lý dữ liệu bay ADS-B thời gian thực từ OpenSky Network và cung cấp insights cho nhiều đối tượng người dùng khác nhau.

---

## Kiến Trúc Lambda Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCE                                          │
│                                                                              │
│   OpenSky Network ADS-B API  (cập nhật mỗi 10 giây, ~10,000 flights/poll)   │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                         AWS Lambda (poll mỗi 60s)
                                   │
                    ┌──────────────▼─────────────┐
                    │  Amazon Kinesis Data Streams │
                    │  (2 shards, ~1000 rec/s)     │
                    └──────────┬──────────────────┘
                               │
              ┌────────────────┴──────────────────┐
              │                                   │
              ▼                                   ▼
   ┌─────────────────────┐            ┌─────────────────────────┐
   │   SPEED LAYER        │            │   S3 Data Lake           │
   │   (Streaming)        │            │   /bronze/raw_states/    │
   │                      │            │   (GZIP, 60s buffer)     │
   │  DLT CONTINUOUS      │            └──────────┬──────────────┘
   │  Bronze → Silver     │                       │
   │  → 11 Gold tables    │            ┌──────────▼──────────────┐
   │  Latency: <30s       │            │   BATCH LAYER            │
   │                      │            │   (Scheduled)            │
   └──────────┬───────────┘            │                          │
              │                        │  Daily 2AM: 05_batch.py  │
              │                        │  → 7 batch views         │
              │                        │  History: 30d / 52w      │
              │                        └──────────┬───────────────┘
              │                                   │
              └──────────────┬────────────────────┘
                             │
                    ┌────────▼────────────────────┐
                    │   SERVING LAYER              │
                    │   Daily 4AM: 06_serving.py   │
                    │                              │
                    │  MERGE batch + speed         │
                    │  → 7 serving views           │
                    └────────┬────────────────────┘
                             │
              ┌──────────────┼──────────────────┐
              ▼              ▼                  ▼
      Dashboard         BI Tools           Alerts
      (SQL 30s)       (Power BI,         (Email/Slack
                       Tableau)           khi anomaly)
```

---

## Medallion Architecture (trong Speed Layer)

```
RAW JSON (S3)
    │
    ▼  Auto Loader (cloudFiles)
┌─────────────────────────────────────────┐
│ 🥉 BRONZE: raw_flight_states            │
│ - Append-only, no transformation        │
│ - 17 fields từ OpenSky API              │
│ - Metadata: _source_file, _ingested_at  │
└────────────────────┬────────────────────┘
                     │  Streaming read
                     ▼
┌─────────────────────────────────────────┐
│ 🥈 SILVER: flights                      │
│ - Cast types, trim strings              │
│ - Convert units (m/s→km/h, m→ft)        │
│ - Classify flight_phase                 │
│ - Extract airline_icao từ callsign      │
│ - Flag cargo flights                    │
│ - Add lat_bin/lon_bin spatial grid      │
│ - Drop null lat/lon records             │
└────────────────────┬────────────────────┘
                     │  Streaming read × 11
                     ▼
┌─────────────────────────────────────────┐
│ 🥇 GOLD: 11 analytics tables            │
│ (xem chi tiết bên dưới)                 │
└─────────────────────────────────────────┘
```

---

## Chi Tiết Luồng Data

### Bước 1: Ingestion (AWS Lambda → Kinesis)

**Input từ OpenSky API:**
```json
{
  "time": 1710652800,
  "states": [
    ["abc123", "VNA123 ", "Vietnam", 1710652799, 1710652800,
     106.7, 10.8, 9800.0, false, 245.0, 135.0, 3.2,
     null, 10200.0, "1234", false, 0],
    ["xyz456", "UAL891 ", "United States", 1710652799, 1710652800,
     -87.6, 41.9, 11000.0, false, 890.0, 270.0, 0.0,
     null, 11200.0, "5678", false, 0]
  ]
}
```

**Output → Kinesis (1 record/aircraft):**
```json
{
  "icao24": "abc123",
  "callsign": "VNA123",
  "origin_country": "Vietnam",
  "time_position": 1710652800,
  "last_contact": 1710652800,
  "longitude": 106.7,
  "latitude": 10.8,
  "baro_altitude": 9800.0,
  "on_ground": false,
  "velocity": 245.0,
  "true_track": 135.0,
  "vertical_rate": 3.2,
  "geo_altitude": 10200.0,
  "squawk": "1234",
  "ingestion_time": "2024-03-17T04:00:00Z"
}
```

---

### Bước 2: Bronze Layer (Auto Loader → Delta)

**Input:** GZIP JSON files từ S3 `/bronze/raw_states/`

**Output — `workspace_7474644985505263.bronze.raw_flight_states`:**
```
icao24 | callsign | origin_country | latitude | longitude | baro_altitude | on_ground | velocity | vertical_rate | _bronze_ingested_at       | _source_file
abc123 | VNA123   | Vietnam        | 10.8     | 106.7     | 9800.0        | false     | 245.0    | 3.2           | 2024-03-17 04:00:05.123   | s3://sky.../2024/03/17/04/file001.gz
xyz456 | UAL891   | United States  | 41.9     | -87.6     | 11000.0       | false     | 890.0    | 0.0           | 2024-03-17 04:00:05.456   | s3://sky.../2024/03/17/04/file001.gz
```

---

### Bước 3: Silver Layer (Cleaning & Enrichment)

**Transformations applied:**

| Field | Before | After | Rule |
|-------|--------|-------|------|
| `callsign` | `"VNA123 "` | `"VNA123"` | trim() |
| `airline_icao` | — | `"VNA"` | callsign[:3] |
| `velocity` | `245.0` m/s | `882.0` km/h | × 3.6 |
| `baro_altitude` | `9800.0` m | `32152` ft | × 3.28084 |
| `flight_phase` | — | `"CRUISING"` | if alt>10000ft & vrate<±3 |
| `is_cargo` | — | `false` | airline_icao in CARGO_LIST |
| `lat_bin` | — | `10` | floor(latitude) |
| `lon_bin` | — | `106` | floor(longitude) |

**Output — `workspace_7474644985505263.silver.flights`:**
```
icao24 | callsign | airline_icao | origin_country | latitude | longitude | altitude_ft | velocity_kmh | flight_phase | is_cargo | lat_bin | lon_bin
abc123 | VNA123   | VNA          | Vietnam        | 10.8     | 106.7     | 32152       | 882.0        | CRUISING     | false    | 10      | 106
xyz456 | UAL891   | UAL          | United States  | 41.9     | -87.6     | 36745       | 3204.0       | CRUISING     | false    | 41      | -88
```

---

## Speed Layer Usecases (11 Gold Tables)

### Gold 1: `airspace_density` — Mật độ không phận (1 phút)

**Usecase:** Air traffic control, airport capacity planning

**Input (Silver):** Tất cả flights trong 1 phút qua
**Processing:** GROUP BY lat_bin, lon_bin, country
**Output:**
```
lat_bin | lon_bin | origin_country | flight_count | window_start
10      | 106     | Vietnam        | 47           | 2024-03-17 04:00:00
41      | -88     | United States  | 312          | 2024-03-17 04:00:00
```
**Ý nghĩa:** Ô lưới 10°N/106°E (HCM City area) có 47 chuyến bay trong 1 phút → controller biết vùng nào đang tắc nghẽn.

---

### Gold 2: `flight_phase_stats` — Thống kê pha bay (5 phút)

**Usecase:** Fleet operations monitoring, maintenance scheduling

**Output:**
```
origin_country | flight_phase  | count | avg_altitude_ft | avg_speed_kmh
Vietnam        | CRUISING      | 89    | 35420           | 890
Vietnam        | CLIMBING      | 12    | 15300           | 650
Vietnam        | DESCENDING    | 8     | 8900            | 720
Vietnam        | ON_GROUND     | 156   | 0               | 0
```
**Ý nghĩa:** Vietnam Airlines có 89 chuyến đang cruising → fleet đang hoạt động bình thường.

---

### Gold 3: `flight_alerts` — Cảnh báo an toàn (realtime)

**Usecase:** Safety monitoring, incident prevention

**Trigger:** `vertical_rate < -10 m/s` (rapid descent)
**Output:**
```
icao24 | callsign | altitude_ft | vertical_rate | latitude | longitude | alert_time
def789 | QVN456   | 5200        | -15.3         | 15.2     | 108.9     | 2024-03-17 04:02:31
```
**Ý nghĩa:** Flight QVN456 đang descend nhanh ở 5,200ft → cần kiểm tra ngay.

---

### Gold 4: `hourly_traffic` — Traffic volume theo giờ

**Usecase:** Airport slot planning, revenue forecasting

**Output:**
```
origin_country | hour              | total_flights | avg_altitude | airborne_count
Vietnam        | 2024-03-17 04:00  | 234           | 28500        | 89
Thailand       | 2024-03-17 04:00  | 187           | 31200        | 76
```

---

### Gold 5: `route_demand_index` — Chỉ số nhu cầu tuyến bay (15 phút)

**Usecase:** Airline revenue management, route investment decisions

**Output:**
```
airline_icao | origin_country | flight_count | demand_score | window_start
VNA          | Vietnam        | 47           | 78.5         | 2024-03-17 04:00
THA          | Thailand       | 31           | 52.3         | 2024-03-17 04:00
```
**Ý nghĩa:** VNA có demand_score=78.5 trên tuyến Vietnam → tín hiệu tốt để mở rộng tần suất bay.

---

### Gold 6: `airline_market_share` — Thị phần hãng bay (5 phút)

**Usecase:** Competitive intelligence, stock analysis

**Output:**
```
airline_icao | active_flights | market_share_pct | window_start
UAL          | 892            | 8.92%            | 2024-03-17 04:00
DAL          | 756            | 7.56%            | 2024-03-17 04:00
VNA          | 134            | 1.34%            | 2024-03-17 04:00
```
**Ý nghĩa:** UAL đang dẫn đầu với 8.92% thị phần toàn cầu tại thời điểm này.

---

### Gold 7: `cargo_flow` — Dòng chảy hàng hóa (1 phút)

**Usecase:** Logistics operations, supply chain monitoring

**Output:**
```
origin_region  | cargo_flights | total_flights | cargo_pct | window_start
East Asia      | 234           | 1205          | 19.4%     | 2024-03-17 04:00
North America  | 412           | 2341          | 17.6%     | 2024-03-17 04:00
Southeast Asia | 89            | 678           | 13.1%     | 2024-03-17 04:00
```

---

### Gold 8: `airport_congestion` — Tắc nghẽn sân bay (5 phút)

**Usecase:** Airport operations, flight delay prediction

**Output:**
```
icao_code | airport_name    | aircraft_count | congestion_score | window_start
KLAX      | Los Angeles     | 89             | 0.89             | 2024-03-17 04:00
VVTS      | Ho Chi Minh     | 34             | 0.34             | 2024-03-17 04:00
```
**Ý nghĩa:** LAX có congestion_score=0.89 (gần tới ngưỡng capacity 100 aircraft) → dự báo delay.

---

### Gold 9: `tourism_demand_signal` — Tín hiệu du lịch (15 phút)

**Usecase:** Tourism investment, hotel/resort demand forecasting

**Output:**
```
region_name      | inbound_flights | source_countries | window_start
Phuket/Cambodia  | 47              | 23               | 2024-03-17 04:00
Ho Chi Minh      | 89              | 31               | 2024-03-17 04:00
Dubai Region     | 234             | 67               | 2024-03-17 04:00
```

---

### Gold 10: `fuel_burn_estimate` — Ước tính tiêu hao nhiên liệu (1 phút)

**Usecase:** Airline fuel cost management, carbon credit trading

**Output:**
```
airline_icao | active_flights | total_fuel_kg_hr | total_co2_kg_hr | window_start
UAL          | 892            | 2,230,000        | 7,025,600       | 2024-03-17 04:00
VNA          | 134            | 335,000          | 1,055,250       | 2024-03-17 04:00
```
**Ý nghĩa:** VNA đốt ~335 tấn nhiên liệu/giờ → chi phí ~$250,000/giờ (jet fuel $0.75/kg).

---

### Gold 11: `economic_activity_index` — Chỉ số kinh tế hàng không (1 giờ)

**Usecase:** Macro economic forecasting, GDP leading indicator

**Output:**
```
window_start      | total_flights | total_countries | aviation_index | cargo_ratio
2024-03-17 04:00  | 9,847         | 134             | 9,847          | 18.3%
2024-03-17 03:00  | 8,923         | 128             | 8,923          | 17.1%
```
**Ý nghĩa:** Index tăng 10.4% so với giờ trước → kinh tế toàn cầu đang hoạt động tích cực.

---

## Batch Layer Usecases (7 Batch Views)

### Batch 1: `airline_reliability_score` — Điểm độ tin cậy hãng bay (Daily)

**Schedule:** Mỗi ngày 2:00 AM UTC
**Input:** 30 ngày lịch sử từ Silver
**Processing time:** ~5-10 phút

**Input (Silver, 30 ngày):**
```
icao24 | airline_icao | on_ground | velocity_kmh | altitude_ft | vertical_rate | snapshot_time
abc123 | VNA          | false     | 882          | 35000       | 0.1           | 2024-02-16...
abc124 | VNA          | false     | 879          | 35100       | -0.2          | 2024-02-16...
... (triệu records)
```

**Output:**
```
airline_icao | reliability_score | airborne_pct | speed_stddev | rapid_descent | unique_aircraft
VNA          | 82.3              | 67.4%        | 45.2 km/h    | 3             | 89
ANA          | 91.5              | 71.2%        | 23.1 km/h    | 0             | 234
RYR          | 74.1              | 58.3%        | 67.8 km/h    | 7             | 456
```
**Ý nghĩa:** ANA (All Nippon) có reliability score cao nhất (91.5) → tốt cho nhà đầu tư quan tâm ESG.

---

### Batch 2: `route_growth_trend` — Xu hướng tăng trưởng tuyến bay (Weekly)

**Schedule:** Mỗi tuần thứ Hai 2:00 AM UTC
**Input:** 52 tuần lịch sử

**Output:**
```
airline_icao | origin_country | growth_rate_pct | avg_4w_current | avg_4w_prior
VNA          | Vietnam        | +23.4%          | 134.5          | 109.0
THA          | Thailand       | -8.2%           | 87.3           | 95.1
SIA          | Singapore      | +15.7%          | 312.0          | 269.7
```
**Ý nghĩa:** VNA tăng 23.4% so với 4 tuần trước → tín hiệu mở rộng, cân nhắc đầu tư.

---

### Batch 3: `airport_traffic_ranking` — Xếp hạng sân bay (Daily)

**Output:**
```
rank | icao_code | airport_name        | traffic_score | airline_diversity | country_diversity
1    | KLAX      | Los Angeles         | 8920          | 67                | 45
2    | EGLL      | London Heathrow     | 8456          | 89                | 67
15   | VVTS      | Ho Chi Minh City    | 2341          | 23                | 28
```

---

### Batch 4: `fuel_efficiency_ranking` — Xếp hạng hiệu quả nhiên liệu (Daily)

**Output:**
```
airline_icao | efficiency_grade | est_co2_kg_per_km | avg_cruise_speed | esg_risk
ANA          | A                | 2.1               | 895 km/h         | LOW
VNA          | B                | 2.7               | 878 km/h         | LOW
RYR          | C                | 3.3               | 832 km/h         | MEDIUM
```
**Ý nghĩa:** Grade A = phù hợp ESG funds; Grade D = rủi ro carbon tax.

---

### Batch 5: `cargo_trade_lane_analysis` — Phân tích làn thương mại hàng hóa (Weekly)

**Output:**
```
week       | origin_region | airline_icao | unique_cargo_aircraft | historical_avg_speed
2024-03-11 | East Asia     | CCA          | 234                   | 876 km/h
2024-03-11 | North America | FDX          | 412                   | 892 km/h
2024-03-11 | Southeast Asia| SQC          | 89                    | 883 km/h
```
**Ý nghĩa:** East Asia → North America lane tăng 15% → supply chain đang phục hồi.

---

### Batch 6: `tourism_seasonality_pattern` — Mô hình mùa vụ du lịch (Weekly)

**Output:**
```
region_name     | avg_weekly_flights_52w | peak_weekly_flights | trough_weekly_flights
Ho Chi Minh     | 234.5                  | 412 (Tết)           | 89 (Aug)
Phuket/Cambodia | 187.3                  | 356 (Dec-Jan)       | 67 (Sep)
```
**Ý nghĩa:** Phuket peak vào Tháng 12-1 → hotel/resort nên tăng giá, airlines nên tăng tần suất.

---

### Batch 7: `economic_weekly_index` — Chỉ số kinh tế hàng tuần (Weekly)

**Output:**
```
week       | aviation_index | index_4w_ma | total_unique_aircraft | countries_active
2024-03-11 | 287,450        | 265,320     | 9,847                 | 134
2024-03-04 | 271,230        | 258,100     | 9,234                 | 131
2024-01-08 | 198,450        | 195,230     | 7,123                 | 119
```
**Ý nghĩa:** Index 52-week trending up +44.8% → aviation sector phục hồi sau COVID.

---

## Serving Layer — Unified Views

### Serving View 1: `airline_360` — Airline 360° Dashboard

**Merge:** Batch reliability (30-day) + Live market share (30-min)

```
airline_icao | reliability_score | airborne_pct | live_active_flights | live_market_share_pct | esg_grade
VNA          | 82.3              | 67.4%        | 134                 | 1.34%                 | B
ANA          | 91.5              | 71.2%        | 312                 | 3.12%                 | A
UAL          | 78.9              | 61.3%        | 892                 | 8.92%                 | C
```

**Dùng cho:** Investor dashboard — nhìn một màn hình biết ngay: lịch sử tin cậy + trạng thái live.

---

### Serving View 2: `airport_ops` — Airport Operations Center

**Merge:** Batch traffic ranking (30-day) + Live congestion (5-min)

```
icao_code | airport_name  | traffic_score | live_congestion_score | live_aircraft_count | operational_status
KLAX      | Los Angeles   | 8920          | 0.89                  | 89                  | HIGH
VVTS      | Ho Chi Minh   | 2341          | 0.34                  | 34                  | NORMAL
EGLL      | Heathrow      | 8456          | 0.95                  | 95                  | CRITICAL
```

**Dùng cho:** Airport ops team — biết ngay sân bay nào đang critical, cần điều phối slot.

---

### Serving View 3: `route_intelligence` — Route Planning

**Merge:** Batch growth trend (52-week) + Live demand index (15-min)

```
airline_icao | origin_country | growth_rate_pct | live_demand_score | investment_signal
VNA          | Vietnam        | +23.4%          | 78.5              | STRONG_BUY
THA          | Thailand       | -8.2%           | 31.2              | HOLD
SIA          | Singapore      | +15.7%          | 65.4              | BUY
```

**Dùng cho:** Airline route planning team và aviation investors — tín hiệu buy/sell dựa trên cả trend lịch sử và demand hiện tại.

---

### Serving View 4: `cargo_intelligence`

```
origin_region  | historical_cargo_aircraft | live_cargo_flights | live_cargo_pct
East Asia      | 234                       | 89                 | 19.4%
North America  | 412                       | 134                | 17.6%
Southeast Asia | 89                        | 31                 | 13.1%
```

---

### Serving View 5: `sustainability_report` — ESG Dashboard

```
airline_icao | efficiency_grade | est_co2_kg_per_km | live_co2_kg_hr | esg_risk_flag
ANA          | A                | 2.1               | 1,234,500      | LOW_RISK
VNA          | B                | 2.7               | 1,055,250      | LOW_RISK
RYR          | C                | 3.3               | 3,456,000      | MEDIUM_RISK
```

---

### Serving View 6: `economic_dashboard`

```
week       | aviation_index | index_4w_ma | live_aviation_index | live_countries
2024-03-11 | 287,450        | 265,320     | 9,847               | 134
2024-03-04 | 271,230        | 258,100     | 9,847               | 134
```
**Ý nghĩa:** So sánh trend 52 tuần với trạng thái live ngay lập tức.

---

### Serving View 7: `tourism_dashboard`

```
region_name     | avg_weekly_flights_52w | live_inbound_flights | vs_seasonal_avg_pct | tourism_signal
Ho Chi Minh     | 234.5                  | 312                  | +33.1%              | ABOVE_TREND
Phuket/Cambodia | 187.3                  | 145                  | -22.5%              | BELOW_TREND
Dubai Region    | 456.2                  | 478                  | +4.8%               | ON_TREND
```
**Ý nghĩa:** HCMC đang ABOVE_TREND +33% → peak season, cơ hội kinh doanh du lịch tốt.

---

## Latency & Freshness Summary

| Layer | Usecase | Latency | Freshness |
|-------|---------|---------|-----------|
| Speed | Flight alerts | <10s | Realtime |
| Speed | Airport congestion | 30s | 5-min window |
| Speed | Market share | 30s | 5-min window |
| Speed | Cargo flow | 30s | 1-min window |
| Speed | Route demand | 30s | 15-min window |
| Speed | Economic index | 30s | 1-hour window |
| Batch | Reliability score | 24h | 30-day history |
| Batch | Route growth trend | 7 days | 52-week history |
| Batch | Fuel efficiency | 24h | 30-day history |
| Serving | Airline 360 | 24h + 30s | Batch daily + live overlay |
| Serving | Airport ops | 24h + 30s | Batch daily + live congestion |
| Serving | Route intelligence | 7d + 30s | Batch weekly + live demand |

---

## Mô Hình Dữ Liệu (Data Model)

```
bronze.raw_flight_states
    ↓ (streaming transform)
silver.flights
    ↓ (11 streaming aggregations)
gold.*  (speed layer — 11 tables)
    ↓ (daily batch join with silver history)
batch.* (batch layer — 7 tables)
    ↓ (daily merge job)
serving.* (serving layer — 7 unified views)
    ↓
Databricks SQL Dashboard (30s refresh)
```

---

## Giá Trị Kinh Doanh Theo Đối Tượng

| Đối tượng | Serving view | Insight | Hành động |
|-----------|-------------|---------|-----------|
| **Nhà đầu tư chứng khoán** | airline_360 | VNA reliability 82.3, market share đang tăng | Mua cổ phiếu VNA |
| **Nhà đầu tư bất động sản** | tourism_dashboard | HCMC +33% above trend | Đầu tư khách sạn Q4 |
| **Logistics manager** | cargo_intelligence | East Asia-US lane tăng | Đặt thêm container |
| **Airport operator** | airport_ops | Heathrow CRITICAL | Điều phối slot khẩn |
| **ESG fund manager** | sustainability_report | RYR grade C | Loại khỏi ESG portfolio |
| **Macro economist** | economic_dashboard | Index tăng 44.8% YoY | GDP Q2 dự báo tốt |
| **Airline route planner** | route_intelligence | VNA Vietnam STRONG_BUY | Mở thêm tần suất |
| **Air traffic controller** | gold.flight_alerts | QVN456 rapid descent | Liên lạc khẩn với pilot |
