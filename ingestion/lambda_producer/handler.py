"""
Lambda function: OpenSky Network Poller → Kinesis Data Streams

Triggered by EventBridge every 1 minute.
Each invocation polls the OpenSky Network API every 10 seconds (6 polls/minute).
Each aircraft state is converted to a flat dict and published as a Kinesis record.

Environment variables:
  KINESIS_STREAM_NAME   : Target Kinesis stream name
  POLL_INTERVAL_SECONDS : Seconds between polls (default: 10)
  POLL_COUNT            : Number of polls per invocation (default: 6)
  OPENSKY_USERNAME      : (optional) OpenSky username for higher rate limit
  OPENSKY_PASSWORD      : (optional) OpenSky password
  AWS_REGION_NAME       : AWS region (default: us-east-1)
  BBOX_LAMIN/LOMIN/LAMAX/LOMAX : Bounding box filter (default: whole world)
"""

import json
import os
import time
import logging
from datetime import datetime, timezone

import boto3
import urllib3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# urllib3 PoolManager is created once at module level (reused across warm Lambda invocations)
http = urllib3.PoolManager(
    timeout=urllib3.Timeout(connect=5.0, read=15.0),
    retries=urllib3.Retry(total=2, backoff_factor=0.5),
)

STREAM_NAME = os.environ["KINESIS_STREAM_NAME"]
OPENSKY_USERNAME = os.environ.get("OPENSKY_USERNAME", "")
OPENSKY_PASSWORD = os.environ.get("OPENSKY_PASSWORD", "")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_SECONDS", "10"))
POLL_COUNT = int(os.environ.get("POLL_COUNT", "6"))
AWS_REGION = os.environ.get("AWS_REGION_NAME", "us-east-1")

BBOX = {
    "lamin": float(os.environ.get("BBOX_LAMIN", "-90")),
    "lomin": float(os.environ.get("BBOX_LOMIN", "-180")),
    "lamax": float(os.environ.get("BBOX_LAMAX", "90")),
    "lomax": float(os.environ.get("BBOX_LOMAX", "180")),
}

# Kinesis client is created once at module level
kinesis = boto3.client("kinesis", region_name=AWS_REGION)

# OpenSky API returns states as a list — field positions are fixed
STATE_FIELD_NAMES = [
    "icao24",         # [0]  Transponder address
    "callsign",       # [1]  Flight number / callsign
    "origin_country", # [2]  Country of origin
    "time_position",  # [3]  Unix timestamp of last position update
    "last_contact",   # [4]  Unix timestamp of last signal received
    "longitude",      # [5]  WGS-84 longitude (degrees)
    "latitude",       # [6]  WGS-84 latitude (degrees)
    "baro_altitude",  # [7]  Barometric altitude (meters)
    "on_ground",      # [8]  Boolean: true if on ground
    "velocity",       # [9]  Ground speed (m/s)
    "true_track",     # [10] True track angle (degrees, 0=North)
    "vertical_rate",  # [11] Vertical rate (m/s, negative = descending)
    "sensors",        # [12] Sensor IDs (usually null, excluded from output)
    "geo_altitude",   # [13] Geometric altitude (meters)
    "squawk",         # [14] Transponder code
    "spi",            # [15] Special purpose indicator
    "position_source",# [16] 0=ADS-B, 1=ASTERIX, 2=MLAT, 3=FLARM
]


def build_opensky_url() -> str:
    """Build the OpenSky API URL with optional bounding box."""
    base = "https://opensky-network.org/api/states/all"
    # Only add bbox params if not the whole world
    if BBOX["lamin"] > -90 or BBOX["lomin"] > -180 or BBOX["lamax"] < 90 or BBOX["lomax"] < 180:
        params = (
            f"?lamin={BBOX['lamin']}&lomin={BBOX['lomin']}"
            f"&lamax={BBOX['lamax']}&lomax={BBOX['lomax']}"
        )
        return base + params
    return base


def fetch_flight_states() -> dict | None:
    """Fetch current aircraft states from OpenSky Network API."""
    url = build_opensky_url()
    try:
        if OPENSKY_USERNAME and OPENSKY_PASSWORD:
            headers = urllib3.make_headers(
                basic_auth=f"{OPENSKY_USERNAME}:{OPENSKY_PASSWORD}"
            )
            response = http.request("GET", url, headers=headers)
        else:
            response = http.request("GET", url)

        if response.status == 200:
            return json.loads(response.data.decode("utf-8"))
        elif response.status == 429:
            logger.warning("OpenSky rate limit reached — skipping this poll.")
            return None
        else:
            logger.error(f"OpenSky API error: HTTP {response.status}")
            return None
    except urllib3.exceptions.TimeoutError:
        logger.error("OpenSky API request timed out.")
        return None
    except Exception as exc:
        logger.error(f"Unexpected error fetching OpenSky data: {exc}")
        return None


def parse_state_vector(state: list, snapshot_time: int) -> dict:
    """
    Convert a raw OpenSky state vector (list of 17 values) to a flat dict.
    Fields at unexpected positions are set to None.
    """
    record: dict = {}
    for idx, field_name in enumerate(STATE_FIELD_NAMES):
        if field_name == "sensors":
            continue  # exclude — usually null, not useful for analytics
        record[field_name] = state[idx] if idx < len(state) else None

    # Trim whitespace from callsign
    if record.get("callsign"):
        record["callsign"] = record["callsign"].strip() or None

    # Metadata fields added by Lambda
    record["snapshot_time"] = snapshot_time
    record["ingestion_time"] = datetime.now(timezone.utc).isoformat()

    return record


def publish_to_kinesis(records: list[dict]) -> int:
    """
    Publish a list of aircraft state dicts to Kinesis Data Streams.
    Sends in batches of 500 (Kinesis PutRecords limit).
    Returns number of successfully published records.
    """
    published = 0
    batch_size = 500

    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        kinesis_records = [
            {
                "Data": json.dumps(r, default=str).encode("utf-8"),
                "PartitionKey": r.get("icao24") or "unknown",
            }
            for r in batch
        ]
        try:
            response = kinesis.put_records(
                Records=kinesis_records,
                StreamName=STREAM_NAME,
            )
            failed = response.get("FailedRecordCount", 0)
            published += len(batch) - failed
            if failed > 0:
                logger.warning(f"Kinesis PutRecords: {failed} records failed in batch.")
        except Exception as exc:
            logger.error(f"Error publishing batch to Kinesis: {exc}")

    return published


def lambda_handler(event, context):
    """
    Main Lambda entry point.
    Polls OpenSky POLL_COUNT times with POLL_INTERVAL seconds between polls.
    """
    total_aircraft = 0
    total_published = 0

    for poll_num in range(1, POLL_COUNT + 1):
        logger.info(f"[Poll {poll_num}/{POLL_COUNT}] Fetching OpenSky states...")
        data = fetch_flight_states()

        if data and data.get("states"):
            snapshot_time = data.get("time", int(time.time()))
            states = data["states"]

            # Filter out records with missing lat/lon (cannot be placed on a map)
            valid_states = [
                s for s in states
                if len(s) > 6 and s[5] is not None and s[6] is not None
            ]

            records = [
                parse_state_vector(state, snapshot_time)
                for state in valid_states
            ]

            n_published = publish_to_kinesis(records)
            total_aircraft += len(states)
            total_published += n_published

            logger.info(
                f"[Poll {poll_num}/{POLL_COUNT}] "
                f"total={len(states)}, valid={len(valid_states)}, published={n_published}"
            )
        else:
            logger.warning(f"[Poll {poll_num}/{POLL_COUNT}] No data received.")

        # Wait between polls, except after the last one
        if poll_num < POLL_COUNT:
            time.sleep(POLL_INTERVAL)

    logger.info(f"Invocation complete: aircraft_seen={total_aircraft}, published={total_published}")
    return {
        "statusCode": 200,
        "aircraft_seen": total_aircraft,
        "published": total_published,
    }
