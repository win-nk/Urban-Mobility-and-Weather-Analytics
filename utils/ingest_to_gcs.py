# utils/ingest_to_gcs.py
import os
import json
import uuid
import time
from datetime import datetime, timezone
from typing import List, Tuple
import requests
from google.cloud import storage, secretmanager

PROJECT_ID = os.getenv("GCP_PROJECT")
RAW_BUCKET = os.getenv("RAW_BUCKET")

def _secret(secret_id: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/latest"
    return client.access_secret_version(request={"name": name}).payload.data.decode("utf-8")

def _upload_json_to_gcs(bucket: str, path: str, payload: dict) -> str:
    client = storage.Client()
    blob = client.bucket(bucket).blob(path)
    blob.upload_from_string(json.dumps(payload, ensure_ascii=False), content_type="application/json")
    return f"gs://{bucket}/{path}"

def _ts_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def _today_date() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def fetch_weather(city_q: str) -> dict:
    """city_q เช่น 'Bangkok,TH'"""
    api_key = _secret("OPENWEATHER_API_KEY")
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {"q": city_q, "appid": api_key, "units": "metric"}
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    return r.json()

def fetch_directions(origin: str, destination: str) -> dict:
    api_key = _secret("GOOGLE_MAPS_API_KEY")
    url = "https://maps.googleapis.com/maps/api/directions/json"
    params = {
        "origin": origin,
        "destination": destination,
        "departure_time": "now",
        "key": api_key,
    }
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def ingest_weather_to_gcs(cities: List[str]) -> List[str]:
    """คืนรายการ gs://... ที่อัปโหลดแล้ว"""
    paths = []
    for city in cities:
        payload = fetch_weather(city)
        ts = _ts_utc()
        uid = uuid.uuid4().hex
        date_part = _today_date()
        safe_city = city.replace(" ", "_").replace(",", "_")
        path = f"raw/weather/city={safe_city}/date={date_part}/{ts}_{uid}.json"
        paths.append(_upload_json_to_gcs(RAW_BUCKET, path, payload))
        time.sleep(0.5)  # กัน rate burst
    return paths

def ingest_traffic_to_gcs(routes: List[Tuple[str, str]]) -> List[str]:
    """routes: [(origin, destination), ...]"""
    paths = []
    for origin, dest in routes:
        payload = fetch_directions(origin, dest)
        ts = _ts_utc()
        uid = uuid.uuid4().hex
        date_part = _today_date()
        safe_route = f"{origin}__to__{dest}".replace(" ", "_").replace(",", "_")
        path = f"raw/traffic/route={safe_route}/date={date_part}/{ts}_{uid}.json"
        paths.append(_upload_json_to_gcs(RAW_BUCKET, path, payload))
        time.sleep(0.5)
    return paths

# สำหรับทดสอบ local เท่านั้น
if __name__ == "__main__":
    print(ingest_weather_to_gcs(["Bangkok,TH"]))
    print(ingest_traffic_to_gcs([("Bangkok,TH", "Khon Kaen,TH")]))
