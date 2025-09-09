# weather_traffic_pipeline_bq_only.py
from datetime import datetime, timedelta
import json
from airflow import DAG 
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator, BigQueryCheckOperator

# === Config ===
PROJECT_ID = Variable.get("PROJECT_ID", default_var="famous-mix-471512-i3")
BQ_DATASET = Variable.get("BQ_DATASET", default_var="dataset_weather_traffic")
RAW_BUCKET = Variable.get("RAW_BUCKET", default_var="raw__js")

# กำหนดเมืองและเส้นทางที่อยากเก็บ (แก้ได้ใน Airflow Variables ถ้าต้องการ)
# ตัวอย่างค่า variable:
# CITIES='["Bangkok,TH","Khon Kaen,TH"]'
# ROUTES='[["Bangkok,TH","Khon Kaen,TH"],["Khon Kaen,TH","Bangkok,TH"]]'
CITIES = json.loads(Variable.get("CITIES", default_var='["Bangkok,TH"]'))
ROUTES = json.loads(Variable.get("ROUTES", default_var='[["Bangkok,TH","Khon Kaen,TH"]]'))

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="weather_traffic_pipeline_bq_only",
    start_date=datetime(2025, 9, 8),
    schedule_interval="0 * * * *",  
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["urban-mobility-and-weather", "bigquery-only"],
) as dag:

    # 1) Ingest: ดึง API → ลง GCS (raw__js)
    def _ingest():
        mod = __import__("utils.ingest_to_gcs", fromlist=["ingest_weather_to_gcs","ingest_traffic_to_gcs"])
        paths_w = mod.ingest_weather_to_gcs(CITIES)
        paths_t = mod.ingest_traffic_to_gcs([tuple(r) for r in ROUTES])
        return {"weather": paths_w, "traffic": paths_t}

    ingest = PythonOperator(
        task_id="ingest_to_gcs",
        python_callable=_ingest,
    )

    # 2) Load: GCS → BigQuery (staging) + ตั้ง ingestion-time partition
    load_weather = GCSToBigQueryOperator(
        task_id="load_stg_weather_raw",
        project_id=PROJECT_ID,           
        location="us-central1",          
        bucket=RAW_BUCKET,
        source_objects=["raw/weather/city=*/date=*/*.json"], 
        destination_project_dataset_table=f"{PROJECT_ID}.{BQ_DATASET}.stg_weather_raw",
        source_format="NEWLINE_DELIMITED_JSON",
        autodetect=True,
        write_disposition="WRITE_APPEND",
        ignore_unknown_values=True,
        time_partitioning={"type": "DAY"},
    )

    load_traffic = GCSToBigQueryOperator(
        task_id="load_stg_traffic_raw",
        project_id=PROJECT_ID,           
        location="us-central1",          
        bucket=RAW_BUCKET,
        source_objects=["raw/traffic/route=*/date=*/*.json"],
        destination_project_dataset_table=f"{PROJECT_ID}.{BQ_DATASET}.stg_traffic_raw",
        source_format="NEWLINE_DELIMITED_JSON",
        autodetect=True,
        write_disposition="WRITE_APPEND",
        ignore_unknown_values=True,
        time_partitioning={"type": "DAY"},
    )

    # 3) Transform: staging → curated (include ไฟล์ SQL ที่อยู่ใต้ dags/sql/)
    transform_weather = BigQueryInsertJobOperator(
        task_id="transform_weather",
        project_id=PROJECT_ID,           
        location="us-central1",          
        configuration={
            "query": {
                "query": "{% include 'sql/transform_weather.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    transform_traffic = BigQueryInsertJobOperator(
        task_id="transform_traffic",
        project_id=PROJECT_ID,           
        location="us-central1", 
        configuration={
            "query": {
                "query": "{% include 'sql/transform_traffic.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    # 4) Data Quality: ข้อมูลวันนี้ต้องมีอย่างน้อย 1 แถว
    dq_weather = BigQueryCheckOperator(
        task_id="dq_weather_not_empty",
        sql=f"SELECT COUNT(1) FROM `{PROJECT_ID}.{BQ_DATASET}.weather_observations` WHERE DATE(load_time)=CURRENT_DATE();",
        use_legacy_sql=False,
        location="us-central1",
    )
    dq_traffic = BigQueryCheckOperator(
        task_id="dq_traffic_not_empty",
        sql=f"SELECT COUNT(1) FROM `{PROJECT_ID}.{BQ_DATASET}.traffic_observations` WHERE DATE(load_time)=CURRENT_DATE();",
        use_legacy_sql=False,
        location="us-central1",
    )

    ingest >> [load_weather, load_traffic]

    load_weather  >> transform_weather  >> dq_weather
    load_traffic  >> transform_traffic  >> dq_traffic

