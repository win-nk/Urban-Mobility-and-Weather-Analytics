# Description
### Objective
Build a near-real-time ELT pipeline that combines current weather (OpenWeather) and route/traffic data (Google Maps Directions/Traffic).
Raw JSON is stored in GCS (partitioned paths), then loaded to BigQuery staging and transformed by SQL into query-ready marts for downstream analysis. The pipeline runs every 15 minutes, is orchestrated by Airflow (on VPS), and uses Secret Manager for API keys plus data-quality checks in BigQuery.
No dashboarding is included in this project.

### Architecture
<img width="1431" height="951" alt="gcp_DIAGRAM" src="https://github.com/user-attachments/assets/f8c9dda1-a52b-4b19-bd18-26134fba5bc5" />

### Sources
- OpenWeather (Current Weather)
- Google Maps Directions/Traffic

### Tools & Technologies
- Orchestration: Apache Airflow (on VPS)
- Storage & Warehouse: Google Cloud Storage, BigQuery
- Security: Secret Manager for API keys; IAM roles for the Airflow service account
- Languages/Libraries: Python (requests, google-cloud-storage, google-cloud-secret-manager)
- Format: JSON (raw), SQL (transforms)
- Config: Airflow Variables (PROJECT_ID, BQ_DATASET, RAW_BUCKET, CITIES, ROUTES)

### Pipelines
Airflow Directed Acyclic Graphs (DAG)
<img width="961" height="280" alt="Screenshot 2025-09-09 182248" src="https://github.com/user-attachments/assets/dc4bcb11-c773-4c4f-8cf5-4d09dba0e9d9" />

### Final Result
- Automated, parameterised ELT pipeline running every 15 minutes
- Raw API payloads → GCS (partitioned) → BigQuery staging → marts (weather_observations, traffic_observations)
- Data-quality checks (non-empty / recency) succeed per run
- Secrets handled via Secret Manager; IAM scoped to GCS & BigQuery
- No BI/dashboard layer included; outputs are query-ready tables for downstream use
- weather_observations <img width="1678" height="47" alt="image" src="https://github.com/user-attachments/assets/8d3934d0-597b-413f-8970-aa44156ab188" />
- traffic_observations <img width="1486" height="151" alt="image" src="https://github.com/user-attachments/assets/f9a435ac-7277-4c41-8a0e-34c027c63c7c" />

