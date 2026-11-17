# 🎵 Spotify End-to-End Data Engineering Pipeline

[🇪🇸 Leer en Español](README.es.md)

An end-to-end Data Engineering and Analytics project that extracts, processes, models, and visualizes music industry data from Spotify. The pipeline integrates API consumption, custom REST API development, web scraping, distributed data processing, containerized orchestration, cloud warehousing, and Business Intelligence dashboards.

---

## 🏗️ System Architecture

```text
[Spotify API & Web Scraper (Playwright)] ──▶ [Custom FastAPI] ──▶ [Pandas & PySpark]
                                                                          │
                                                                 (Apache Airflow / Docker)
                                                                          │
                    ┌─────────────────────────────────────────────────────┴─────────────────────────────────────────────────────┐
                    ▼                                                                                                           ▼
         [PostgreSQL (OLTP)]                                                                                         [Google BigQuery (OLAP)]
                    │                                                                                                           │
                    └─────────────────────────────────────────────────────┬─────────────────────────────────────────────────────┘
                                                                          ▼
                                                                [dbt Transformations]
                                                                          │
                                                          ┌───────────────┴───────────────┐
                                                          ▼                               ▼
                                                   [Looker Studio]                   [Power BI]
```

---

## 🛠️ Tech Stack

* **Languages:** Python, SQL
* **Data Extraction:** Spotipy (Spotify Web API), FastAPI (Custom REST API), Playwright (Web Scraping)
* **Data Processing:** Pandas, PySpark
* **Orchestration:** Apache Airflow (TaskFlow API, TaskGroups, Sensors, ShortCircuitOperators)
* **Databases & Warehousing:** PostgreSQL (SQLAlchemy), Google BigQuery
* **Data Modeling:** dbt (Data Build Tool)
* **Infrastructure & Containerization:** Docker, Docker Compose
* **Business Intelligence:** Looker Studio, Power BI

---

## 💡 Key Engineering Highlights & Challenges Solved

* **Idempotent Data Ingestion:** Implemented PostgreSQL native `ON CONFLICT DO NOTHING` clauses indexed by unique Spotify IDs (`id_spotify`) via SQLAlchemy, preventing pipeline crashes on duplicate album or track names while maintaining transactional integrity.
* **Modular Airflow Orchestration:** Refactored monolithic workflows into decoupled `@task_group` modules (`Proceso_Artista`, `Proceso_Album`, `Proceso_Canciones`) utilizing dynamic parameters (`Param`) and `ShortCircuitOperator` flags for conditional execution.
* **Hybrid Storage Strategy:** Designed a dual-sink architecture storing normalized relational entities in PostgreSQL and analytical datasets in Google BigQuery for downstream transformations.
* **Analytics Engineering with dbt:** Built modular SQL models to transform raw ingested tables into dimensional data marts ready for BI consumption.

---

## 📂 Project Structure

```text
Portafolio/
├── docker/                        # Airflow & core services Dockerfiles
├── docker_dbt/                    # dbt container configuration
├── docker_fastapi/                # FastAPI container configuration
├── Scripts_trabajos/              # Airflow DAGs and orchestration workflows
├── Spotify_api/                   # Core ETL & application codebase
│   ├── Conexiones/                # Spotify API, PostgreSQL & BigQuery connectors
│   ├── Esquemas/                  # Data validation schemas
│   ├── Evidencia Fisica (Imagenes)/ # Execution logs, schemas & BI screenshots
│   ├── FastAPI/                   # Custom REST API endpoints
│   ├── Models/                    # SQLAlchemy ORM models
│   ├── Playwright/                # Web scraping modules
│   ├── PySpark/                   # Distributed processing scripts
│   ├── Spotify_Api/               # Spotify data extraction logic
│   ├── Transforms/                # Pandas & data cleaning pipelines
│   └── main.py                    # Main application entry point
├── docker-compose.yaml            # Multi-container orchestration setup
├── README.md                      # English documentation
└── README.es.md                   # Spanish documentation
```

---

## 📊 Pipeline Execution & Results

### 1. Apache Airflow Orchestration
End-to-end ETL workflow execution and downstream dbt transformation triggers.

* **Full Pipeline Execution:**
  <img src="Spotify_api/Evidencia Fisica (Imagenes)/airflow-pipeline-completo.png" alt="Full Airflow Pipeline" width="800"/>

* **DAGs & dbt Integration:**
  <img src="Spotify_api/Evidencia Fisica (Imagenes)/airflow-dags-etl.png" alt="Airflow ETL DAGs" width="800"/>
  <img src="Spotify_api/Evidencia Fisica (Imagenes)/airflow-pipeline-dbt.png" alt="Airflow dbt Pipeline" width="800"/>

* **TaskGroups Detail (Artist, Album, Tracks & Additional Info):**
  <img src="Spotify_api/Evidencia Fisica (Imagenes)/airflow-artista-album.png" alt="Artist and Album TaskGroup" width="800"/>
  <img src="Spotify_api/Evidencia Fisica (Imagenes)/airflow-canciones-mas-inf.png" alt="Tracks TaskGroup" width="800"/>

### 2. Custom REST API (FastAPI)
Interactive Swagger UI documentation for custom data ingestion and serving endpoints.
<img src="Spotify_api/Evidencia Fisica (Imagenes)/fastapi-endpoints.png" alt="FastAPI Endpoints" width="800"/>

### 3. PostgreSQL Relational Storage (OLTP)
Entity-Relationship (ER) diagram and verification of idempotent batch inserts.
<img src="Spotify_api/Evidencia Fisica (Imagenes)/postgres-diagrama-er.png" alt="Postgres ER Diagram" width="800"/>
<img src="Spotify_api/Evidencia Fisica (Imagenes)/postgres-artista-mas-inf.png" alt="Postgres Artist Tables" width="800"/>
<img src="Spotify_api/Evidencia Fisica (Imagenes)/postgres-albunes-canciones.png" alt="Postgres Albums and Tracks" width="800"/>

### 4. Google BigQuery Cloud Warehouse (OLAP)
Cloud dataset schema and analytical tables ready for BI consumption.
<img src="Spotify_api/Evidencia Fisica (Imagenes)/bigquery-estructura.png" alt="BigQuery Schema" width="800"/>
<img src="Spotify_api/Evidencia Fisica (Imagenes)/bigquery-artistas.png" alt="BigQuery Artists Table" width="800"/>
<img src="Spotify_api/Evidencia Fisica (Imagenes)/bigquery-album.png" alt="BigQuery Album Table" width="800"/>
<img src="Spotify_api/Evidencia Fisica (Imagenes)/bigquery-canciones.png" alt="BigQuery Tracks Table" width="800"/>
<img src="Spotify_api/Evidencia Fisica (Imagenes)/bigquery-mas-inf.png" alt="BigQuery Extra Info Table" width="800"/>

### 5. Business Intelligence (BI) Dashboards
Executive reports powered by dbt dimensional models.

* **Looker Studio Report:**
  <img src="Spotify_api/Evidencia Fisica (Imagenes)/looker-studio-reporte.png" alt="Looker Studio Report" width="800"/>

* **Power BI Dashboard:**
  <img src="Spotify_api/Evidencia Fisica (Imagenes)/power-bi-reporte.png" alt="Power BI Dashboard" width="800"/>
