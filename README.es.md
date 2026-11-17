# 🎵 Pipeline de Ingeniería de Datos End-to-End de Spotify

[🇺🇸 Read in English](README.md)

Proyecto integral de Ingeniería y Analítica de Datos que extrae, procesa, modela y visualiza información de la industria musical desde Spotify. El flujo abarca desde el consumo y creación de APIs REST y web scraping, hasta el procesamiento distribuido, orquestación en contenedores, almacenamiento en la nube y tableros de Inteligencia de Negocios (BI).

---

## 🏗️ Arquitectura del Sistema

```text
[Spotify API & Web Scraper (Playwright)] ──▶ [API Propia FastAPI] ──▶ [Pandas & PySpark]
                                                                              │
                                                                     (Apache Airflow / Docker)
                                                                              │
                    ┌─────────────────────────────────────────────────────────┴─────────────────────────────────────────────────────────┐
                    ▼                                                                                                                   ▼
         [PostgreSQL (OLTP)]                                                                                                 [Google BigQuery (OLAP)]
                    │                                                                                                                   │
                    └─────────────────────────────────────────────────────────┬─────────────────────────────────────────────────────────┘
                                                                              ▼
                                                                    [Transformaciones dbt]
                                                                              │
                                                              ┌───────────────┴───────────────┐
                                                              ▼                               ▼
                                                       [Looker Studio]                   [Power BI]
```

---

## 🛠️ Stack Tecnológico

* **Lenguajes:** Python, SQL
* **Extracción de Datos:** Spotipy (Spotify Web API), FastAPI (APIs REST propias), Playwright (Web Scraping)
* **Procesamiento de Datos:** Pandas, PySpark
* **Orquestación:** Apache Airflow (TaskFlow API, TaskGroups, Sensores, ShortCircuitOperators)
* **Bases de Datos y Data Warehouse:** PostgreSQL (SQLAlchemy), Google BigQuery
* **Modelado Analítico:** dbt (Data Build Tool)
* **Infraestructura y Contenedores:** Docker, Docker Compose
* **Visualización (BI):** Looker Studio, Power BI

---

## 💡 Decisiones Técnicas y Retos Resueltos

* **Ingesta Idempotente:** Implementación de sentencias nativas de PostgreSQL `ON CONFLICT DO NOTHING` indexadas por el identificador único de Spotify (`id_spotify`) mediante SQLAlchemy. Esto evita fallos por duplicidad de nombres de álbumes o canciones y mantiene limpia la integridad transaccional.
* **Orquestación Modular en Airflow:** Estructuración del flujo en módulos desacoplados con `@task_group` (`Proceso_Artista`, `Proceso_Album`, `Proceso_Canciones`), utilizando parámetros dinámicos (`Param`) y operadores `ShortCircuitOperator` para ejecuciones condicionales.
* **Arquitectura de Almacenamiento Híbrida:** Diseño de doble destino que almacena entidades relacionales normalizadas en PostgreSQL y conjuntos de datos analíticos en Google BigQuery.
* **Analytics Engineering con dbt:** Creación de modelos SQL modulares para transformar tablas crudas en modelos dimensionales listos para consumo en herramientas de BI.

---

## 📂 Estructura del Proyecto

```text
Portafolio/
├── docker/                        # Dockerfiles de Airflow y servicios base
├── docker_dbt/                    # Configuración del contenedor de dbt
├── docker_fastapi/                # Configuración del contenedor de FastAPI
├── Scripts_trabajos/              # DAGs de Airflow y flujos de orquestación
├── Spotify_api/                   # Código fuente principal del ETL y aplicación
│   ├── Conexiones/                # Conectores a Spotify API, PostgreSQL y BigQuery
│   ├── Esquemas/                  # Esquemas de validación de datos
│   ├── Evidencia Fisica (Imagenes)/ # Capturas de logs, esquemas y tableros BI
│   ├── FastAPI/                   # Endpoints de la API REST propia
│   ├── Models/                    # Modelos ORM de SQLAlchemy
│   ├── Playwright/                # Módulos de Web Scraping
│   ├── PySpark/                   # Scripts de procesamiento distribuido
│   ├── Spotify_Api/               # Lógica de extracción de Spotify
│   ├── Transforms/                # Pipelines de limpieza con Pandas
│   └── main.py                    # Punto de entrada principal
├── docker-compose.yaml            # Orquestación multi-contenedor
├── README.md                      # Documentación en inglés
└── README.es.md                   # Documentación en español
```

---

## 📊 Evidencia Física y Resultados del Pipeline

### 1. Orquestación en Apache Airflow
Flujo completo de extracción, transformación y carga (ETL), junto con la ejecución de modelos analíticos en dbt.

* **Pipeline Completo en Ejecución:**
  <img src="Spotify_api/Evidencia Fisica (Imagenes)/airflow-pipeline-completo.png" alt="Airflow Pipeline Completo" width="800"/>

* **DAGs y Flujo de dbt:**
  <img src="Spotify_api/Evidencia Fisica (Imagenes)/airflow-dags-etl.png" alt="Airflow DAGs" width="800"/>
  <img src="Spotify_api/Evidencia Fisica (Imagenes)/airflow-pipeline-dbt.png" alt="Airflow Pipeline dbt" width="800"/>

* **Detalle de TaskGroups (Artista, Álbum, Canciones y Más Info):**
  <img src="Spotify_api/Evidencia Fisica (Imagenes)/airflow-artista-album.png" alt="TaskGroup Artista y Album" width="800"/>
  <img src="Spotify_api/Evidencia Fisica (Imagenes)/airflow-canciones-mas-inf.png" alt="TaskGroup Canciones" width="800"/>

### 2. API Propia (FastAPI)
Documentación interactiva (Swagger UI) de los endpoints desarrollados para el consumo y exposición de datos.
<img src="Spotify_api/Evidencia Fisica (Imagenes)/fastapi-endpoints.png" alt="Endpoints FastAPI" width="800"/>

### 3. Modelado y Almacenamiento en PostgreSQL (OLTP)
Diagrama Entidad-Relación (ER) y validación de registros insertados de forma idempotente en las tablas relacionales.
<img src="Spotify_api/Evidencia Fisica (Imagenes)/postgres-diagrama-er.png" alt="Diagrama ER Postgres" width="800"/>
<img src="Spotify_api/Evidencia Fisica (Imagenes)/postgres-artista-mas-inf.png" alt="Tablas Artista Postgres" width="800"/>
<img src="Spotify_api/Evidencia Fisica (Imagenes)/postgres-albunes-canciones.png" alt="Tablas Albumes y Canciones Postgres" width="800"/>

### 4. Data Warehouse en Google BigQuery (OLAP)
Estructura del dataset en la nube y validación de las tablas analíticas listas para visualización.
<img src="Spotify_api/Evidencia Fisica (Imagenes)/bigquery-estructura.png" alt="Estructura BigQuery" width="800"/>
<img src="Spotify_api/Evidencia Fisica (Imagenes)/bigquery-artistas.png" alt="Tabla Artistas BigQuery" width="800"/>
<img src="Spotify_api/Evidencia Fisica (Imagenes)/bigquery-album.png" alt="Tabla Album BigQuery" width="800"/>
<img src="Spotify_api/Evidencia Fisica (Imagenes)/bigquery-canciones.png" alt="Tabla Canciones BigQuery" width="800"/>
<img src="Spotify_api/Evidencia Fisica (Imagenes)/bigquery-mas-inf.png" alt="Tabla Mas Inf BigQuery" width="800"/>

### 5. Inteligencia de Negocios (BI)
Tableros interactivos finales construidos sobre los modelos analíticos de dbt.

* **Reporte en Looker Studio:**
  <img src="Spotify_api/Evidencia Fisica (Imagenes)/looker-studio-reporte.png" alt="Reporte Looker Studio" width="800"/>

* **Dashboard en Power BI:**
  <img src="Spotify_api/Evidencia Fisica (Imagenes)/power-bi-reporte.png" alt="Reporte Power BI" width="800"/>
