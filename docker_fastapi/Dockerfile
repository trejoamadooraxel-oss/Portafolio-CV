FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Tu código de negocio se monta como volumen (igual que en Airflow),
# así que no necesitamos COPY aquí para el código en sí.

EXPOSE 8000

CMD ["uvicorn", "Spotify_api.FastAPI.fast_main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]