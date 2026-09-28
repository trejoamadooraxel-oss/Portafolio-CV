FROM apache/airflow:2.10.3-python3.10

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-3.10.txt"

USER root

ENV PLAYWRIGHT_BROWSERS_PATH=/usr/local/share/playwright

RUN playwright install --with-deps chromium && \
    chmod -R 775 /usr/local/share/playwright

USER airflow