FROM astro/base:4.6.0
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY dags /opt/airflow/dags
