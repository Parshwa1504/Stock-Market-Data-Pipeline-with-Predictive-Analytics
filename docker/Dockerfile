FROM apache/airflow:2.9.3-python3.11

# Install extra Python deps needed by your DAGs / tasks
COPY /requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt
