# ============================================================
# Oracle OPC P6 Pipeline - Custom Airflow Image
# Base: apache/airflow:2.8.4-python3.11
# ============================================================
FROM apache/airflow:2.8.4-python3.11

# Switch to root for system-level installs
USER root

# Install OS dependencies (curl for healthchecks)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user for pip installs
USER airflow

# Copy and install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /requirements.txt

# Copy source package
COPY src/ /opt/airflow/src/
COPY config/ /opt/airflow/config/
COPY dags/ /opt/airflow/dags/

# Ensure src is importable
ENV PYTHONPATH="/opt/airflow:${PYTHONPATH}"

# Airflow home
ENV AIRFLOW_HOME=/opt/airflow

WORKDIR /opt/airflow
