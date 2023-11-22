FROM apache/airflow:2.7.1
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
#ADD requirements.txt . 
USER airflow

RUN pip install apache-airflow-providers-trino 
RUN pip install openmetadata-managed-apis~=1.1.7.2 
RUN pip install openmetadata-ingestion==1.1.7.2
RUN pip install astronomer-cosmos==1.2.4

RUN mkdir -p /opt/airflow/dag_generated_configs

# install dbt into a virtual environment
#WORKDIR /usr/local/airflow/
ENV PIP_USER=false
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
     pip install --no-cache-dir dbt-trino && deactivate
ENV PIP_USER=true
ENV PATH="$PATH:/opt/airflow/dbt_venv/bin"


ENV AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT="120.0"

