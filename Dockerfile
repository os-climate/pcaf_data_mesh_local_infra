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


RUN pip install apache-airflow-providers-trino \
                openmetadata-managed-apis~=1.2.0 \
                openmetadata-ingestion==1.2.0 \
                astronomer-cosmos==1.2.5 \
                typing-extensions==4.5.0 \
                dbt-artifacts-parser

RUN mkdir -p /opt/airflow/dag_generated_configs

# install dbt into a virtual environment
#WORKDIR /usr/local/airflow/
ENV PIP_USER=false
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
     pip install --no-cache-dir dbt-trino==1.6.2 && deactivate
ENV PIP_USER=true
ENV PATH="$PATH:/opt/airflow/dbt_venv/bin"


ENV AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT="120.0"

