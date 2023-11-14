FROM apache/airflow:2.7.3
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --upgrade pip
RUN pip install --no-cache-dir "apache-airflow==2.7.3" \ 
                    "apache-airflow-providers-trino" \
                    "openmetadata-ingestion==1.1.5.0" \
                    astronomer-cosmos 

# install dbt into a virtual environment
#WORKDIR /usr/local/airflow/
ENV PIP_USER=false
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
     pip install --no-cache-dir dbt-trino && deactivate
ENV PIP_USER=true
ENV PATH="$PATH:/opt/airflow/dbt_venv/bin"
ENV AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT="120.0"