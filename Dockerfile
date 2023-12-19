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
                openmetadata-managed-apis~=1.2.2 \
                openmetadata-ingestion==1.2.2 \
                astronomer-cosmos==1.2.5 \
                typing-extensions==4.5.0 \
                dbt-artifacts-parser

RUN mkdir -p /opt/airflow/dag_generated_configs

#COPY Pipfile Pipfile.lock setup.py test_environment.py .env ./ 

# install dbt into a virtual environment
#WORKDIR /usr/local/airflow/
ENV PIP_USER=false
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir pipenv dbt-trino==1.5.1 dbt-fal==1.5.9 pycountry country_converter && \
    pipenv install && deactivate
ENV PIP_USER=true
ENV PATH="$PATH:/opt/airflow/dbt_venv/bin"


ENV AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT="120.0"

