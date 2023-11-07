FROM apache/airflow:2.7.1

#ADD requirements.txt . 
USER airflow


RUN pip install apache-airflow-providers-trino 
RUN pip install openmetadata-managed-apis~=1.1.7.2 
RUN pip install openmetadata-ingestion==1.1.7.2

RUN mkdir -p /opt/airflow/dag_generated_configs
