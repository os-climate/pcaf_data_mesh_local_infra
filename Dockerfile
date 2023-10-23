#FROM ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.7.1}
FROM docker.getcollate.io/openmetadata/ingestion:1.1.7
#ADD requirements.txt .

# RUN umask 0002; \
#     mkdir -p /opt/airflow/dag_generated_configs

#RUN pip install --upgrade pip

#RUN pip install openmetadata-managed-apis --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.1/constraints-3.8.txt"
#RUN pip install openmetadata-ingestion[all] 
#COPY instantclient /instantclient
#ENV LD_LIBRARY_PATH=/instantclient

RUN pip install  apache-airflow-providers-trino
RUN pip install apache-airflow-providers-amazon
# RUN pip install openmetadata-managed-apis==1.1.1.0
# RUN pip install openmetadata-ingestion[airflow]==1.1.1.0

#RUN pip install "python-daemon>=3.0.0"
# remove all airflow providers except for docker
#RUN pip install --no-cache-dir apache-airflow==${AIRFLOW_VERSION} -r requirements.txt