# PCAF Data Mesh POC Infrastructure

A repository containing the local infrastructure for running the PCAF data ingestion pipeline. 

## How to build image 
make sure existing images , conatianes and volumes are  removed before builidng new iamge  
    
    docker compose rm -fsv 

## Build new image.  

    docker compose build --no-cache 

## How to run the all service defined in you docker compose 

    docker compose up

## How to run the specific service in you docker compose 

    docker compose up <service Name>

## Adding new DAG to your project

all DAG  must be added to the filder airflow/dags/

## Adding dependencies
if any of your DAG need dependency python packaged , the dependencies should be added to " Dockerfile located in root of the project 

Example : RUN pip install apache-airflow-providers-trino

## OpenmetaData integration configuration 
Airflow required to have authendication token from MetaData server to get auhtendicaed to send leanage data back to OpenMetaData. Currently this token by default generated from Open MetaData application. In order to get tocken, after run the "docker compose up", You need to login to OpenMeta applcation with the following credential 
url      -   localhost:8585
user id  -   admin 
pasword  - admin
and then copy the token from openMetaData applcation by navigating to ( settings--> BOT --> ingestion-bot --> ) and update environment variable "AIRFLOW__LINEAGE__JWT_TOKEN" to the docker-compose.xml file located in the root of the project . 

## Restart only Airflow container 

    docker compose up airflow-webserver