#usa a imagem base oficial do Airflow
FROM apache/airflow:3.1.3

#define usuário
USER airflow

#define o diretório de trabalho no container
WORKDIR /opt/airflow