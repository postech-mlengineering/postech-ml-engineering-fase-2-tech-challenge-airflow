#usa a imagem base oficial do Airflow
FROM apache/airflow:3.1.3

#define usuário
USER airflow

#define o diretório de trabalho no container
WORKDIR /opt/airflow

#copia os arquivos de configuração do Poetry
COPY requirements.txt ./

#instala dependencias de requirements, certificando-se de que está no PATH
RUN pip install -r requirements.txt