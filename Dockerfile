#usa a imagem base oficial do Airflow
FROM apache/airflow:3.1.3

USER root

#atualize o sistema e instale as dependências necessárias para o Chrome e WebDriver
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    gnupg \
    unzip \
    libnss3 \
    libx11-xcb1 \
    libxcomposite1 \
    libxcursor1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxi6 \
    libxrandr2 \
    libxrender1 \
    libxss1 \
    libxtst6 \
    fonts-liberation \
    libasound2 \
    libgbm-dev \
    && wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get install -y ./google-chrome-stable_current_amd64.deb \
    && rm google-chrome-stable_current_amd64.deb \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

#define usuário
USER airflow

#define o diretório de trabalho no container
WORKDIR /opt/airflow

#copia os arquivos de configuração do Poetry
COPY requirements.txt ./

#instala dependencias de requirements, certificando-se de que está no PATH
RUN pip install -r requirements.txt