# Repositório do Airflow para o Tech Challenge da Fase 2 da Pós-Graduação em Machine Learning Engineering da FIAP

Este repositório consiste em um pipeline de dados orquestrado com Apache Airflow via Docker, desenvolvido para automatizar a ingestão e processamento de dados do Índice Bovespa. A solução integra técnicas de web scraping com serviços da AWS para transformar dados disponibilizados pela B3 em ativos analíticos prontos para consumo em um Data Lakehouse.

**Fluxo**

O pipeline orquestrado pelo Apache Airflow disponibiliza os dados em um Data Lake com arquitetura medalhão disponível em um bucket S3:

1.  **Web Scraping (Selenium):** um script web scraping é disparado para realizar a navegação automatizada no portal da B3 via Selenium e realizar o download dos dados da Carteira do Dia do Índice Bovespa
2.  **Ingestão no S3 (Bronze):** os arquivos extraídos são persistidos localmente e enviados para a camada bronze do bucket S3. Os dados são armazenados em seu formato original, preservando a granularidade com particionamento por data de extração
3.  **Transformação (AWS Glue - Silver):** um job no AWS Glue é disparado para limpeza, tipagem e conversão dos dados para Parquet, particionando por data e categoria, otimizando drasticamente a performance de leitura e custo de armazenamento
4.  **Transformação (AWS Glue - Gold):** um outro job de processamento transforma os dados da camada silver em tabelas analíticas disponibilizadas na camada gold do bucket S3
5.  **Data Lakehouse (Athena):** o fluxo encerra com a atualização automatizada via AWS Glue Crawler das tabelas transformadas no AWS Athena, disponibilizando-as para consultas SQL de alta performance

### Arquitetura

O diagrama abaixo ilustra a arquitetura do projeto:

<br><p align='center'><img src="https://github.com/postech-mlengineering/postech-ml-engineering-fase-2-tech-challenge-airflow/blob/729f6481e5ae0a5341d1006b45eaf0087feb72f2/docs/img/postech_ml_engineering_fase_2_tech_challenge_arquitetura.drawio.png" alt="Arquitetura"></p><br>

### Pré-requisitos

**Ambiente do Apache Airflow**

Certifique-se de ter o Python 3.11 e o Docker 29.1.1 instalados em seu sistema.

**Infraestrutura AWS**

É necessário que a conta AWS possua os recursos abaixo configurados.

1. Recursos e Serviços

*   **Amazon S3 (Data Lake):**
    *   **Bucket Central:** `postech-ml-engineering-fase-2-tech-challenge-bucket`
    *   **Camadas (Medalhão):**
        *   `bronze/`: armazenamento de arquivos brutos extraídos
        *   `silver/`: dados limpos e padronizados após o primeiro processamento
        *   `gold/`: dados agregados e prontos para consumo analítico
    *   **Pastas de Suporte:**
        *   `scripts/`: repositório dos arquivos `.py` (PySpark) utilizados pelos Glue Jobs
        *   `athena-results/`: local obrigatório para armazenamento de outputs do AWS Athena

*   **AWS Glue (Catálogo e ETL):**
    *   **Data Catalog:** banco de dados `db_bovespa` para gestão de metadados
    *   **Jobs PySpark:**
        *   `job_bronze_to_silver`: transformação e limpeza inicial
        *   `job_silver_to_gold`: agregações
    *   **Crawlers:**
        *   `index_composition`, `asset_moving_average`, `sector_market_share`: responsáveis por atualizar automaticamente o esquema das tabelas no AWS Glue Data Catalog

*   **Amazon Athena:**
    *   Configurado com o Workgroup `primary` e apontando o *Query Result Location* para a pasta de resultados no S3

2. Políticas e Permissões (IAM)

As permissões foram configuradas observando o princípio do privilégio mínimo.

*   **Usuário de Orquestração (`airflow-user`):**
    Utilizado pelo Apache Airflow para disparar o pipeline. Possui permissões restritas via **Política Inline**:
    *   **S3:** acesso de escrita (`PutObject`) limitado à pasta `bronze/`
    *   **Glue Jobs:** permissão apenas para iniciar (`StartJobRun`) e monitorar os jobs específicos do projeto
    *   **Glue Crawlers:** permissão apenas para iniciar (`StartCrawler`) os crawlers listados acima

<details>
<summary>Clique para visualizar o JSON</summary>

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "S3UploadPermissions",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:PutObjectAcl",
                "s3:GetObject"
            ],
            "Resource": "arn:aws:s3:::postech-ml-engineering-fase-2-tech-challenge-bucket/bronze/*"
        },
        {
            "Sid": "GlueJobControl",
            "Effect": "Allow",
            "Action": [
                "glue:StartJobRun",
                "glue:GetJobRun",
                "glue:GetJobRuns"
            ],
            "Resource": [
                "arn:aws:glue:us-east-1:<id_da_sua_conta_aws>:job/job_bronze_to_silver",
                "arn:aws:glue:us-east-1:<id_da_sua_conta_aws>:job/job_silver_to_gold"
            ]
        },
        {
            "Sid": "GlueCrawlerControl",
            "Effect": "Allow",
            "Action": [
                "glue:StartCrawler",
                "glue:GetCrawler"
            ],
            "Resource": [
                "arn:aws:glue:us-east-1:<id_da_sua_conta_aws>:crawler/index_composition",
                "arn:aws:glue:us-east-1:<id_da_sua_conta_aws>:crawler/asset_moving_average",
                "arn:aws:glue:us-east-1:<id_da_sua_conta_aws>:crawler/sector_market_share"
            ]
        }
    ]
}
```
</details>

*   **Role de Execução (`glue-role`):**
    Role de serviço que o AWS Glue assume para processar os dados.
    *   **S3 Access:** permissão de leitura e escrita em todas as camadas do bucket do projeto
    *   **CloudWatch Logs:** permissão para criar grupos de logs e gravar logs de execução (fundamental para monitoramento e debug)
    *   **Glue Catalog:** permissão para criar e alterar tabelas/partições dentro do banco de dados `db_bovespa`
    *   **Trust Relationship:** configurada para permitir que apenas o serviço `glue.amazonaws.com` assuma esta identidade

<details>
<summary>Clique para visualizar o JSON</summary>

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "S3Access",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::postech-ml-engineering-fase-2-tech-challenge-bucket",
                "arn:aws:s3:::postech-ml-engineering-fase-2-tech-challenge-bucket/*"
            ]
        },
        {
            "Sid": "Logging",
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": [
                "arn:aws:logs:*:*:log-group:/aws-glue/*"
            ]
        },
        {
            "Sid": "GlueCatalog",
            "Effect": "Allow",
            "Action": [
                "glue:GetDatabase",
                "glue:GetTable",
                "glue:GetTables",
                "glue:CreateTable",
                "glue:UpdateTable",
                "glue:GetPartition",
                "glue:GetPartitions",
                "glue:BatchGetPartition",
                "glue:BatchCreatePartition",
                "glue:UpdatePartition"
            ],
            "Resource": [
                "arn:aws:glue:us-east-1:<id_da_sua_conta_aws>:catalog",
                "arn:aws:glue:us-east-1:<id_da_sua_conta_aws>:database/db_bovespa",
                "arn:aws:glue:us-east-1:<id_da_sua_conta_aws>:table/db_bovespa/*"
            ]
        }
    ]
}
```
</details>

### Instalação

Clone o repositório e instale as dependências:

```bash
git clone [https://github.com/postech-mlengineering/postech-ml-engineering-fase-2-tech-challenge-airflow.git](https://github.com/postech-mlengineering/postech-ml-engineering-fase-2-tech-challenge-airflow.git)

cd postech-ml-techchallenge-fase-2-airflow
```

### Como Rodar a Aplicação

Para subir o ambiente completo do Apache Airflow (Webserver, Scheduler, Postgres) via Docker:

1. Configure as variáveis de ambiente criando um arquivo .env na raiz do projeto e preencha conforme o conteúdo abaixo:

```bash
#variáveis de sistema para permissões
AIRFLOW_UID=1000
AIRFLOW_GID=0
#configurações de acesso à interface web
_AIRFLOW_WWW_USER_USERNAME=<usuario_de_sua_escolha>
_AIRFLOW_WWW_USER_PASSWORD=<senha_de_sua_escolha>
```

2. Inicie a aplicação:

```bashAWS_ACCESS_KEY_ID=<seu_access_key_id_aws>
AWS_SECRET_ACCESS_KEY=<seu_secret_key_aws>
AWS_REGION=<sua_regiao_aws>
docker-compose up --build -d
```

A UI do Apache Airflow estará rodando em http://localhost:8080.

Certifique-se de configurar as variáveis de ambiente necessárias para a execução da rotina na seção Admin -> Variables da UI. 

```bash
AWS_ACCESS_KEY_ID=<seu_access_key_id_aws>
AWS_SECRET_ACCESS_KEY=<seu_secret_key_aws>
```

### Tecnologias

| Componente | Tecnologia | Versão | Descrição |
| :--- | :--- | :--- | :--- |
| **Orquestrador** | **Apache Airflow** | `3.1.3` | Framework para orquestração de workflows |
| **Linguagem** | **Python** | `3.11` | Linguagem para desenvolvimento de scripts |
| **Análise de Dados** | **Pandas** | `3.0.1` | Biblioteca para manipulação de dados |
| **Armazenamento** | **Fastparquet** | `2025.12.0` | Engine para leitura/escrita de arquivos Parquet |
| **SDK** | **Boto3** | `1.42.71` | SDK da AWS para integração |
| **ETL** | **PySpark (AWS Glue)** | *(Nativo)* | Framework para processamento distribuído |
| **Web Scraping** | **Selenium** | `4.41.0` | Framework para web scraping |
| **Automação** | **Webdriver Manager**| `4.0.2` | Biblioteca para gerenciamento automatizado de drivers do Chrome |
| **Infraestrutura** | **Docker** | `29.1.1` | Ferramenta de containerização para paridade entre ambientes |
| **Gerenciamento** | **Poetry** | `2.2.1` | Gerenciador de ambientes virtuais para isolamento de dependências |

### Deploy

O deploy pode ser realizado via Docker para a padronização e o isolamento de ambiente.

### Link da Apresentação



### Colaboradores

[Jorge Platero](https://github.com/jorgeplatero)

[Leandro Delisposti](https://github.com/LeandroDelisposti)

[Hugo Rodrigues](https://github.com/Nokard)