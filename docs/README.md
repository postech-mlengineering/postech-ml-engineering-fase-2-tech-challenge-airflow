# Repositório do Airflow para o Tech Challenge da Fase 2 da Pós-Graduação em Machine Learning Engineering da FIAP

Este repositório consiste em um pipeline de dados orquestrado com Apache Airflow via Docker, desenvolvido para automatizar a ingestão e processamento de dados do Índice Bovespa. A solução integra técnicas de web scraping com serviços da AWS para transformar dados disponibilizados pela B3 em ativos analíticos prontos para consumo em um Data Lakehouse

**Fluxo**

O pipeline disponibiliza os dados em uma arquitetura medalhão em um Data Lake na AWS S3:

1.  **Extração (Selenium):** utiliza Selenium para realizar a navegação automatizada no portal da B3. O processo realiza o bypass de iframes e extrai a Carteira do Dia do Índice Bovespa
2.  **Ingestão (Bronze):** os arquivos extraídos são persistidos localmente e enviados para a camada bronze do bucket S3 via Boto3 em uma rotina diária (segunda a sexta). Os dados são armazenados em seu formato original, preservando a granularidade com particionamento por data de extração
3.  **Transformação (AWS Glue - Silver):** o Apache Airflow dispara um job no AWS Glue para limpeza, normalização de tipos, tratamento de valores nulos, conversão para Parquet e particionamento por data e categoria, otimizando drasticamente a performance de leitura e custo de armazenamento
4.  **Transformação (AWS Glue - Gold Layer):** um outro job de processamento transforma os dados da camada silver em tabelas analíticas disponibilizadas na camada gold
5.  **Data Lakehouse (Athena):** o fluxo encerra com a atualização automatizada, via AWS Glue Crawler, das tabelas transformadas no AWS Athena, disponibilizando-as para consultas SQL de alta performance

**Dados**

O pipeline gerencia e disponibiliza as seguintes entidades no Glue Catalog:

*   **tb_index_composition**: detalhamento da carteira do dia
*   **tb_asset_moving_average**: cálculo de médias móveis dos ativos que compõem o índice
*   **tb_sector_market_share**: visão consolidada da relevância e peso de cada setor econômico dentro do Índice Bovespa

### Arquitetura

O diagrama abaixo ilustra a arquitetura do projeto:

<br><p align='center'><img src='' alt='Arquitetura'></p>

### Pré-requisitos

**Ambiente do Apache Airflow**

Certifique-se de ter o Python 3.11 e o Docker 29.1.1 instalados em seu sistema.

**Infraestrutura AWS**

É necessário que a conta AWS possua os seguintes recursos configurados:

1. Amazon S3 (Data Lake e Armazenamento)

Toda a estrutura de arquivos e diretórios será centralizada no bucket: `postech-ml-engineering-fase-2-tech-challenge-bucket`.

*   **Estrutura de Pastas do Data Lake (Medalhão):**
    *   `bronze/`: dados brutos
    *   `silver/`: dados limpos
    *   `gold/`: dados agregados e prontos para consumo/análise
*   **Armazenamento de Scripts (Glue):**
    *   `scripts/`: diretório destinado aos scripts PySpark que serão executados pelos Glue Jobs
*   **Resultados de Consultas (Athena):**
    *   `athena-results/`: diretório configurado como *Query Result Location* para armazenar os logs e resultados de consultas do Amazon Athena

2. AWS IAM (Identidade e Acessos)

Configuração do usuário e das permissões necessárias para que o Apache Airflow interaja com a AWS e os serviços AWS interajam entre si.

*   **IAM User:** usuário específico (ex: `airflow-user`) cujas credenciais serão usadas no Apache Airflow com as seguintes políticas:
    *   Políticas Gerenciadas:
        *   `AmazonS3FullAccess`: Acesso total para leitura e escrita no bucket.
        *   `AWSGlueServiceRole`: Permissões necessárias para operações de serviço do Glue
    *   Políticas Customizadas:
        *   `glue-role-full`: Permissões totais para criação, exclusão e execução de componentes do AWS Glue
        *   `athena-role-full`: Permissões totais para execução de queries e gerenciamento do AWS Athena
    
*   **IAM Role:** 
    *  `glue-service-role`: política necessária para o usuário airflow informe a role de serviço para o AWS Glue durante a orquestração.

3. AWS Glue (ETL e Catálogo)

Componentes de processamento e organização de metadados.

*   **AWS Glue Data Catalog:**
    *   Banco de dados: `db_bovespa` (para mapeamento das tabelas das camadas silver e gold)
*   **AWS Glue Jobs:**
    *   Jobs PySpark previamente criados para realizar as transformações entre as camadas, devendo estar vinculados à IAM Role de serviço e apontando para os scripts na pasta `s3://.../scripts/`
*   **AWS Glue Crawlers:**
    *   Crawlers configurados para as camadas **Silver** e **Gold** para atualização automática do esquema das tabelas e metadados no AWS Glue Data Catalog

4. Amazon Athena

Configuração para consumo de dados via SQL.

*   **Athena Workgroup:** utilização de um workgroup ativo (ex: `primary`) para o gerenciamento das execuções de queries.
*   **Configuração de Saída:** definir o caminho `s3://postech-ml-engineering-fase-2-tech-challenge-bucket/athena-results/` como o local padrão de saída para evitar erros de permissão durante a execução de queries via Apache Airflow


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

### Integrações

### Deploy

O deploy foi realizado utilizando uma instância EC2 na AWS, via Docker para a padronização e o isolamento de ambiente.

### Link da Apresentação



### Colaboradores

[Jorge Platero](https://github.com/jorgeplatero)

[Leandro Delisposti](https://github.com/LeandroDelisposti)

[Hugo Rodrigues](https://github.com/Nokard)