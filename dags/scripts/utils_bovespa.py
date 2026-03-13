import os
import time
import logging
from typing import List
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import boto3


logger = logging.getLogger("airflow.task")


def web_scraping(
        dest_folder_path: str,
        process_date: str
    ) -> None:
    """
    Realiza web scraping no site da B3 para extrair a composição da 
    carteira do IBOVESPA.

    Args:
        dest_folder_path (str): Caminho local onde o arquivo CSV será 
        baixado e armazenado.
        process_date (str): Data de processamento.

    Raises:
        Exception: Caso o download não seja detectado no tempo limite ou 
        ocorra falha no navegador.
    """
    logger.info(f"Extração para a data {process_date}")
    try:
        #removendo arquivo
        for file in os.listdir(os.path.abspath(dest_folder_path)):
            file_path = os.path.join(os.path.abspath(dest_folder_path), file)
            os.remove(file_path)

        #configurações do chrome
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")

        prefs = {
            "download.default_directory": os.path.abspath(dest_folder_path),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        chrome_options.add_experimental_option("prefs", prefs)

        logger.info("Iniciando Google Chrome...")
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)

        logger.info("Acessando B3...")
        url = "https://www.b3.com.br/pt_br/market-data-e-indices/indices/indices-amplos/indice-bovespa-b3-br-ibovespa-b3-br-composicao-carteira.htm"
        driver.get(url)
        
        wait = WebDriverWait(driver, 30)

        logger.info("Aguardando iframe...")
        wait.until(EC.presence_of_element_located((By.ID, "bvmf_iframe")))
        driver.switch_to.frame("bvmf_iframe")
        logger.info("Entrou no iframe.")

        logger.info("Selecionando filtro por Setor de Atuação...")
        dropdown = wait.until(EC.presence_of_element_located((By.ID, "segment")))
        Select(dropdown).select_by_value("2")
        
        logger.info("Clicando em Buscar...")
        btn_buscar = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'BUSCAR')]")))
        driver.execute_script("arguments[0].click();", btn_buscar)

        time.sleep(5) 

        logger.info("Localizando botão de download...")
        download_btn = wait.until(EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, "Download")))
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", download_btn)
        time.sleep(1)
        
        logger.info("Clicando no botão de download...")
        driver.execute_script("arguments[0].click();", download_btn)
        
        logger.info(f"Aguardando arquivo em: {dest_folder_path}")
        file_path = None
        for i in range(20):
            files = [f for f in os.listdir(dest_folder_path) if f.endswith(".csv")]
            if files:
                file_path = os.path.join(dest_folder_path, files[0])
                logger.info(f"Sucesso! Arquivo baixado: {files[0]}")
                break
            time.sleep(1)

        if file_path:
            logger.info(f"Arquivo salvo em: {dest_folder_path}")
        else:
            logger.error("Erro: O download não foi detectado a tempo.")
            raise

    except Exception as e:
        logger.error(f"Ocorreu um erro: {e}")
    finally:
        driver.quit()
        logger.info("Processo finalizado.")


def upload_to_bronze(
        aws_access_key_id: str, 
        aws_secret_access_key: str, 
        region: str,
        src_folder_path: str,
        dest_bucket_name: str,
        dest_s3_folder_path: str,
        process_date: str
    ) -> None:
    """
    Realiza o upload do arquivo local para o bucket S3.

    Args:
        aws_access_key_id (str): ID da chave de acesso AWS.
        aws_secret_access_key (str): Chave de acesso secreta AWS.
        region (str): Região AWS onde o bucket está localizado.
        src_folder_path (str): Caminho da pasta local de origem dos arquivos.
        dest_bucket_name (str): Nome do bucket S3 de destino.
        dest_s3_folder_path (str): Caminho de destino dentro do bucket.
        process_date (str): Data de processamento.

    Raises:
        Exception: Se a pasta de origem estiver vazia ou ocorrer erro durante a transmissão para o S3.
    """
    logger.info(f"Iniciando upload para a data {process_date}")

    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region
    )

    files = os.listdir(src_folder_path)
    
    if not files:
        logger.warning(f"Nenhum arquivo encontrado em {src_folder_path}")
        raise

    file_name = files[0]
    file_path = os.path.join(src_folder_path, file_name)

    try:
        s3.upload_file(file_path, dest_bucket_name, os.path.join(dest_s3_folder_path, file_name))
        logger.info(f"Arquivo {file_path} carregado com sucesso para {dest_bucket_name}/{dest_s3_folder_path}")
    except Exception as e:
        logger.error(f"Erro ao carregar o arquivo {file_path}:", e)


def submit_glue_job(
    aws_access_key_id: str,
    aws_secret_access_key: str,
    region: str,
    job_name: str,
    script_args: dict,
    process_date: str
) -> None:
    """
    Dispara a execução de um AWS Glue Job e monitora seu status em tempo real até a finalização.

    Args:
        aws_access_key_id (str): ID da chave de acesso AWS.
        aws_secret_access_key (str): Chave de acesso secreta AWS.
        region (str): Região AWS onde o Glue Job está configurado.
        job_name (str): Nome do job a ser executado.
        script_args (dict): Dicionário de argumentos (parâmetros) enviados para o script Glue.
        process_date (str): Data de processamento.
    Raises:
        Exception: Se o job retornar status 'FAILED', 'STOPPED' ou 'TIMEOUT', ou falha na comunicação com a API.
    """
    logger.info(f"Iniciando Glue Job para a data {process_date}")

    client = boto3.client(
        'glue',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region
    )

    logger.info(f"Iniciando Glue Job: {job_name}")
    
    response = client.start_job_run(
        JobName=job_name,
        Arguments=script_args
    )
    
    job_run_id = response['JobRunId']
    logger.info(f"Job Run ID: {job_run_id}")

    while True:
        status_get = client.get_job_run(JobName=job_name, RunId=job_run_id)
        status = status_get['JobRun']['JobRunState']
        
        if status == 'SUCCEEDED':
            logger.info(f"Glue Job {job_name} finalizado com sucesso!")
            break
        elif status in ['FAILED', 'STOPPED', 'TIMEOUT']:
            error_message = status_get['JobRun'].get('ErrorMessage', 'Sem mensagem de erro detalhada.')
            logger.error(f"Erro no Glue Job {job_name}. Status: {status}. Erro: {error_message}")
            raise Exception(f"Glue Job falhou com status: {status}")
        else:
            logger.info(f"Aguardando Glue Job... Status atual: {status}")
            time.sleep(30)


def load_athena_tables(
    aws_access_key_id: str,
    aws_secret_access_key: str,
    region: str,
    database: str,
    athena_tables: List[str],
    s3_athena_path: str,
    process_date: str
) -> None:
    """
    Executa o comando MSCK REPAIR TABLE via Athena para uma lista de tabelas.

    Args:
        aws_access_key_id (str): ID da chave de acesso AWS.
        aws_secret_access_key (str): Chave de acesso secreta AWS.
        region (str): Região AWS onde o Athena/Glue estão localizados.
        database (str): Nome do banco de dados no Glue Data Catalog.
        athena_tables (List[str]): Lista com os nomes das tabelas a serem carregadas.
        s3_athena_path (str): Caminho no S3 para armazenar os logs de execução das queries.
        process_date (str): Data de processamento.

    Raises:
        Exception: Se a query do Athena falhar ou for cancelada para qualquer uma das tabelas.
    """
    logger.info(f"Iniciando carga da tabelas para a data {process_date}")

    client = boto3.client(
        'athena',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region
    )

    for table_name in athena_tables:
        query = f"MSCK REPAIR TABLE {table_name}"
        
        logger.info(f"Carregando tabela {database}.{table_name}")

        response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={'OutputLocation': s3_athena_path}
        )

        execution_id = response['QueryExecutionId']

        while True:
            status_get = client.get_query_execution(QueryExecutionId=execution_id)
            status = status_get['QueryExecution']['Status']['State']

            if status == 'SUCCEEDED':
                logger.info(f"Tabela {table_name} carregada com sucesso.")
                break
            elif status in ['FAILED', 'CANCELLED']:
                reason = status_get['QueryExecution']['Status'].get('StateChangeReason', 'Erro desconhecido')
                logger.error(f"Falha ao carregar tabela {table_name}. Status: {status}. Motivo: {reason}")
                raise Exception(f"Athena Query {status} para {table_name}: {reason}")
            else:
                logger.info(f"Aguardando carga da tabela {table_name}. Status: {status}")
                time.sleep(5)