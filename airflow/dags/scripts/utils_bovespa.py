import os
import time
import logging
from typing import List, Tuple
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import boto3


logger = logging.getLogger("airflow.task")


def upload_to_s3(
        dest_folder_path: str,
        process_date: str
    ) -> None:
    """
    
    """
    try:
        #removendo arquivo
        for file in os.listdir(dest_folder_path):
            file_path = os.path.join(dest_folder_path, file)
            os.remove(file_path)

        #configurações do chrome
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")

        prefs = {
            "download.default_directory": dest_folder_path,
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
            logger.info(f"Arquivo processado salvo em: {dest_folder_path}")
        else:
            logger.error("Erro: O download não foi detectado a tempo.")
            raise

    except Exception as e:
        logger.info(f"Ocorreu um erro: {e}")
    finally:
        driver.quit()
        logger.info("Processo finalizado.")


def upload_to_s3(
        aws_access_key_id: str, 
        aws_secret_access_key: str, 
        region: str,
        src_folder_path: str,
        dest_bucket_name: str,
        dest_s3_folder_path: str
    ) -> None:
    """
    Realiza o upload dos arquivos produtos.csv, usuarios.csv e vendas.csv para a pasta bronze no S3.

    Args:
        aws_access_key_id (str): ID da chave de acesso AWS.
        aws_secret_access_key (str): Chave de acesso secreta AWS.
        region (str): Região da AWS onde o bucket está localizado.
        dest_bucket_name (str): Nome do bucket.
        dest_s3_folder_path (str): Caminho de destino do arquivo.
    """
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region
    )

    files: List[Tuple[str, str]] = [
        ("/home/platero/big_data_pipelines/data/bronze/vendas/produtos.csv", dest_s3_folder_path + "produtos.csv"),
        ("/home/platero/big_data_pipelines/data/bronze/vendas/usuarios.csv", dest_s3_folder_path + "usuarios.csv"),
        ("/home/platero/big_data_pipelines/data/bronze/vendas/vendas.csv", dest_s3_folder_path + "vendas.csv")
    ]

    for file_path, s3_file_name in files:
        try:
            s3.upload_file(file_path, dest_bucket_name, s3_file_name)
            logger.info(f"Arquivo {file_path} carregado com sucesso para {dest_bucket_name}/{s3_file_name}")
        except Exception as e:
            logger.error(f"Erro ao carregar o arquivo {file_path}:", e)