import os
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd


DEST_DIR = os.path.join(os.getcwd(), 'data', 'raw')
os.makedirs(DEST_DIR, exist_ok=True)

#configurações do Chrome
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--window-size=1920,1080")

prefs = {
    "download.default_directory": DEST_DIR,
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
}
chrome_options.add_experimental_option("prefs", prefs)

print("Iniciando Google Chrome...")
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)

try:
    print("Acessando B3...")
    url = "https://www.b3.com.br/pt_br/market-data-e-indices/indices/indices-amplos/indice-bovespa-b3-br-ibovespa-b3-br-composicao-carteira.htm"
    driver.get(url)
    
    wait = WebDriverWait(driver, 30)
    
    print("Aguardando iframe...")
    wait.until(EC.presence_of_element_located((By.ID, "bvmf_iframe")))
    driver.switch_to.frame("bvmf_iframe")
    print("Entrou no iframe.")

    print("Selecionando filtro por Setor de Atuação...")
    dropdown = wait.until(EC.presence_of_element_located((By.ID, "segment")))
    Select(dropdown).select_by_value("2")
    
    print("Clicando em Buscar...")
    btn_buscar = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'BUSCAR')]")))
    driver.execute_script("arguments[0].click();", btn_buscar)
    
    time.sleep(5) 

    print("Localizando botão de download...")
    download_btn = wait.until(EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, "Download")))
    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", download_btn)
    time.sleep(1)
    
    print("Clicando no botão de download...")
    driver.execute_script("arguments[0].click();", download_btn)
    
    print(f"Aguardando arquivo em: {DEST_DIR}")
    file_path = None
    for i in range(20):
        files = [f for f in os.listdir(DEST_DIR) if f.endswith(".csv")]
        if files:
            file_path = os.path.join(DEST_DIR, files[0])
            print(f"Sucesso! Arquivo baixado: {files[0]}")
            break
        time.sleep(1)
        
    if file_path:
        print("Iniciando processamento do DataFrame...")
        
        df = pd.read_csv(
            file_path, 
            sep=";", 
            encoding="latin1", 
            skiprows=1, 
            skipfooter=2, 
            engine="python",
            index_col=False
        )

        df = df.dropna(axis=1, how="all")
        
        df.columns = ["setor", "cod", "acao", "tipo", "qtd", "part", "acum"]

        df["data_pregao"] = datetime.now().strftime("%Y-%m-%d")

        df["qtd"] = df["qtd"].str.replace(".", "", regex=False).astype(int)
        df["part"] = df["part"].str.replace(",", ".", regex=False).astype(float)
        df["acum"] = df["acum"].str.replace(",", ".", regex=False).astype(float)

        print("DataFrame criado com sucesso!")
        print(df.head())

        final_path = os.path.join(DEST_DIR, "indice_bovespa.parquet")
        df.to_parquet(final_path)
        print(f"Arquivo processado salvo em: {final_path}")

    else:
        print("Erro: O download não foi detectado a tempo.")

except Exception as e:
    print(f"Ocorreu um erro: {e}")
    driver.save_screenshot("erro_setor_b3.png")
finally:
    driver.quit()
    print("Processo finalizado.")