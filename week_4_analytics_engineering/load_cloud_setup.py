import os
import urllib.request
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
import snowflake.connector

# Carregar as variáveis de ambiente
load_dotenv()

# --- CONFIGURAÇÕES ---
SNOW_ACCOUNT = os.getenv("SNOW_ACCOUNT")
SNOW_USER = os.getenv("SNOW_USER")
SNOW_PASSWORD = os.getenv("SNOW_PASSWORD")
SNOW_DATABASE = os.getenv("SNOW_DATABASE")
SNOW_SCHEMA = os.getenv("SNOW_SCHEMA")
SNOW_STAGE = "DE_ZOOMCAMP_STAGE"

# Módulo 4: Yellow e Green para 2019 e 2020
TAXI_TYPES = ["yellow", "green"]
YEARS = ["2019", "2020"]
MONTHS = [f"{i:02d}" for i in range(1, 13)] # 12 meses

DOWNLOAD_DIR = "temp_data"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

def get_connection():
    return snowflake.connector.connect(
        user=SNOW_USER, password=SNOW_PASSWORD, account=SNOW_ACCOUNT,
        database=SNOW_DATABASE, schema=SNOW_SCHEMA
    )

def download_file(params):
    taxi, year, month = params
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi}_tripdata_{year}-{month}.parquet"
    file_name = f"{taxi}_tripdata_{year}-{month}.parquet"
    file_path = os.path.join(DOWNLOAD_DIR, file_name)
    
    try:
        if not os.path.exists(file_path):
            print(f"Downloading {file_name}...")
            urllib.request.urlretrieve(url, file_path)
        return file_path
    except Exception as e:
        print(f"Erro no download {url}: {e}")
        return None

def upload_to_snowflake(file_path):
    if not file_path: return
    conn = get_connection()
    try:
        cursor = conn.cursor()
        abs_path = os.path.abspath(file_path)
        print(f"Uploading {os.path.basename(file_path)}...")
        # PUT envia para o Stage
        cursor.execute(f"PUT file://{abs_path} @{SNOW_STAGE} AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
    finally:
        conn.close()

if __name__ == "__main__":
    # 1. Criar lista de tarefas (combinação de taxi, ano e mês)
    tasks = []
    for taxi in TAXI_TYPES:
        for year in YEARS:
            for month in MONTHS:
                tasks.append((taxi, year, month))

    print(f"Iniciando processamento de {len(tasks)} arquivos...")

    # 2. Downloads em massa
    with ThreadPoolExecutor(max_workers=8) as executor:
        file_paths = list(executor.map(download_file, tasks))

    # 3. Uploads para o Snowflake Stage
    print("Enviando para o Snowflake Stage...")
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_snowflake, filter(None, file_paths))

    print("\n--- TUDO PRONTO NO STAGE ---")