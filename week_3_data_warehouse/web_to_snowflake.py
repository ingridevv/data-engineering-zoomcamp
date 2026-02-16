
import os
import urllib.request
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
import snowflake.connector
import time

# Carregar as variáveis de ambiente do arquivo .env
load_dotenv()

# --- CONFIGURAÇÕES DO SNOWFLAKE ---
SNOW_ACCOUNT = os.getenv("SNOW_ACCOUNT")
SNOW_USER = os.getenv("SNOW_USER")
SNOW_PASSWORD = os.getenv("SNOW_PASSWORD")
SNOW_DATABASE = os.getenv("SNOW_DATABASE")
SNOW_SCHEMA = os.getenv("SNOW_SCHEMA")
SNOW_STAGE = "DE_ZOOMCAMP_STAGE" # Nome do Stage que será criado

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-"
MONTHS = [f"{i:02d}" for i in range(1, 7)]
DOWNLOAD_DIR = "temp_data"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

def get_connection():
    return snowflake.connector.connect(
        user=SNOW_USER,
        password=SNOW_PASSWORD,
        account=SNOW_ACCOUNT,
        database=SNOW_DATABASE,
        schema=SNOW_SCHEMA
    )

def setup_snowflake():
    """Cria o stage se não existir"""
    conn = get_connection()
    try:
        conn.cursor().execute(f"CREATE STAGE IF NOT EXISTS {SNOW_STAGE}")
        print(f"Stage '{SNOW_STAGE}' pronto.")
    finally:
        conn.close()

def download_file(month):
    url = f"{BASE_URL}{month}.parquet"
    file_path = os.path.join(DOWNLOAD_DIR, f"yellow_tripdata_2024-{month}.parquet")
    try:
        if not os.path.exists(file_path):
            print(f"Downloading {url}...")
            urllib.request.urlretrieve(url, file_path)
            print(f"Downloaded: {file_path}")
        return file_path
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None

def upload_to_snowflake(file_path):
    """Envia o arquivo para o Stage do Snowflake via comando PUT"""
    if not file_path: return
    
    conn = get_connection()
    try:
        cursor = conn.cursor()
        file_name = os.path.basename(file_path)
        print(f"Uploading {file_name} to Snowflake Stage {SNOW_STAGE}...")
        
        # O comando PUT envia o arquivo local para o Stage
        # O parâmetro auto_compress=True é o padrão e comprime em gzip
        cursor.execute(f"PUT file://{os.path.abspath(file_path)} @{SNOW_STAGE} OVERWRITE = TRUE")
        
        print(f"Uploaded: @{SNOW_STAGE}/{file_name}")
    except Exception as e:
        print(f"Failed to upload {file_path} to Snowflake: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    setup_snowflake()

    print("Iniciando downloads...")
    with ThreadPoolExecutor(max_workers=4) as executor:
        file_paths = list(executor.map(download_file, MONTHS))

    print("Iniciando uploads para o Snowflake...")
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(upload_to_snowflake, filter(None, file_paths))

    print("Processo concluído. Arquivos estão no Stage do Snowflake.")