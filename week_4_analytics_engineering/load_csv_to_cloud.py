import pandas as pd
import os
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# 1. Carregar variáveis de ambiente
load_dotenv()

def get_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOW_USER"),
        password=os.getenv("SNOW_PASSWORD"),
        account=os.getenv("SNOW_ACCOUNT"),
        database=os.getenv("SNOW_DATABASE"),
        schema=os.getenv("SNOW_SCHEMA"),
        warehouse="COMPUTE_WH",
        role="ACCOUNTADMIN"
    )

def upload_from_datatalks():
    taxis = ['yellow']
    years = [2020]
    
    conn = get_connection()

    for taxi in taxis:
        for year in years:
            for month in range(1, 13):
                file_name = f"{taxi}_tripdata_{year}-{month:02d}.csv.gz"
                dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi}/{file_name}"
                
                try:
                    print(f"\n--- Iniciando: {file_name} ---")
                    
                    # Usamos chunksize para não estourar a RAM do Codespaces
                    # 100.000 linhas por vez é um tamanho seguro
                    chunks = pd.read_csv(dataset_url, compression='gzip', low_memory=False, chunksize=100000)
                    
                    table_name = f"{taxi}_tripdata".upper()
                    chunk_no = 1
                    
                    for df in chunks:
                        # Padronização das colunas em cada pedaço
                        df.columns = [c.upper() for c in df.columns]
                        
                        # Se for o primeiro chunk do primeiro mês, ele pode criar a tabela
                        # Nos demais, ele apenas insere (append)
                        print(f"Enviando bloco {chunk_no} ({len(df)} linhas)...")
                        
                        write_pandas(conn, df, table_name, auto_create_table=True)
                        chunk_no += 1
                    
                    print(f"✅ Sucesso total: {file_name}")
                    
                except Exception as e:
                    print(f"Falha ao processar {file_name}: {e}")
                    continue
    
    conn.close()

if __name__ == "__main__":
    upload_from_datatalks()
    print("\n--- CARGA COMPLETA ---")