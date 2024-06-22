import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Carregar variáveis de ambiente
load_dotenv()

# Variáveis de ambiente do destino
DB_HOST_DEST = os.getenv('DB_HOST_DEST')
DB_PORT_DEST = os.getenv('DB_PORT_DEST')
DB_NAME_DEST = os.getenv('DB_NAME_DEST')
DB_USER_DEST = os.getenv('DB_USER_DEST')
DB_PASS_DEST = os.getenv('DB_PASS_DEST')
DB_SCHEMA_DEST = os.getenv('DB_SCHEMA_DEST')

# Variáveis de ambiente da fonte
DB_HOST_SOURCE = os.getenv('DB_HOST_SOURCE')
DB_PORT_SOURCE = os.getenv('DB_PORT_SOURCE')
DB_NAME_SOURCE = os.getenv('DB_NAME_SOURCE')
DB_USER_SOURCE = os.getenv('DB_USER_SOURCE')
DB_PASS_SOURCE = os.getenv('DB_PASS_SOURCE')
DB_SCHEMA_SOURCE = os.getenv('DB_SCHEMA_SOURCE')

def extract_sales_data():
    conn = psycopg2.connect(
        dbname=DB_NAME_SOURCE,
        user=DB_USER_SOURCE,
        password=DB_PASS_SOURCE,
        host=DB_HOST_SOURCE,
        port=DB_PORT_SOURCE
    )
    query = "SELECT * FROM public.venda;"
    df_sales = pd.read_sql(query, conn)
    conn.close()
    return df_sales

def load_sales_data(df_sales):
    engine = create_engine(f'postgresql://{DB_USER_DEST}:{DB_PASS_DEST}@{DB_HOST_DEST}:{DB_PORT_DEST}/{DB_NAME_DEST}')
    df_sales.to_sql('venda', engine, if_exists='replace', index=False, schema=DB_SCHEMA_DEST)
    print("Dados carregados com sucesso no banco de destino.")

if __name__ == "__main__":
    # Extrair dados da fonte
    df_sales_data = extract_sales_data()
    
    # Carregar dados no banco de destino
    load_sales_data(df_sales_data)
