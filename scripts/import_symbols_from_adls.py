
import os
import sys
import logging
import pandas as pd
import numpy as np

# Add project root to path to import core modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.blob_storage import BlobStorageClient
from core.postgres import connect
from dotenv import load_dotenv

# Load .env file
load_dotenv()


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def map_dtype_to_postgres(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "BIGINT"
    elif pd.api.types.is_float_dtype(dtype):
        return "DOUBLE PRECISION"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    else:
        return "TEXT"

def create_table_from_df(cursor, table_name, df):
    columns = []
    for col, dtype in df.dtypes.items():
        pg_type = map_dtype_to_postgres(dtype)
        # Sanitize column name slightly
        safe_col = col.lower().replace(" ", "_").replace("-", "_")
        columns.append(f'"{safe_col}" {pg_type}')
    
    create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)});"
    logger.info(f"Creating table with SQL: {create_sql}")
    cursor.execute(create_sql)
    
    # Return the safe column names in order
    return [f'"{col.lower().replace(" ", "_").replace("-", "_")}"' for col in df.columns]

def main():
    try:
        # 1. Connect to Azure Blob Storage
        logger.info("Connecting to Azure Blob Storage...")
        blob_client = BlobStorageClient(container_name='common')
        
        # 2. Read CSV from ADLS
        file_path = "df_symbols.csv"
        logger.info(f"Reading {file_path} from ADLS 'common' container...")
        
        blob_service = blob_client.container_client.get_blob_client(file_path)
        
        if not blob_service.exists():
            logger.error(f"File {file_path} does not exist in 'common' container.")
            return

        download_stream = blob_service.download_blob()
        df = pd.read_csv(download_stream)
        
        # Replace NaN with None for SQL NULL compatibility
        df = df.replace({np.nan: None})
        
        logger.info(f"Successfully read {len(df)} rows from {file_path}")
        logger.info(f"Columns: {df.columns.tolist()}")

        # 3. Connect to Postgres
        dsn = os.environ.get("POSTGRES_DSN")
        if not dsn:
            logger.error("POSTGRES_DSN environment variable is not set.")
            return
            
        logger.info("Connecting to PostgreSQL...")
        
        # Use psycopg 3 connection
        with connect(dsn) as conn:
            with conn.cursor() as cur:
                table_name = "symbols"
                
                # Drop table to ensure fresh start (as requested 'replace' behavior)
                logger.info(f"Dropping table '{table_name}' if exists...")
                cur.execute(f"DROP TABLE IF EXISTS {table_name}")
                
                # Create table
                safe_columns = create_table_from_df(cur, table_name, df)
                
                logger.info(f"Inserting {len(df)} rows...")
                
                # Use COPY for bulk insert
                with cur.copy(f"COPY {table_name} ({', '.join(safe_columns)}) FROM STDIN") as copy:
                    for _, row in df.iterrows():
                        copy.write_row(row)
            
            # Commit transaction
            conn.commit()
        
        logger.info("Import completed successfully.")
        
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
