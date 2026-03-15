import pandas as pd
import os
import shutil
import time
from sqlalchemy import create_engine, text
from datetime import datetime
from db_config import db_user, db_password, db_host, db_name

print("Waiting for MySQL to settle down...")
time.sleep(30)

# Config
BASE_DIR = os.path.dirname(os.path.abspath(__file__))  # folder script .py
incoming_folder = os.path.join(BASE_DIR, "../data/incoming_customer_address_daily")
processed_folder = os.path.join(BASE_DIR, "../data/processed_customer_address_daily")

engine = create_engine(
    f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}"
)

#Create or Checked ingested_files_table for tracking
with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS ingested_files (
            source_file VARCHAR(255) PRIMARY KEY,
            ingestion_timestamp DATETIME(3)
        )
    """))

def ingest_files():
    if not os.path.exists(processed_folder):
        os.makedirs(processed_folder)

    for file in os.listdir(incoming_folder):
        if file.startswith("customer_addresses") and file.endswith(".csv"):
            file_path = os.path.join(incoming_folder, file)
            print(f"Processing file: {file}")

            try:
                # Checked file in ingested_files table
                with engine.begin() as conn:
                    result = conn.execute(
                        text("SELECT COUNT(*) FROM ingested_files WHERE source_file=:file"),
                        {"file": file}
                    ).scalar()

                    if result > 0:
                        shutil.move(file_path, os.path.join(processed_folder, file))
                        print(f"File {file} sudah pernah di-ingest. Skip.")
                        continue
                    
                # Read CSV files
                df = pd.read_csv(file_path, sep=';')
                # strip kolom
                df.columns = df.columns.str.strip()
                # konversi tipe data
                df['id'] = df['id'].astype(int)
                df['customer_id'] = df['customer_id'].astype(int)
                df['created_at'] = pd.to_datetime(df['created_at'])

                # metadata tambahan
                df['source_file'] = file
                df['ingestion_timestamp'] = datetime.now()

                # insert ke table raw (mysql)
                with engine.begin() as conn:
                    df.to_sql(
                        "customer_addresses_raw",
                        con=conn,
                        if_exists="append",
                        index=False
                    )

                    # catat historical file yang telah di insert di ingested_files table
                    conn.execute(
                        text("INSERT INTO ingested_files (source_file, ingestion_timestamp) VALUES (:file, :ts)"),
                        {"file": file, "ts": datetime.now()}
                    )

                # file yang telah diinsert dipindahkan ke folder processed
                shutil.move(file_path, os.path.join(processed_folder, file))

                print(f"{file} successfully processed")

            except Exception as e:
                print(f"Error processing {file}: {e}")

#MAIN
if __name__ == "__main__":
    ingest_files()