import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()
db_url = os.getenv("DATABASE_URL")

if not db_url:
    print("DATABASE_URL not found in .env")
    exit()

try:
    engine = create_engine(db_url)
    print("Engine created")
except Exception as e:
    print("Failed to create engine")
    print(e)
    exit()

csv_path = 'data/cleaned_pred_main.csv' 

if not os.path.exists(csv_path):
    print(f"File not found at {csv_path}")
    exit()

print(f"Reading data from {csv_path}...")
df = pd.read_csv(csv_path)
print(f"Data ready: {df.shape[0]} rows.")

table_name = 'machine_data_raw'
print(f"Uploading to table '{table_name}'...")

try:
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    
    print(f"Data loaded into table '{table_name}'!")
    
except Exception as e:
    print("Error uploading data:")
    print(e)