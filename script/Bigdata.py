import os
import zipfile
import pandas as pd
from pymongo import MongoClient
from kaggle.api.kaggle_api_extended import KaggleApi

# ------------------------------------
# 1. KONFIGURASI
# ------------------------------------
DATA_DIR = "./data/raw"
os.makedirs(DATA_DIR, exist_ok=True)

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "datalake"

DATASETS = [
    ("singhnavjot2062001/11000-medicine-details", "11000"),
    ("shudhanshusingh/250k-medicines-usage-side-effects-and-substitutes", "250k")
]

# ------------------------------------
# 2. DOWNLOAD & UNZIP DARI KAGGLE
# ------------------------------------
def download_and_extract():
    api = KaggleApi()
    api.authenticate()

    for dataset, folder in DATASETS:
        out_path = os.path.join(DATA_DIR, folder)
        os.makedirs(out_path, exist_ok=True)
        
        print(f"Downloading {dataset} ...")
        api.dataset_download_files(dataset, path=out_path, unzip=True)
        print(f"Extracted to {out_path}\n")

# ------------------------------------
# 3. LOAD KE MONGODB
# ------------------------------------
def load_to_mongo():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    # Loop folder hasil download
    for dataset, folder in DATASETS:
        folder_path = os.path.join(DATA_DIR, folder)

        for file in os.listdir(folder_path):
            if file.endswith(".csv"):
                coll_name = f"medicines_raw_{folder}"
                csv_path = os.path.join(folder_path, file)
                print(f"Importing {csv_path} -> MongoDB collection {coll_name}")

                df = pd.read_csv(csv_path, low_memory=False)
                df = df.rename(columns=lambda c: c.strip().lower().replace(" ", "_"))
                df = df.where(pd.notnull(df), None)  # NaN -> None

                records = df.to_dict(orient="records")
                if records:
                    db[coll_name].insert_many(records)
                print(f"✅ Inserted {len(records)} records into {coll_name}\n")

    print("🚀 Import selesai!")

if __name__ == "__main__":
    print("=== LANGKAH 1: DOWNLOAD DATASET DARI KAGGLE ===")
    download_and_extract()
    print("=== LANGKAH 2: LOAD CSV -> MONGODB ===")
    load_to_mongo()
    print("✅ DONE — Dataset siap di MongoDB.")
