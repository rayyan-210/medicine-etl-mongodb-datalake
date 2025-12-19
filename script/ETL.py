import pandas as pd
import os
from pymongo import MongoClient
import numpy as np
from pathlib import Path

# --- 1. Setup paths (relative to script location) ---
# Get the script's directory
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent  # pharmaceutical-data-pipeline folder

# Define paths relative to project root
DATALAKE_DIR = PROJECT_ROOT / "datalake"
SILVER_DIR = DATALAKE_DIR / "silver"
GOLD_DIR = DATALAKE_DIR / "gold"

# Create directories if they don't exist
SILVER_DIR.mkdir(parents=True, exist_ok=True)
GOLD_DIR.mkdir(parents=True, exist_ok=True)

print(f"[INFO] Project Root: {PROJECT_ROOT}")
print(f"[INFO] Datalake: {DATALAKE_DIR}")

# --- 2. Connect to MongoDB ---
# Use environment variable for MongoDB URI (optional, for flexibility)
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
client = MongoClient(MONGO_URI)
db = client["datalake"]

# --- 3. Extract from MongoDB ---
print("[INFO] Extracting data from MongoDB...")
medicine_details = list(db["medicines_raw_11000"].find())
medicine_dataset = list(db["medicines_raw_250k"].find())

df_details = pd.DataFrame(medicine_details)
df_dataset = pd.DataFrame(medicine_dataset)

# Remove MongoDB _id column
if '_id' in df_details.columns:
    df_details = df_details.drop('_id', axis=1)
if '_id' in df_dataset.columns:
    df_dataset = df_dataset.drop('_id', axis=1)

print(f"[INFO] medicine_details: {df_details.shape[0]} rows, {df_details.shape[1]} columns")
print(f"[INFO] medicine_dataset: {df_dataset.shape[0]} rows, {df_dataset.shape[1]} columns")

# --- 4. Transform: Normalize columns ---
df_details.columns = df_details.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('-', '_')
df_dataset.columns = df_dataset.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('-', '_')

# --- 5. Clean drug names ---
if 'name' in df_dataset.columns:
    df_dataset['drug_name'] = df_dataset['name'].str.lower().str.strip().str.replace(r'[^\w\s]', '', regex=True)
    df_dataset['drug_name'] = df_dataset['drug_name'].fillna('')

if 'medicine_name' in df_details.columns:
    df_details['drug_name'] = df_details['medicine_name'].str.lower().str.strip().str.replace(r'[^\w\s]', '', regex=True)
    df_details['drug_name'] = df_details['drug_name'].fillna('')

# --- 6. Handle side effects ---
# For df_dataset
side_effect_cols = [f'sideeffect{i}' for i in range(42)]
existing_se_cols = [col for col in side_effect_cols if col in df_dataset.columns]

if existing_se_cols:
    def clean_side_effects(row):
        effects = [v for v in row if pd.notna(v) and str(v).strip() != '']
        return effects if effects else []
    
    df_dataset['side_effects'] = df_dataset[existing_se_cols].apply(clean_side_effects, axis=1)
else:
    df_dataset['side_effects'] = [[] for _ in range(len(df_dataset))]

# For df_details
if 'side_effects' in df_details.columns:
    def parse_side_effects(val):
        if pd.isna(val) or val == '':
            return []
        try:
            effects = str(val).split(',')
            return [e.strip() for e in effects if e.strip()]
        except:
            return []
    
    df_details['side_effects'] = df_details['side_effects'].apply(parse_side_effects)
else:
    df_details['side_effects'] = [[] for _ in range(len(df_details))]

# --- 7. Remove duplicates ---
if 'drug_name' in df_dataset.columns:
    df_dataset = df_dataset.drop_duplicates(subset=['drug_name'])

if 'drug_name' in df_details.columns:
    df_details = df_details.drop_duplicates(subset=['drug_name'])

# --- 8. Remove empty drug names ---
df_dataset = df_dataset[df_dataset['drug_name'].str.strip() != '']
df_details = df_details[df_details['drug_name'].str.strip() != '']

# --- 9. Fill NaNs KOMPREHENSIF ---
for col in df_details.columns:
    if col != 'side_effects':
        if df_details[col].dtype == 'object':
            df_details[col] = df_details[col].fillna('').astype(str).str.strip()
        else:
            df_details[col] = df_details[col].fillna(0)

for col in df_dataset.columns:
    if col != 'side_effects':
        if df_dataset[col].dtype == 'object':
            df_dataset[col] = df_dataset[col].fillna('').astype(str).str.strip()
        else:
            df_dataset[col] = df_dataset[col].fillna(0)

# --- 10. Save to Silver Layer ---
silver_details_path = SILVER_DIR / "medicine_details_cleaned.parquet"
silver_dataset_path = SILVER_DIR / "medicine_dataset_cleaned.parquet"

df_details.to_parquet(silver_details_path, index=False)
df_dataset.to_parquet(silver_dataset_path, index=False)

print("[INFO] Silver layer created!")
print(f"[INFO] Saved to: {silver_details_path}")
print(f"[INFO] Saved to: {silver_dataset_path}")
print(f"[CHECK] df_details nulls: {df_details.isnull().sum().sum()}")
print(f"[CHECK] df_dataset nulls: {df_dataset.isnull().sum().sum()}")

# --- 11. Create Gold Table ---
print("\n[INFO] Creating Gold Layer...")

# Ensure required columns exist
catalog_cols = ['drug_name', 'active_ingredient', 'formulation', 'dosage', 'manufacturer']
for col in catalog_cols:
    if col not in df_details.columns:
        df_details[col] = ''
    if col not in df_dataset.columns:
        df_dataset[col] = ''

# Combine catalog & remove duplicates
catalog = pd.concat([df_details[catalog_cols], df_dataset[catalog_cols]], ignore_index=True)
catalog = catalog.drop_duplicates(subset=['drug_name']).fillna('').astype(str).apply(lambda x: x.str.strip())

print(f"[INFO] Catalog created: {len(catalog)} unique drugs")

# Aggregate side effects
se_exploded = df_dataset[['drug_name', 'side_effects']].explode('side_effects')
se_exploded = se_exploded[se_exploded['side_effects'].notna() & (se_exploded['side_effects'].astype(str).str.strip() != '')]
se_agg = se_exploded.groupby('drug_name')['side_effects'].apply(lambda x: list(set(x))).reset_index()

print(f"[INFO] Aggregated side effects for {len(se_agg)} drugs")

# Build Gold Table
gold = catalog.copy()
gold['side_effects'] = gold['drug_name'].map(se_agg.set_index('drug_name')['side_effects']).apply(lambda x: x if isinstance(x, list) else [])

# Final null protection
for col in catalog_cols:
    gold[col] = gold[col].fillna('').astype(str).str.strip()

# Validation
total_nulls = gold.isnull().sum().sum()
if total_nulls == 0:
    print("✅ Gold layer is clean!")
else:
    print(f"⚠️  {total_nulls} nulls remaining!")

# Statistics
print(f"[STATS] Gold table shape: {gold.shape}")
print(f"[STATS] Drugs with side effects: {(gold['side_effects'].apply(len) > 0).sum()}")
print(f"[STATS] Total side effects recorded: {gold['side_effects'].apply(len).sum()}")

# Sample
print("\n[SAMPLE] First 3 drugs with side effects:")
for idx, row in gold[gold['side_effects'].apply(len) > 0].head(3).iterrows():
    print(f"  - {row['drug_name']}: {len(row['side_effects'])} side effects")

# Save Gold Table
gold_path = GOLD_DIR / "gold_meds_combined.parquet"
gold.to_parquet(gold_path, index=False)

print(f"\n[SUCCESS] Gold table saved to: {gold_path}")
print("[SUCCESS] Gold table ready for Power BI!")