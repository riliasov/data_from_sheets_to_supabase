import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from src.data_marts.aggregator import build_all_datamarts

# Get absolute path to secrets/.env
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
env_path = os.path.join(base_dir, "secrets", ".env")
load_dotenv(env_path)

# Setup DB connection
DATABASE_URL = os.getenv("SUPABASE_DB_URL")
if not DATABASE_URL: 
    print("ERROR: SUPABASE_DB_URL not found")
    exit(1)

try:
    engine = create_engine(DATABASE_URL)
except Exception as e:
    print(f"Error creating engine: {e}")
    exit(1)

def check_client(client_name):
    print(f"Checking client: {client_name}")
    print("=" * 80)
    
    # Run full aggregation
    datamarts = build_all_datamarts(engine)
    balance_df = datamarts['balance']
    
    # Filter for client
    mask = balance_df['klient_id'].str.contains(client_name, case=False, na=False)
    client_df = balance_df[mask]
    
    if client_df.empty:
        print(f"Client '{client_name}' not found!")
        return
    
    # Show results
    cols = ['klient_id', 'type', 'category', 'total_summa', 'priobreteno', 'experience', 
            'propusk', 'loyalnyy_propusk', 'otmena_po_vine_tsentra', 'propusk_bez_spisaniya',
            'vozvrashcheno', 'pereraschet', 'podareno', 'sgorelo', 'ostatok']
    
    print("\nResults:")
    for idx, row in client_df.iterrows():
        print(f"\nClient: {row['klient_id']}")
        print(f"Type: {row['type']}, Category: {row['category']}")
        print(f"Total Summa: {row['total_summa']}")
        print(f"Priobreteno: {row['priobreteno']}")
        print(f"Experience: {row['experience']}")
        print(f"Propusk: {row['propusk']}")
        print(f"Loyalnyy Propusk: {row['loyalnyy_propusk']}")
        print(f"Otmena po vine tsentra: {row['otmena_po_vine_tsentra']}")
        print(f"Propusk bez spisaniya: {row['propusk_bez_spisaniya']}")
        print(f"Vozvrashcheno: {row['vozvrashcheno']}")
        print(f"Pereraschet: {row['pereraschet']}")
        print(f"Podareno: {row['podareno']}")
        print(f"Sgorelo: {row['sgorelo']}")
        print(f"Ostatok: {row['ostatok']}")
        print("-" * 40)

if __name__ == "__main__":
    # Test cases
    print("\n### Test 1: Ахмерова Муршида Аскар ###")
    check_client("Ахмерова Муршида Аскар")
    
    print("\n\n### Test 2: Абдрафимова Лилия Лейла ###")
    check_client("Абдрафимова Лилия Лейла")
