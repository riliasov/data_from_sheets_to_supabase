#!/usr/bin/env python3
"""Execute drop_datamarts.sql migration"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.config import load_config
import sqlalchemy
from sqlalchemy import text

def run_migration():
    print("🗑️ Dropping datamarts schema...")
    
    config = load_config()
    db_url = config.get('SUPABASE_DB_URL')
    
    if not db_url:
        print("❌ Error: SUPABASE_DB_URL not found")
        return False
    
    engine = sqlalchemy.create_engine(db_url)
    
    # Read SQL file
    sql_file = os.path.join(os.path.dirname(__file__), 'drop_datamarts.sql')
    with open(sql_file, 'r') as f:
        sql = f.read()
    
    try:
        with engine.connect() as conn:
            conn.execute(text(sql))
            conn.commit()
        print("✅ Successfully dropped datamarts schema")
        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    finally:
        engine.dispose()

if __name__ == "__main__":
    run_migration()
