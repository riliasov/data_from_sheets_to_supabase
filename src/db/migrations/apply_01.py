import sys
import os
import sqlalchemy
from sqlalchemy import text

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))

from src.config import load_config

def apply_migration():
    print("🏗️ Применение миграции 01_fix_trainings_cur...")
    
    config = load_config()
    db_url = config.get('SUPABASE_DB_URL')
    engine = sqlalchemy.create_engine(db_url, isolation_level="AUTOCOMMIT")
    
    migration_path = os.path.join(os.path.dirname(__file__), '01_fix_trainings_cur.sql')
    
    with open(migration_path, 'r', encoding='utf-8') as f:
        sql = f.read()
        
    with engine.connect() as connection:
        connection.execute(text(sql))
        print("✅ Миграция успешно применена!")

if __name__ == "__main__":
    apply_migration()
