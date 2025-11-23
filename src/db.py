import psycopg2
from sqlalchemy import create_engine

def get_db_connection(config):
    """
    Returns a raw psycopg2 connection.
    """
    try:
        conn = psycopg2.connect(config['SUPABASE_DB_URL'])
        return conn
    except Exception as e:
        raise Exception(f"Error connecting to database: {e}")

def get_db_engine(config):
    """
    Returns a SQLAlchemy engine.
    """
    try:
        engine = create_engine(config['SUPABASE_DB_URL'])
        return engine
    except Exception as e:
        raise Exception(f"Error creating database engine: {e}")
