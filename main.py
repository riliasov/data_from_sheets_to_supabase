import argparse
import sys
from src.config import load_config
from src.db import get_db_connection
from src.sheets import get_sheets_client

def main():
    parser = argparse.ArgumentParser(description='ETL for Google Sheets to Supabase')
    parser.add_argument('--sync', type=str, choices=['all', 'historical', 'current', 'clients', 'references'], 
                        help='Which part of the ETL to run')
    parser.add_argument('--check-connection', action='store_true', help='Check connections to services')
    
    args = parser.parse_args()
    
    try:
        config = load_config()
        print("Configuration loaded.")
    except Exception as e:
        print(f"Error loading configuration: {e}")
        sys.exit(1)

    if args.check_connection:
        print("Checking connections...")
        try:
            # Check DB
            conn = get_db_connection(config)
            print("Database connection successful.")
            conn.close()
            
            # Check Sheets
            gc = get_sheets_client(config)
            print("Google Sheets connection successful.")
            
        except Exception as e:
            print(f"Connection check failed: {e}")
            sys.exit(1)
        return

    if not args.sync:
        parser.print_help()
        return

    print(f"Starting sync: {args.sync}")
    # TODO: Implement sync logic dispatch
    
if __name__ == '__main__':
    main()
