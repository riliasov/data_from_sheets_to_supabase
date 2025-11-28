from src.pipelines.current_sync import run_current_sync
from src.pipelines.historical_sync import run_historical_sync
from src.pipelines.references_sync import run_references_sync

def main():
    import argparse
    parser = argparse.ArgumentParser(description='ETL Runner')
    parser.add_argument('--scope', choices=['current', 'historical', 'references', 'datamarts', 'all'], 
                        required=True, help='Scope of sync')
    
    args = parser.parse_args()
    
    if args.scope in ['current', 'all']:
        run_current_sync()
    
    if args.scope in ['historical', 'all']:
        run_historical_sync()
        
    if args.scope in ['references', 'all']:
        run_references_sync()

    if args.scope in ['datamarts', 'all']:
        from src.pipelines.sync_data_marts import run_sync_data_marts
        run_sync_data_marts()

if __name__ == '__main__':
    main()
