import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os

def get_sheets_client(config):
    """
    Returns an authenticated gspread client.
    """
    creds_file = config['GOOGLE_SHEETS_CREDENTIALS_FILE']
    
    if not os.path.exists(creds_file):
        raise FileNotFoundError(f"Credentials file not found at: {creds_file}")
        
    scope = ['https://www.googleapis.com/auth/spreadsheets.readonly', 'https://www.googleapis.com/auth/drive.readonly']
    
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name(creds_file, scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        raise Exception(f"Error connecting to Google Sheets: {e}")
