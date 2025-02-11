from f_modules.facebook_ad_insights import get_facebook_insights
from f_modules.config import determine_script_id
from f_modules.token_status import token_status
from f_modules.log import end_log, setup_logging
from dateutil.relativedelta import relativedelta
from google.oauth2 import service_account
from datetime import datetime, timedelta
from f_modules.env_tool import env_check
import logging
import time
import os

def main():
    
    env_check()

    # Script configuratie
    klant = "Aard'g"
    script = "Ad Insights"
    bron = 'Facebook'
    start_time = time.time()

    # GCP credentials
    gc_keys = os.getenv("AARDG_GOOGLE_CREDENTIALS")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gc_keys
    credentials = service_account.Credentials.from_service_account_file(gc_keys)
    project_id = credentials.project_id
    dataset_id = os.getenv('DATASET_ID')
    table_id = os.getenv('INSIGHTS_TABLE_ID')

    # Facebook variabelen
    app_id = os.getenv('FACEBOOK_APP_ID')
    app_secret = os.getenv('FACEBOOK_APP_SECRET')
    short_lived_token = os.getenv('FACEBOOK_ACCESS_TOKEN')
    ad_account_id = os.getenv('FACEBOOK_AD_ACCOUNT_ID')

    # Verbindingsinstellingen
    server = os.getenv('SERVER')
    database = os.getenv('DATABASE')
    username = os.getenv('GEBRUIKERSNAAM')
    password = os.getenv('PASSWORD')
    tabel = "Kosten"
    driver = '{ODBC Driver 18 for SQL Server}'
    greit_connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'

    # Script ID bepalen
    script_id = determine_script_id(greit_connection_string)

    # Set up logging (met database logging)
    setup_logging(greit_connection_string, klant, bron, script, script_id)

    try:
        # Token status bepalen
        access_token = token_status(app_id, app_secret)
        
        # Facebook inzichten ophalen
        get_facebook_insights(access_token, app_id, app_secret, ad_account_id, project_id, dataset_id, table_id)
        
    except Exception as e:
        logging.error(f"Script mislukt: {e}")

    # Eindtijd logging
    end_log(start_time)

if __name__ == '__main__':
    main()