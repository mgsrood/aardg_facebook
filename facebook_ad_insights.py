from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from google.cloud import bigquery
import pandas as pd
import os
from datetime import datetime, timedelta
import pandas_gbq as pd_gbq
from google.oauth2 import service_account
from facebook_token_manager import renew_access_token, load_env

def get_facebook_insights():
    
    # Load environment variables
    load_env()

    # GCP credentials
    gc_keys = os.getenv("AARDG_GOOGLE_CREDENTIALS")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gc_keys
    credentials = service_account.Credentials.from_service_account_file(gc_keys)
    project_id = credentials.project_id
    
    """Haalt Facebook Ads Insights op en schrijft ze weg naar BigQuery."""
    
    # Bepaal de datums
    start_date = (datetime.now() - timedelta(28)).strftime('%Y-%m-%d')
    end_date = datetime.now().strftime('%Y-%m-%d')

    # Facebook credentials
    my_app_id = os.getenv('FACEBOOK_APP_ID')
    my_app_secret = os.getenv('FACEBOOK_APP_SECRET')
    my_access_token = os.getenv('FACEBOOK_LONG_TERM_ACCESS_TOKEN')

    # Initialiseer de Facebook API
    try:
        FacebookAdsApi.init(app_id=my_app_id, app_secret=my_app_secret, access_token=my_access_token)
        print("API initialized successfully.")
    except Exception as e:
        print(f"Error initializing FacebookAdsApi: {e}")
        raise

    # Controleer de Ad Account ID
    ad_account_id = os.getenv('FACEBOOK_AD_ACCOUNT_ID')
    if not ad_account_id:
        raise ValueError("Ad Account ID ontbreekt in de .env file.")

    account = AdAccount(ad_account_id)

    # Velden voor de insights
    fields = [
        AdsInsights.Field.account_currency,
        AdsInsights.Field.account_id,
        AdsInsights.Field.account_name,
        AdsInsights.Field.campaign_name,
        AdsInsights.Field.campaign_id,
        AdsInsights.Field.clicks,
        AdsInsights.Field.date_start,
        AdsInsights.Field.impressions,
        AdsInsights.Field.frequency,
        AdsInsights.Field.cpm,
        AdsInsights.Field.cpp,
        AdsInsights.Field.reach,
        AdsInsights.Field.spend,
        AdsInsights.Field.actions,
        AdsInsights.Field.action_values,
        AdsInsights.Field.place_page_name,
        AdsInsights.Field.outbound_clicks,
        AdsInsights.Field.outbound_clicks_ctr,
        AdsInsights.Field.cost_per_unique_outbound_click,
    ]

    # Parameters voor de API-aanvraag
    params = {
        'time_range': {'since': start_date, 'until': end_date},
        'level': 'campaign',
        'time_increment': 1
    }

    # Ophalen van de insights
    try:
        insights = account.get_insights(fields=fields, params=params)
    except Exception as e:
        if 'Error validating access token' in str(e):
            print("Token expired, renewing...")
            new_token = renew_access_token()
            FacebookAdsApi.init(app_id=my_app_id, app_secret=my_app_secret, access_token=new_token)
            insights = account.get_insights(fields=fields, params=params)
        else:
            raise

    # Verwerken van de data
    data = []
    for insight in insights:
        actions = {action['action_type']: action['value'] for action in insight.get('actions', [])}
        action_values = {action['action_type']: action['value'] for action in insight.get('action_values', [])}

        outbound_clicks = next((click['value'] for click in insight.get('outbound_clicks', []) if click['action_type'] == 'outbound_click'), 0)
        outbound_clicks_ctr = next((click['value'] for click in insight.get('outbound_clicks_ctr', []) if click['action_type'] == 'outbound_click'), 0)
        cost_per_unique_outbound_click = next((click['value'] for click in insight.get('cost_per_unique_outbound_click', []) if click['action_type'] == 'outbound_click'), 0)

        data.append({
            'account_id': insight.get('account_id'),
            'account_name': insight.get('account_name'),
            'account_currency': insight.get('account_currency'),
            'campaign_id': insight.get('campaign_id'),
            'campaign_name': insight.get('campaign_name'),
            'date': insight.get('date_start'),
            'spend': insight.get('spend'),
            'reach': insight.get('reach'),
            'impressions': insight.get('impressions'),
            'frequency': insight.get('frequency'),
            'cpm': insight.get('cpm'),
            'cpp': insight.get('cpp'),
            'outbound_clicks': outbound_clicks,
            'outbound_clicks_ctr': outbound_clicks_ctr,
            'cost_per_unique_outbound_click': cost_per_unique_outbound_click,
            'purchases': actions.get('omni_purchase'),
            'purchase_value': action_values.get('omni_purchase'),
            'complete_registrations': actions.get('omni_complete_registration'),
        })

    # Zet de data om naar een DataFrame
    df = pd.DataFrame(data)

    # Zorg ervoor dat het DataFrame niet leeg is
    if df.empty:
        print("Geen nieuwe Facebook Ads data gevonden.")
        return

    # Correcte datatypes
    df = df.astype(str)

    # BigQuery configuratie
    dataset_id = os.getenv('DATASET_ID')
    table_id = os.getenv('INSIGHTS_TABLE_ID')
    table = f'{project_id}.{dataset_id}.{table_id}'

    # Initialize BigQuery client
    client = bigquery.Client()

    # Load bestaande data uit BigQuery
    query = f"""
    SELECT * FROM `{table}`
    WHERE DATE(date) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND CURRENT_DATE()
    """
    try:
        existing_df = pd_gbq.read_gbq(query, project_id=project_id)
    except Exception as e:
        print(f"Error loading existing data from BigQuery: {e}")
        existing_df = pd.DataFrame()

    # Combineer nieuwe data met bestaande data
    combined_df = pd.concat([existing_df, df]).drop_duplicates(subset=['account_id', 'campaign_id', 'date'], keep='last')

    # Schrijf de data naar een tijdelijke tabel in BigQuery
    temp_table = f'{project_id}.{dataset_id}.temp_{table_id}'
    pd_gbq.to_gbq(combined_df, temp_table, project_id=project_id, if_exists='replace')

    # Merge query om de hoofd tabel te updaten
    merge_query = f"""
    MERGE `{table}` T
    USING `{temp_table}` S
    ON T.account_id = S.account_id AND T.campaign_id = S.campaign_id AND T.date = S.date
    WHEN MATCHED THEN
        UPDATE SET
            T.account_name = S.account_name,
            T.account_currency = S.account_currency,
            T.spend = S.spend,
            T.reach = S.reach,
            T.impressions = S.impressions,
            T.frequency = S.frequency,
            T.cpm = S.cpm,
            T.cpp = S.cpp,
            T.outbound_clicks = S.outbound_clicks,
            T.outbound_clicks_ctr = S.outbound_clicks_ctr,
            T.cost_per_unique_outbound_click = S.cost_per_unique_outbound_click,
            T.purchases = S.purchases,
            T.purchase_value = S.purchase_value,
            T.complete_registrations = S.complete_registrations
    WHEN NOT MATCHED THEN
        INSERT (
            account_id, account_name, account_currency, campaign_id, campaign_name, date, spend, reach,
            impressions, frequency, cpm, cpp, outbound_clicks, outbound_clicks_ctr,
            cost_per_unique_outbound_click, purchases, purchase_value, complete_registrations
        )
        VALUES (
            S.account_id, S.account_name, S.account_currency, S.campaign_id, S.campaign_name, S.date, S.spend, S.reach,
            S.impressions, S.frequency, S.cpm, S.cpp, S.outbound_clicks, S.outbound_clicks_ctr,
            S.cost_per_unique_outbound_click, S.purchases, S.purchase_value, S.complete_registrations
        )
    """
    client.query(merge_query).result()
    client.delete_table(temp_table)

    print(f"Data written to BigQuery table {table}")

if __name__ == "__main__":
    get_facebook_insights()
