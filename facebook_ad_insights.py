from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from dotenv import load_dotenv
import os
import pandas as pd
from datetime import datetime, timedelta
import requests
from google.cloud import bigquery
import pandas_gbq as pd_gbq
from google.oauth2 import service_account

# Load environment variables from .env file
load_dotenv()

# Get the GCP keys
gc_keys = os.getenv("AARDG_GOOGLE_CREDENTIALS")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gc_keys

credentials = service_account.Credentials.from_service_account_file(gc_keys)
project_id = credentials.project_id

def renew_access_token():
    app_id = os.getenv('FACEBOOK_APP_ID')
    app_secret = os.getenv('FACEBOOK_APP_SECRET')
    long_term_token = os.getenv('FACEBOOK_LONG_TERM_ACCESS_TOKEN')

    url = f"https://graph.facebook.com/v20.0/oauth/access_token?grant_type=fb_exchange_token&client_id={app_id}&client_secret={app_secret}&fb_exchange_token={long_term_token}"
    response = requests.get(url)
    if response.status_code == 200:
        new_token = response.json()['access_token']
        update_env_file(new_token)
        print(f"Access token renewed: {new_token}")
        return new_token
    else:
        raise Exception(f"Failed to renew access token: {response.text}")

def update_env_file(new_token):
    with open('.env', 'r') as file:
        lines = file.readlines()

    with open('.env', 'w') as file:
        for line in lines:
            if line.startswith('FACEBOOK_LONG_TERM_ACCESS_TOKEN'):
                file.write(f"FACEBOOK_LONG_TERM_ACCESS_TOKEN={new_token}\n")
            else:
                file.write(line)

def get_facebook_insights():
    # Dates
    future = datetime.now() - timedelta(0)
    past = datetime.now() - timedelta(28)
    start_date = past.strftime('%Y-%m-%d')
    end_date = future.strftime('%Y-%m-%d')

    # Facebook app credentials
    my_app_id = os.getenv('FACEBOOK_APP_ID')
    my_app_secret = os.getenv('FACEBOOK_APP_SECRET')
    my_access_token =  os.getenv('FACEBOOK_LONG_TERM_ACCESS_TOKEN')

    # Initialise the API
    try:
        FacebookAdsApi.init(app_id=my_app_id, app_secret=my_app_secret, access_token=my_access_token)
        print("API initialized successfully.")
    except Exception as e:
        print(f"Error initializing FacebookAdsApi: {e}")
        raise

    # Specify the Ad Account ID
    ad_account_id = os.getenv('FACEBOOK_AD_ACCOUNT_ID')

    if not ad_account_id:
        raise ValueError("Ad Account ID is not set. Please check your .env file and ensure FACEBOOK_AD_ACCOUNT_ID is correctly set.")

    # Define the AdAccount object
    try:
        account = AdAccount(ad_account_id)
        print(f"AdAccount object created: {account}")
    except Exception as e:
        print(f"Error creating AdAccount object: {e}")
        raise

    # Define the fields you want to retrieve
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

    # Define the parameters
    params = {
        'time_range': {
            'since': start_date,
            'until': end_date
        },
        'level': 'campaign',
        'time_increment': 1
    }

    # Retrieve insights
    try:
        insights = account.get_insights(fields=fields, params=params)
    except Exception as e:
        if 'Error validating access token' in str(e):
            print("Access token expired, renewing token...")
            new_token = renew_access_token()
            FacebookAdsApi.init(app_id=my_app_id, app_secret=my_app_secret, access_token=new_token)
            insights = account.get_insights(fields=fields, params=params)
        else:
            raise

    # Create lists to turn insights into a DataFrame
    data = []

    for insight in insights:
        actions = {action['action_type']: action['value'] for action in insight.get('actions', [])}
        action_values = {action['action_type']: action['value'] for action in insight.get('action_values', [])}

        outbound_clicks = 0
        if 'outbound_clicks' in insight:
            for click in insight['outbound_clicks']:
                if click['action_type'] == 'outbound_click':
                    outbound_clicks = click['value']
                    break

        outbound_clicks_ctr = 0
        if 'outbound_clicks_ctr' in insight:
            for click in insight['outbound_clicks_ctr']:
                if click['action_type'] == 'outbound_click':
                    outbound_clicks_ctr = click['value']
                    break

        cost_per_unique_outbound_click = 0
        if 'cost_per_unique_outbound_click' in insight:
            for click in insight['cost_per_unique_outbound_click']:
                if click['action_type'] == 'outbound_click':
                    cost_per_unique_outbound_click = click['value']
                    break   

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

    # Turn lists into a DataFrame
    df = pd.DataFrame(data)

    # Correct data types
    df = df.astype(str)

    # BigQuery configuration
    project_id = os.getenv('PROJECT_ID')
    dataset_id = os.getenv('DATASET_ID')
    table_id = os.getenv('INSIGHTS_TABLE_ID')
    table = f'{project_id}.{dataset_id}.{table_id}'

    # Initialize BigQuery client
    client = bigquery.Client()

    # Load existing data from BigQuery
    query = f"""
    SELECT * FROM `{table}`
    WHERE DATE(date) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND CURRENT_DATE()
    """
    existing_df = pd_gbq.read_gbq(query, project_id=project_id)

    # Combine new data with existing data
    combined_df = pd.concat([existing_df, df])

    # Drop duplicates based on 'account_id', 'campaign_id', and 'date'
    combined_df = combined_df.drop_duplicates(subset=['account_id', 'campaign_id', 'date'], keep='last')

    # Write combined DataFrame to a temporary table in BigQuery
    temp_table = f'{project_id}.{dataset_id}.temp_{table_id}'
    pd_gbq.to_gbq(combined_df, temp_table, project_id=project_id, if_exists='replace')

    # Use MERGE statement to update the main table with the data from the temporary table
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

    # Drop the temporary table
    client.delete_table(temp_table)

    print(f"Data written to BigQuery table {table}")

if __name__ == "__main__":
    get_facebook_insights()
