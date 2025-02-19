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
from dateutil.relativedelta import relativedelta

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
    response = requests.get(url, timeout=120)
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

def get_facebook_insights(start_date, end_date):
    # Facebook app credentials
    my_app_id = os.getenv('FACEBOOK_APP_ID')
    my_app_secret = os.getenv('FACEBOOK_APP_SECRET')
    my_access_token =  os.getenv('FACEBOOK_LONG_TERM_ACCESS_TOKEN')

    # Initialise the API
    try:
        FacebookAdsApi.init(app_id=my_app_id, app_secret=my_app_secret, access_token=my_access_token)
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
            'account_id': str(insight.get('account_id')),
            'account_name': str(insight.get('account_name')),
            'account_currency': str(insight.get('account_currency')),
            'campaign_id': str(insight.get('campaign_id')),
            'campaign_name': str(insight.get('campaign_name')),
            'date': str(insight.get('date_start')),
            'spend': str(insight.get('spend')),
            'reach': str(insight.get('reach')),
            'impressions': str(insight.get('impressions')),
            'frequency': str(insight.get('frequency')),
            'cpm': str(insight.get('cpm')),
            'cpp': str(insight.get('cpp')),
            'outbound_clicks': str(outbound_clicks),
            'outbound_clicks_ctr': str(outbound_clicks_ctr),
            'cost_per_unique_outbound_click': str(cost_per_unique_outbound_click),
            'purchases': str(actions.get('omni_purchase')),
            'purchase_value': str(action_values.get('omni_purchase')),
            'complete_registrations': str(actions.get('omni_complete_registration')),
        })

    # Turn lists into a DataFrame
    df = pd.DataFrame(data)

    # Convert all columns to string type
    df = df.astype(str)

    # BigQuery configuration
    project_id = os.getenv('PROJECT_ID')
    dataset_id = os.getenv('DATASET_ID')
    table_id = os.getenv('INSIGHTS_TABLE_ID')
    table = f'{project_id}.{dataset_id}.{table_id}'

    # Write DataFrame to BigQuery
    pd_gbq.to_gbq(df, table, project_id=project_id, if_exists='append')

    print(f"Data written to BigQuery table {table}")

if __name__ == "__main__":
    end_date = datetime.now()
    start_date = end_date - relativedelta(months=37)

    current_start_date = start_date

    while current_start_date < end_date:
        current_end_date = current_start_date + timedelta(days=30)
        if current_end_date > end_date:
            current_end_date = end_date

        start_date_str = current_start_date.strftime('%Y-%m-%d')
        end_date_str = current_end_date.strftime('%Y-%m-%d')

        print(f"Fetching data from {start_date_str} to {end_date_str}")
        get_facebook_insights(start_date_str, end_date_str)

        current_start_date = current_end_date + timedelta(days=1)
