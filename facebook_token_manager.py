import os
import requests
from dotenv import load_dotenv

def load_env():
    """Laad de omgevingsvariabelen opnieuw."""
    load_dotenv()

def renew_access_token():
    """Vernieuwt het Facebook Access Token en slaat het op in de .env file."""
    load_env()

    app_id = os.getenv('FACEBOOK_APP_ID')
    app_secret = os.getenv('FACEBOOK_APP_SECRET')
    long_term_token = os.getenv('FACEBOOK_LONG_TERM_ACCESS_TOKEN')

    url = f"https://graph.facebook.com/v20.0/oauth/access_token?grant_type=fb_exchange_token&client_id={app_id}&client_secret={app_secret}&fb_exchange_token={long_term_token}"

    response = requests.get(url)
    if response.status_code == 200:
        new_token = response.json().get('access_token')
        if new_token:
            update_env_file(new_token)
            print(f"Access token renewed: {new_token}")
            return new_token
        else:
            raise Exception("No access token found in the response.")
    else:
        raise Exception(f"Failed to renew access token: {response.text}")

def update_env_file(new_token):
    """Schrijft het nieuwe token naar de .env file."""
    with open('.env', 'r') as file:
        lines = file.readlines()

    with open('.env', 'w') as file:
        for line in lines:
            if line.startswith('FACEBOOK_LONG_TERM_ACCESS_TOKEN'):
                file.write(f"FACEBOOK_LONG_TERM_ACCESS_TOKEN={new_token}\n")
            else:
                file.write(line)

    print(".env file updated with new access token.")
    load_env()  # Laad de nieuwe token direct in het script
