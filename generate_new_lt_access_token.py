import requests
from dotenv import load_dotenv
import os

def update_env_file(new_token):
    with open('/Users/maxrood/werk/codering/aardg/projecten/facebook/.env', 'r') as file:
        lines = file.readlines()

    with open('/Users/maxrood/werk/codering/aardg/projecten/facebook/.env', 'w') as file:
        for line in lines:
            if line.startswith('FACEBOOK_LONG_TERM_ACCESS_TOKEN'):
                file.write(f"FACEBOOK_LONG_TERM_ACCESS_TOKEN={new_token}\n")
            else:
                file.write(line)

def renew_access_token():
    # Load environment variables from .env file
    load_dotenv()

    app_id = os.getenv('FACEBOOK_APP_ID')
    app_secret = os.getenv('FACEBOOK_APP_SECRET')
    short_lived_token = os.getenv('FACEBOOK_ACCESS_TOKEN')

    url = f"https://graph.facebook.com/v20.0/oauth/access_token?grant_type=fb_exchange_token&client_id={app_id}&client_secret={app_secret}&fb_exchange_token={short_lived_token}"

    response = requests.get(url)
    if response.status_code == 200:
        new_token = response.json()['access_token']
        update_env_file(new_token)
        print(f"Access token renewed: {new_token}")
        return new_token
    else:
        raise Exception(f"Failed to renew access token: {response.text}")



if __name__ == "__main__":
    renew_access_token()
