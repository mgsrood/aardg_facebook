from f_modules.env_tool import determine_base_dir
import requests
import logging
import json
import os

def get_facebook_long_term_token(short_lived_token, app_id, app_secret):
    """Extends a short-lived Facebook access token to a long-term token"""

    url = f"https://graph.facebook.com/v20.0/oauth/access_token?" \
          f"grant_type=fb_exchange_token&client_id={app_id}&client_secret={app_secret}" \
          f"&fb_exchange_token={short_lived_token}"

    response = requests.get(url, timeout=120)

    if response.status_code == 200:
        new_long_term_token = response.json()['access_token']
        logging.info(f"Long-term access token generated")
        
        # Save token
        update_token(new_long_term_token)
        
        return new_long_term_token
    else:
        raise Exception(f"Error extending token: {response.text}")

def update_token(new_token):
    """Saves the new token in tokens.json"""
    
    base_dir = determine_base_dir()
    token_file = os.path.join(base_dir, 'projecten', 'facebook', 'tokens.json')

    tokens = {"facebook_long_term_access_token": new_token}

    try:
        with open(token_file, 'w') as file:
            json.dump(tokens, file, indent=4)
    except Exception as e:
        logging.error(f"Updaten token mislukt: {e}")

    logging.info("Token successfully updated and saved in tokens.json")