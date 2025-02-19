from f_modules.long_term_token import get_facebook_long_term_token
from f_modules.env_tool import determine_base_dir
import requests
import logging
import signal
import json
import os

class TimeoutException(Exception):
    pass

def timeout_handler(signum, frame):
    """Timeout handler die een uitzondering gooit als er te lang gewacht wordt op input"""
    raise TimeoutException

def load_token():
    """Laad de opgeslagen token uit tokens.json"""
    
    # Locatie bepalen
    base_dir = determine_base_dir()
    token_file = os.path.join(base_dir, 'projecten', 'facebook', 'tokens.json')
    
    if os.path.exists(token_file):
        with open(token_file, 'r') as file:
            try:
                tokens = json.load(file)
                return tokens.get("facebook_long_term_access_token", None)
            except json.JSONDecodeError:
                return None
    return None

def check_token_validity(access_token, app_id, app_secret):
    """Controleer of de huidige token nog geldig is"""

    debug_url = f"https://graph.facebook.com/debug_token?input_token={access_token}&access_token={app_id}|{app_secret}"
    response = requests.get(debug_url, timeout=120)
    
    if response.status_code == 200:
        data = response.json()
        return data["data"]["is_valid"]
    else:
        return False

def token_status(app_id, app_secret):
    
    # Laad huidige token
    access_token = load_token()

    # Geldigheid valideren
    valid = check_token_validity(access_token, app_id, app_secret)

    if valid == True:
        logging.info("Token nog geldig")
        return access_token
    else:
        logging.info("Token niet meer geldig, nieuwe aanvragen")
        
        # Timeout instellen voor input
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(10)  # Start de timer
        
        try:
            short_lived_token = input("Token invoeren: ").strip()
            signal.alarm(0)

            if short_lived_token:
                new_long_term_token = get_facebook_long_term_token(short_lived_token, app_id, app_secret)
                return new_long_term_token
            else:
                print("Ongeldige invoer.")
            return None
    
        except TimeoutException:
            logging.error("Geen input ontvangen binnen de tijdslimiet. Taak wordt afgebroken.")
            return None 
