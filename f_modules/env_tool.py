from dotenv import load_dotenv
import logging
import os

def determine_base_dir():
    if "maxrood" in os.path.expanduser("~"):  # Specifiek voor je MacBook
            return "/Users/maxrood/werk/greit/klanten/aardg/"
    else:  # Voor je VM
        return "/home/maxrood/aardg/"

def env_check():
    base_dir = determine_base_dir()
    env_path = os.path.join(base_dir, '.env')
    
    if os.path.exists(env_path):
            load_dotenv()
            logging.info("Lokaal draaien: .env bestand gevonden en geladen.")
    else:
        logging.info("Draaien in productieomgeving: .env bestand niet gevonden.")