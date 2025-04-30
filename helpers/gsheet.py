from google.oauth2.service_account import Credentials
import json
import gspread
import streamlit as st
import os
from dotenv import load_dotenv
import json

load_dotenv()

spreadsheet_url = os.getenv("SS_URL")

# https://docs.google.com/spreadsheets/d/1ZaJfzTBwAjHh7LMRWcmaRBX9AhUir7bHFrhc-2AbTOs/

def get_ss_url():
    return spreadsheet_url

def get_gspread_client():
    # Configure credentials
    credentials_path = os.getenv("GOOGLE_CREDENTIALS_PATH", ".streamlit/google_service_account.json")
    if not credentials_path:
        raise ValueError("Google credentials not found in environment variables.")
    
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    credentials = Credentials.from_service_account_file(
        credentials_path,
        scopes=scope
    )

    # Create client
    client = gspread.authorize(credentials)

    return client

def data_paster():
    pass