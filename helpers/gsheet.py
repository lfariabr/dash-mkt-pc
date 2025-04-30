from google.oauth2.service_account import Credentials
import json
import gspread
import streamlit as st

def get_ss_url():
    return st.secrets["gsheets"]["spreadsheet_url"]

def get_gspread_client():
    # Get credentials from secrets
    credentials = json.loads(st.secrets["gsheets"]["credentials"])
    spreadsheet_url = st.secrets["gsheets"]["spreadsheet_url"]

    # Configure credentials
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    credentials = Credentials.from_service_account_info(
        credentials,
        scopes=scope
    )

    # Create client
    client = gspread.authorize(credentials)

    return client
