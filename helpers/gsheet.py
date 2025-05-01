from google.oauth2.service_account import Credentials
import gspread
import streamlit as st
import os
import json
from dotenv import load_dotenv
load_dotenv()

"""
Linha da planilha:
https://docs.google.com/spreadsheets/d/1ZaJfzTBwAjHh7LMRWcmaRBX9AhUir7bHFrhc-2AbTOs/
"""
def get_ss_url():
    # Prefer Streamlit Cloud secrets, fallback to .env
    return st.secrets["general"]["SS_URL"] if "general" in st.secrets else os.getenv("SS_URL")

def get_gspread_client():
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    if "google_service_account" in st.secrets:
        # 🟢 On Streamlit Cloud
        credentials = Credentials.from_service_account_info(
            st.secrets["google_service_account"],
            scopes=scope
        )
    else:
        # 🧪 On local dev: load from .env path
        credentials_path = os.getenv("GOOGLE_CREDENTIALS_PATH", ".streamlit/google_service_account.json")
        if not os.path.exists(credentials_path):
            raise FileNotFoundError(f"Credentials file not found at: {credentials_path}")

        credentials = Credentials.from_service_account_file(
            credentials_path,
            scopes=scope
        )

    client = gspread.authorize(credentials)
    return client

def push_to_ss():
    pass