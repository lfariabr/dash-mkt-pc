import requests
from dotenv import load_dotenv
import os
import streamlit as st

load_dotenv()

WEBHOOK_DISCORD = os.getenv('WEBHOOK_DISCORD')

def send_discord_message(message):
    url = WEBHOOK_DISCORD
    data = {"content": message}
    response = requests.post(url, json=data)
    return response.status_code