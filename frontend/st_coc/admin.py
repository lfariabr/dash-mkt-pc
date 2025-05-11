import streamlit as st
import pandas as pd
from google.oauth2.service_account import Credentials
from helpers.gsheet import get_gspread_client, get_ss_url
from helpers.discord import send_discord_message
from frontend.st_coc.adminLojas import load_page_adminLojas
from frontend.st_coc.adminConsultoras import load_page_adminConsultoras
from frontend.st_coc.adminAtendentes import load_page_adminAtendentes

def load_page_admin():
    """
    Load and display admin data from Google Sheets.
    
    Returns:
        df_admin: DataFrame containing admin data
    """
    st.title("💎 Admin")
    st.markdown("---")
    st.subheader("Selecione uma opção abaixo 👇")

    with st.expander("Clique para analisar e gerenciar Lojas 🏬"):
        load_page_adminLojas()
    
    with st.expander("Clique para analisar e gerenciar Consultoras 👩‍💻"):
        load_page_adminConsultoras()
    
    with st.expander("Clique para analisar e gerenciar Atendentes 🛎️"):
        load_page_adminAtendentes()
    