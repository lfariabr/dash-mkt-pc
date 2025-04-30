import streamlit as st
import pandas as pd
from google.oauth2.service_account import Credentials
from helpers.gsheet import get_gspread_client, get_ss_url
from utils.discord import send_discord_message


def load_page_adminConsultoras():
    """
    Load and display consultoras data from Google Sheets.
    
    Returns:
        None
    """

    st.title("💎 Admin")
    st.markdown("---")
    st.subheader("Lista de Consultoras")

    if st.button("Carregar"):
        send_discord_message(f"Loading data in page adminConsultoras")
        with st.spinner("Carregando dados..."):

            try:
                spreadsheet_url = get_ss_url()
                client = get_gspread_client()

                # Consultoras
                sheet_name = client.open_by_url(spreadsheet_url)
                consultoras = sheet_name.worksheet("consultoras")
                dados_consultoras = consultoras.get_all_values()

                if not dados_consultoras:
                    st.warning("No data found in the spreadsheet!")
                    st.stop()
        
                df_consultoras = pd.DataFrame(dados_consultoras[1:], columns=dados_consultoras[0])
                st.subheader("Consultoras")
                st.dataframe(
                    df_consultoras,
                    hide_index=True,
                    height=len(dados_consultoras) * 35,
                    use_container_width=True)

            except Exception as e:
                st.error(f"An error occurred: {str(e)}")
                st.info("Please check your Google Sheets credentials and connection.")