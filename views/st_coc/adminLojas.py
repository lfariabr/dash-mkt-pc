import streamlit as st
import pandas as pd
from google.oauth2.service_account import Credentials
from helpers.gsheet import get_gspread_client, get_ss_url
from utils.discord import send_discord_message

def load_page_adminLojas():
    """
    Load and display lojas data from Google Sheets.
    
    Returns:
        None
    """

    st.title("💎 Admin")
    st.markdown("---")
    st.subheader("Lista de Lojas")

    if st.button("Carregar"):
        send_discord_message(f"Loading data in page adminLojas")
        with st.spinner("Carregando dados..."):
            try:
                spreadsheet_url = get_ss_url()
                client = get_gspread_client()

                # Lojas
                sheet_name = client.open_by_url(spreadsheet_url)
                lojas = sheet_name.worksheet("lojas")
                dados_lojas = lojas.get_all_values()

                if not dados_lojas:
                    st.warning("No data found in the spreadsheet!")
                    st.stop()
                
                df_lojas = pd.DataFrame(dados_lojas[1:], columns=dados_lojas[0])
                st.subheader("Lojas")
                st.dataframe(
                    df_lojas,
                    hide_index=True,
                    height=len(dados_lojas) * 35,
                    use_container_width=True)

            except Exception as e:
                st.error(f"An error occurred: {str(e)}")
                st.info("Please check your Google Sheets credentials and connection.")