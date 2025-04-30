import streamlit as st
import pandas as pd
from google.oauth2.service_account import Credentials
from helpers.gsheet import get_gspread_client, get_ss_url
from utils.discord import send_discord_message
def load_page_adminAtendentes():
    """
    Load and display atendentes data from Google Sheets.
    
    Returns:
        None
    """

    st.title("💎 Admin")
    st.markdown("---")
    st.subheader("Lista de Atendentes")

    if st.button("Carregar"):
        send_discord_message(f"Loading data in page adminAtendentes")
        with st.spinner("Carregando dados..."):
            try:
                spreadsheet_url = get_ss_url()
                client = get_gspread_client()

                # Atendentes
                sheet_name = client.open_by_url(spreadsheet_url)
                atendentes = sheet_name.worksheet("atendentes")
                dados_atendentes = atendentes.get_all_values()

                if not dados_atendentes:
                    st.warning("No data found in the spreadsheet!")
                    st.stop()
                
                df_atendentes = pd.DataFrame(dados_atendentes[1:], columns=dados_atendentes[0])
                st.subheader("Atendentes")
                st.dataframe(
                    df_atendentes,
                    hide_index=True,
                    height=len(dados_atendentes) * 35,
                    use_container_width=True)

            except Exception as e:
                st.error(f"An error occurred: {str(e)}")
                st.info("Please check your Google Sheets credentials and connection.")