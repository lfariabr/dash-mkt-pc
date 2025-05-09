import streamlit as st
import pandas as pd
from google.oauth2.service_account import Credentials
from helpers.gsheet import get_gspread_client, get_ss_url

def get_stores_from_spreadsheet():
    """
    Get atendentes data from Google Sheets and return DataFrames for morning and afternoon shifts.
    
    Returns:
        tuple: (df_stores)
    
    Raises:
        Exception: If there's an error accessing the spreadsheet
    """
    try:
        spreadsheet_url = get_ss_url()
        client = get_gspread_client()

        # Atendentes
        sheet_name = client.open_by_url(spreadsheet_url)
        stores = sheet_name.worksheet("lojas")
        dados_stores = stores.get_all_values()
        
        if len(dados_stores) <= 1:  # Only header or empty
            st.warning("Planilha de lojas vazia ou sem dados")
            return pd.DataFrame()
            
        df_stores = pd.DataFrame(dados_stores[1:], columns=dados_stores[0])
        
        # Ensure required columns exist
        required_columns = ['Unidade', 'Tam']
        if not all(col in df_stores.columns for col in required_columns):
            st.error(f"Planilha de lojas deve conter as colunas: {', '.join(required_columns)}")
            return pd.DataFrame()
            
        # Clean up data
        df_stores['Unidade'] = df_stores['Unidade'].str.strip()
        
        return df_stores
        
    except Exception as e:
        st.error(f"Erro ao carregar dados das lojas: {str(e)}")
        return pd.DataFrame()
