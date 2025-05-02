import streamlit as st
import pandas as pd
import asyncio
from apiCrm.resolvers.fetch_followUpEntriesReportTest import fetch_and_process_followUpEntriesReportTest
from components.date_input import date_input

def load_data(start_date=None, end_date=None, use_api=True):
    """
    Load and preprocess appointments by user data.
    
    Args:
        start_date (str, optional): Start date in YYYY-MM-DD format for API fetch
        end_date (str, optional): End date in YYYY-MM-DD format for API fetch
        use_api (bool): Whether to use the API (default) or fallback mechanism
        
    Returns:
        DataFrame: Processed appointments by user dataframe
    """
    
    if start_date and end_date:
        try:
            # Run the async function using asyncio
            appointments_data = asyncio.run(fetch_and_process_followUpEntriesReportTest(start_date, end_date))

            if not appointments_data:
                st.error("Não foi possível obter dados da API.")
                return pd.DataFrame()
            
            df = pd.DataFrame(appointments_data)
            
            st.success(f"Dados obtidos com sucesso via API: {len(df)} registros carregados.")
            return df
            
        except Exception as e:
            st.error(f"Erro ao buscar dados da API: {str(e)}")
            return pd.DataFrame()
    else:
        st.warning("Por favor, selecione um intervalo de datas.")
        return pd.DataFrame()

def load_page_test():
    """
    Load and display lojas data from Google Sheets.
    
    Returns:
        df_lojas: DataFrame containing lojas data
    """
    st.title("💎 Test")
    st.markdown("---")

    start_date, end_date = date_input()
        
    if st.button("Carregar"):
        with st.spinner("Carregando dados..."):
            df = load_data(start_date, end_date)
            
            if not df.empty:
                st.dataframe(df, hide_index=True)
            