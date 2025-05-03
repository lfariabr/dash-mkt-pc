
import streamlit as st
import pandas as pd
import asyncio
from apiCrm.resolvers.fetch_leadsByUserReportTest import fetch_and_process_leadsByUserReportTest
from components.date_input import date_input

async def fetch_leadsByUser(start_date, end_date):
    """
    Run both API calls concurrently to improve performance.
    """
    leads_data = fetch_and_process_leadsByUserReportTest(start_date, end_date)

    # Executing both fetches
    leads_data = await asyncio.gather(leads_data)
    return leads_data

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
            leads_data = asyncio.run(fetch_leadsByUser(start_date, end_date))

            # if not entries_data or not comments_data or not leads_data:
            if not leads_data:
                st.error("Não foi possível obter dados da API.")
                return pd.DataFrame()
            
            df_leads = pd.DataFrame(leads_data)
            
            st.success(f"Dados obtidos com sucesso via API: {len(df_leads)} registros carregados.")
            return df_leads

            
        except Exception as e:
            st.error(f"Erro ao buscar dados da API: {str(e)}")
            return pd.DataFrame()
    else:
        st.warning("Por favor, selecione um intervalo de datas.")
        return pd.DataFrame()

def load_page_testLeadsbU():
    """
    Load and display lojas data from Google Sheets.
    
    Returns:
        df_lojas: DataFrame containing lojas data
    """
    st.title("💎 Test Leads by User")
    st.markdown("---")

    start_date, end_date = date_input()
        
    if st.button("Carregar"):
        with st.spinner("Carregando dados..."):
            df_leads = load_data(start_date, end_date)
            
            if not df_leads.empty:
                st.subheader("Leads by User")
                st.dataframe(df_leads, hide_index=True)
            