import streamlit as st
import pandas as pd
import asyncio
from apiCrm.resolvers.fetch_followUpEntriesReportTest import fetch_and_process_followUpEntriesReportTest
from apiCrm.resolvers.fetch_followUpsCommentsReportTest import fetch_and_process_followUpsCommentsReportTest
from components.date_input import date_input

async def fetch_followUpEntriesAndComments(start_date, end_date):
    """
    Run both API calls concurrently to improve performance.
    """
    entries_data = fetch_and_process_followUpEntriesReportTest(start_date, end_date)
    comments_data = fetch_and_process_followUpsCommentsReportTest(start_date, end_date)

    # Executing both fetches
    entries_data, comments_data = await asyncio.gather(entries_data, comments_data)
    return entries_data, comments_data

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
            entries_data, comments_data = asyncio.run(fetch_followUpEntriesAndComments(start_date, end_date))

            if not entries_data or not comments_data:
                st.error("Não foi possível obter dados da API.")
                return pd.DataFrame()
            
            df_entries = pd.DataFrame(entries_data)
            df_comments = pd.DataFrame(comments_data)
            
            st.success(f"Dados obtidos com sucesso via API: {len(df_entries)} registros carregados.")
            return df_entries, df_comments
            
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
            df_entries, df_comments = load_data(start_date, end_date)
            
            if not df_entries.empty or not df_comments.empty:
                st.subheader("Follow-ups entries")
                st.dataframe(df_entries, hide_index=True)
                st.subheader("Follow-ups comments")
                st.dataframe(df_comments, hide_index=True)
            