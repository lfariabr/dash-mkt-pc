import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import asyncio
from apiCrm.resolvers.fetch_leadsByUserReport import fetch_and_process_leadsByUserReport

def load_data(start_date=None, end_date=None):
    if start_date and end_date:
        try:
            leads_data = asyncio.run(fetch_and_process_leadsByUserReport(start_date, end_date))
            return pd.DataFrame(leads_data)
        except Exception as e:
            st.error(f"Erro ao carregar dados: {str(e)}")
            return pd.DataFrame()
    else:
        st.warning("Por favor, selecione um intervalo de datas.")
        return pd.DataFrame()


def create_time_filtered_df(df, days=None):
    if days:
        cutoff_date = datetime.now() - timedelta(days=days)
        return df[df['Dia da entrada'] >= cutoff_date]
    return df

def load_page_leadsByUser():
    """Main function to display leads by user data."""
    st.title("📊 1 - Puxada de Leads")

    st.markdown("---")

    st.subheader("Selecione o intervalo de datas para o relatório:")
    
    start_date = None
    end_date = None
    
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "Data Inicial",
            value=datetime.now() - timedelta(days=1),
            max_value=datetime.now()
        ).strftime('%Y-%m-%d')
    with col2:
        end_date = st.date_input(
            "Data Final",
            value=datetime.now(),
            max_value=datetime.now()
        ).strftime('%Y-%m-%d')
    
    if st.button("Carregar"):
        df_leadsByUser = load_data(start_date, end_date)
    
        # Treating data columns:
        leadsByUserColumns = ['name', 'messages_count']
        df_leadsByUser = df_leadsByUser[leadsByUserColumns]
        df_leadsByUser = df_leadsByUser.reset_index(drop=True)
        
        st.dataframe(df_leadsByUser)