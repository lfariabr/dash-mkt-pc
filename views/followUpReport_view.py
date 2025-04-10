import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import asyncio
from apiCrm.resolvers.fetch_followUpEntriesReport import fetch_and_process_followUpEntriesReport
from apiCrm.resolvers.fetch_followUpsCommentsReport import fetch_and_process_followUpsCommentsReport

async def fetch_all_data(start_date, end_date):
    """Run both API calls concurrently to improve performance"""
    entries_task = fetch_and_process_followUpEntriesReport(start_date, end_date)
    comments_task = fetch_and_process_followUpsCommentsReport(start_date, end_date)
    
    # Execute both tasks concurrently
    entries_data, comments_data = await asyncio.gather(entries_task, comments_task)
    return entries_data, comments_data

def load_data(start_date=None, end_date=None):
    if start_date and end_date:
        try:
            # Run both queries concurrently with a single asyncio.run call
            entries_data, comments_data = asyncio.run(fetch_all_data(start_date, end_date))
            return pd.DataFrame(entries_data), pd.DataFrame(comments_data)
        except Exception as e:
            st.error(f"Erro ao carregar dados: {str(e)}")
            return pd.DataFrame(), pd.DataFrame()
    else:
        st.warning("Por favor, selecione um intervalo de datas.")
        return pd.DataFrame(), pd.DataFrame()


def load_page_followUpReport_and_followUpCommentsReport():
    """
    Main function to display follow-up report data.
    """

    st.title("📊 2 - Relatório de Pós-Vendas")
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

        df_entries, df_comments = load_data(start_date, end_date)
        
        st.markdown("---")
        st.subheader("Novos Pós-Vendas")

        entries_columns = ['name', 'follow_ups_count', 'customer_ids']
        df_entries = df_entries[entries_columns]
        df_entries = df_entries.rename(columns={
            'name': 'Consultora de Vendas',
            'follow_ups_count': 'Novos Pós-Vendas',
            'customer_ids': 'ID dos Clientes'
        })
        df_entries = df_entries.reset_index(drop=True)
        # Sort by Consultora de Vendas
        df_entries = df_entries.sort_values(by='Novos Pós-Vendas', ascending=False)
        st.dataframe(df_entries, hide_index=True)
        
        st.markdown("---")
        st.subheader("Comentários de Pós-Vendas")

        comments_columns = ['name', 'comments_count', 'comments_customer_ids']
        df_comments = df_comments[comments_columns]
        df_comments = df_comments.rename(columns={
            'name': 'Consultora de Vendas',
            'comments_count': 'Comentários de Pós-Vendas',
            'comments_customer_ids': 'ID dos Clientes'
        })
        df_comments = df_comments.reset_index(drop=True)
        # Sort by Consultora de Vendas
        df_comments = df_comments.sort_values(by='Comentários de Pós-Vendas', ascending=False)
        st.dataframe(df_comments, hide_index=True)