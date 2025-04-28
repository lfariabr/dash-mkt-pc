import asyncio
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
from components.date_input import date_input
from apiCrm.resolvers.fetch_followUpEntriesReport import fetch_and_process_followUpEntriesReport
from apiCrm.resolvers.fetch_followUpsCommentsReport import fetch_and_process_followUpsCommentsReport
from apiCrm.resolvers.fetch_grossSalesReport import fetch_and_process_grossSales_report
from views.coc.consultoras import consultoras_manha, consultoras_tarde
from views.coc.columns import (
    followUpEntries_display_columns,
    followUpEntries_display_columns_initial_columns,
    followUpComments_display_columns,
    followUpComments_display_columns_initial_columns,
    grossSales_display_columns
)
from helpers.data_wrestler import highlight_total_row, append_totals_row, enrich_consultora_df

async def fetch_all_data(start_date, end_date):
    """Run both API calls concurrently to improve performance"""
    entries_task = fetch_and_process_followUpEntriesReport(start_date, end_date)
    comments_task = fetch_and_process_followUpsCommentsReport(start_date, end_date)
    gross_sales_task = fetch_and_process_grossSales_report(start_date, end_date)
    
    # Execute both tasks concurrently
    entries_data, comments_data, gross_sales_data = await asyncio.gather(entries_task, comments_task, gross_sales_task)
    return entries_data, comments_data, gross_sales_data

def load_data(start_date=None, end_date=None):
    if start_date and end_date:
        try:
            # Run both queries concurrently with a single asyncio.run call
            entries_data, comments_data, gross_sales_data = asyncio.run(fetch_all_data(start_date, end_date))
            return pd.DataFrame(entries_data), pd.DataFrame(comments_data), pd.DataFrame(gross_sales_data)
        except Exception as e:
            st.error(f"Erro ao carregar dados: {str(e)}")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    else:
        st.warning("Por favor, selecione um intervalo de datas.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


def load_page_followUpReport_and_followUpCommentsReport():
    """
    Main function to display follow-up report data.
    """

    st.title("📊 2 - Relatório de Pós-Vendas")
    st.markdown("---")
    st.subheader("Selecione o intervalo de datas para o relatório:")
    
    start_date, end_date = date_input()
    
    if st.button("Carregar"):
        from utils.discord import send_discord_message
        send_discord_message(f"Loading data in page followUpReport_view")
        with st.spinner("Carregando dados..."):

            df_entries, df_comments, df_gross_sales = load_data(start_date, end_date)
            if df_gross_sales.empty:
                st.error("Não foi possível obter dados de vendas.")
                df_gross_sales['chargableTotal'] = 0 
                grouped_columns = ['Consultora de Vendas', 'Valor líquido', 'Pedidos']
                df_gross_sales_manha = pd.DataFrame(columns=grouped_columns)
                df_gross_sales_tarde = pd.DataFrame(columns=grouped_columns)
                return
            st.markdown("---")

            # Relatório de Novas Tarefas de Pós Vendas
            ################## START #####################
            df_entries = df_entries[followUpEntries_display_columns_initial_columns]
            df_entries = df_entries.rename(columns={
                'name': 'Consultora de Vendas',
                'follow_ups_count': 'Novos Pós-Vendas',
                'customer_ids': 'ID dos Clientes'
            })

            # Add location and shift info:
            df_entries = enrich_consultora_df(df_entries, consultoras_manha, 'Manhã')
            df_entries = enrich_consultora_df(df_entries, consultoras_tarde, 'Tarde')
            
            df_entries['Tam'] = 'P'  # Default team
            
            df_entries = df_entries.rename(columns={
                'name': 'Consultora de Vendas',
                'follow_ups_count': 'Novos Pós-Vendas',
                'customer_ids': 'ID dos Clientes'
            })
            
            df_entries = df_entries[followUpEntries_display_columns]
            df_entries_consultoras_manha = df_entries[df_entries['Consultora de Vendas'].isin(consultoras_manha.keys())]
            df_entries_consultoras_tarde = df_entries[df_entries['Consultora de Vendas'].isin(consultoras_tarde.keys())]
            
            df_entries_consultoras_manha = df_entries_consultoras_manha.sort_values(by='Novos Pós-Vendas', ascending=False)
            df_entries_consultoras_tarde = df_entries_consultoras_tarde.sort_values(by='Novos Pós-Vendas', ascending=False)
            
            df_entries_consultoras_manha_filtered = df_entries_consultoras_manha[followUpEntries_display_columns]
            df_entries_consultoras_tarde_filtered = df_entries_consultoras_tarde[followUpEntries_display_columns]
            ################## END #####################

            # Relatório de Comentários de Tarefas de Pós Vendas
            ################## START #####################
            df_comments = df_comments[followUpComments_display_columns_initial_columns]
            df_comments = df_comments.rename(columns={
                'name': 'Consultora de Vendas',
                'comments_count': 'Comentários de Pós-Vendas',
                'comments_customer_ids': 'ID dos Clientes'
            })
            
            df_comments = enrich_consultora_df(df_comments, consultoras_manha, 'Manhã')
            df_comments = enrich_consultora_df(df_comments, consultoras_tarde, 'Tarde')
            
            df_comments = df_comments[followUpComments_display_columns]

            df_comments_consultoras_manha = df_comments[df_comments['Consultora de Vendas'].isin(consultoras_manha.keys())]
            df_comments_consultoras_manha = df_comments_consultoras_manha.reset_index(drop=True)
            df_comments_consultoras_manha = df_comments_consultoras_manha.sort_values(by='Comentários de Pós-Vendas', ascending=False)            
            df_comments_consultoras_tarde = df_comments[df_comments['Consultora de Vendas'].isin(consultoras_tarde.keys())]
            df_comments_consultoras_tarde = df_comments_consultoras_tarde.reset_index(drop=True)
            df_comments_consultoras_tarde = df_comments_consultoras_tarde.sort_values(by='Comentários de Pós-Vendas', ascending=False)
            
            df_comments_consultoras_manha_filtered = df_comments_consultoras_manha[followUpComments_display_columns]
            df_comments_consultoras_tarde_filtered = df_comments_consultoras_tarde[followUpComments_display_columns]
            ################## END #####################


            # Relatório de Venda Mensal Bruta
            ################## START #####################
            if df_gross_sales.empty or 'chargableTotal' not in df_gross_sales.columns:
                # Handle gracefully (log, show warning, or skip further processing)
                print("Warning: No gross sales data or 'chargableTotal' column missing.")
                # Optionally, create the column with default value if needed
                df_gross_sales['chargableTotal'] = 0    
            else:
                df_gross_sales['chargableTotal'] = pd.to_numeric(df_gross_sales['chargableTotal'], errors='coerce').fillna(0)
                df_gross_sales['chargableTotal'] = df_gross_sales['chargableTotal'] / 100
                df_gross_sales['chargableTotal'] = df_gross_sales['chargableTotal'].astype(float)
            
                df_gross_sales = df_gross_sales.loc[df_gross_sales['statusLabel'] == 'Finalizado']

                df_gross_sales_filtered = df_gross_sales[grossSales_display_columns]
                
                df_gross_sales_grouped = df_gross_sales_filtered.groupby('createdBy').agg({'chargableTotal': 'sum', 'id': 'nunique'}).reset_index()
                df_gross_sales_grouped = df_gross_sales_grouped.rename(columns={'createdBy': 'Consultora de Vendas', 'chargableTotal': 'Valor líquido', 'id': 'Pedidos'})
                
                df_gross_sales_manha = df_gross_sales_grouped[df_gross_sales_grouped['Consultora de Vendas'].isin(consultoras_manha.keys())]
                df_gross_sales_manha = df_gross_sales_manha.sort_values(by='Valor líquido', ascending=False)
                df_gross_sales_manha['Valor líquido'] = df_gross_sales_manha['Valor líquido'].round(2)

                df_gross_sales_tarde = df_gross_sales_grouped[df_gross_sales_grouped['Consultora de Vendas'].isin(consultoras_tarde.keys())]
                df_gross_sales_tarde = df_gross_sales_tarde.sort_values(by='Valor líquido', ascending=False)
                df_gross_sales_tarde['Valor líquido'] = df_gross_sales_tarde['Valor líquido'].round(2)
            ################## END #####################

            # Merging final dfs: Manhã, Tarde and Total
            ################## START #####################
            # Manhã
            merged_followUps_manha = pd.merge(
                df_entries_consultoras_manha_filtered, 
                df_comments_consultoras_manha_filtered,
                on='Consultora de Vendas')
            merged_followUpsAndSales_manha = pd.merge(
                merged_followUps_manha,
                df_gross_sales_manha,
                on='Consultora de Vendas')

            merged_followUpsAndSales_manha = merged_followUpsAndSales_manha.drop(columns=['Unidade_y', 'Tam_y', 'Turno_y'])
            merged_followUpsAndSales_manha = merged_followUpsAndSales_manha.rename(columns={'Unidade_x': 'Unidade', 'Tam_x': 'Tam', 'Turno_x': 'Turno'})
            merged_followUpsAndSales_manha = merged_followUpsAndSales_manha.sort_values(by='Comentários de Pós-Vendas', ascending=False)
            merged_followUpsAndSales_manha = merged_followUpsAndSales_manha.drop_duplicates(subset='Consultora de Vendas', keep='first')
            
            # Tarde
            merged_followUps_tarde = pd.merge(
                df_entries_consultoras_tarde_filtered,
                df_comments_consultoras_tarde_filtered,
                on='Consultora de Vendas')
            merged_followUpsAndSales_tarde = pd.merge(
                merged_followUps_tarde,
                df_gross_sales_tarde,
                on='Consultora de Vendas')

            merged_followUpsAndSales_tarde = merged_followUpsAndSales_tarde.drop(columns=['Unidade_y', 'Tam_y', 'Turno_y'])
            merged_followUpsAndSales_tarde = merged_followUpsAndSales_tarde.rename(columns={'Unidade_x': 'Unidade', 'Tam_x': 'Tam', 'Turno_x': 'Turno'})
            merged_followUpsAndSales_tarde = merged_followUpsAndSales_tarde.sort_values(by='Comentários de Pós-Vendas', ascending=False)
            merged_followUpsAndSales_tarde = merged_followUpsAndSales_tarde.drop_duplicates(subset='Consultora de Vendas', keep='first')
            
            # Fechamento
            df_merged_followUpsAndSales_all = pd.concat(
                    [merged_followUpsAndSales_manha, merged_followUpsAndSales_tarde],
                    ignore_index=True
                )
            df_merged_followUpsAndSales_all.sort_values(by='Comentários de Pós-Vendas', ascending=False, inplace=True)
            ################## END #####################

            st.subheader("Consultoras Manhã - Total de Vendas")
            merged_followUpsAndSales_manha = append_totals_row(merged_followUpsAndSales_manha)
            st.dataframe(
                merged_followUpsAndSales_manha.style
                .apply(highlight_total_row, axis=1)
                .format({
                    'Valor líquido': '{:.2f}',
                    'Novos Pós-Vendas': '{:.0f}',
                    'Comentários de Pós-Vendas': '{:.0f}',
                    'Pedidos': '{:.0f}',
                }),
                hide_index=True,
                height=len(merged_followUpsAndSales_manha) * 40
            )
            
            st.subheader("Consultoras Tarde - Total de Vendas")
            # use append_totals_row function
            merged_followUpsAndSales_tarde = append_totals_row(merged_followUpsAndSales_tarde)
            st.dataframe(
                merged_followUpsAndSales_tarde.style
                .apply(highlight_total_row, axis=1)
                .format({
                    'Valor líquido': '{:.2f}',
                    'Novos Pós-Vendas': '{:.0f}',
                    'Comentários de Pós-Vendas': '{:.0f}',
                    'Pedidos': '{:.0f}',
                }),
                hide_index=True,
                height=len(merged_followUpsAndSales_tarde) * 40
            )

            st.subheader("Consultoras - Fechamento")
            # use append_totals_row function
            df_merged_followUpsAndSales_all = append_totals_row(df_merged_followUpsAndSales_all)
            st.dataframe(
                df_merged_followUpsAndSales_all.style
                .apply(highlight_total_row, axis=1)
                .format({
                    'Valor líquido': '{:.2f}',
                    'Novos Pós-Vendas': '{:.0f}',
                    'Comentários de Pós-Vendas': '{:.0f}',
                    'Pedidos': '{:.0f}',
                }),
                hide_index=True,
                height=len(df_merged_followUpsAndSales_all) * 40
            )

            ## Debugging: TOTAL SUM OF DF_GROSS_SALES:
            # total_sum = df_gross_sales['chargableTotal'].sum()
            # st.subheader(f"Total de Vendas: R$ {total_sum:.2f}")
            # st.dataframe(df_gross_sales, hide_index=True)