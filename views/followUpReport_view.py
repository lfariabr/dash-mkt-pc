import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import asyncio
from apiCrm.resolvers.fetch_followUpEntriesReport import fetch_and_process_followUpEntriesReport
from apiCrm.resolvers.fetch_followUpsCommentsReport import fetch_and_process_followUpsCommentsReport
from apiCrm.resolvers.fetch_grossSalesReport import fetch_and_process_grossSales_report
from components.date_input import date_input

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
        with st.spinner("Carregando dados..."):

            df_entries, df_comments, df_gross_sales = load_data(start_date, end_date)
            
            st.markdown("---")
            st.subheader("Novos Pós-Vendas")

            entries_columns = ['name', 'follow_ups_count', 'customer_ids']
            df_entries = df_entries[entries_columns]
            df_entries = df_entries.rename(columns={
                'name': 'Consultora de Vendas',
                'follow_ups_count': 'Novos Pós-Vendas',
                'customer_ids': 'ID dos Clientes'
            })

            consultoras_manha = {
                'Bruna Nascimento Rangel' : 'Copacabana',
                'Anna Flavia Medeiros Paiva' : 'Tijuca',
                'Barbara Sumire Ireijo Bravo' : 'Santos',
                'Carla Tais de Souza Lamego' : 'Jardins',
                'Gabriela Rodrigues Evaristo Barbosa' : 'Osasco',
                'Joyce Bearari da Silva' : 'Campinas',
                'Claudia Vitoria Xavier da Silva' : 'Moema',
                'Laryssa Benicio Santos' : 'Londrina',
                'Julia Bigliazzi Amorim Nogueira' : 'Ipiranga',
                'Diana da Silva Sousa' : 'Jardins',
                'Beatriz Emanoela da Silva' : 'Tatuapé',
                'Larissa Teixeira da Rocha' : 'Tucuruvi',
                'Leticia Araujo Dos Santos' : 'Lapa'
            }

            consultoras_tarde = {
                'Ana Carolina da Silva Coloma' : 'Copacabana',
                'Caroline aparecida dos Santos Costa' : 'Lapa',
                'Ingrid Porciuncula Ferreira da Silva' : 'Jardins',
                'Larissa Gabriely Fonseca de Lima' : 'Campinas',
                'Vitoria Lins Ferreira' : 'Mooca',
                'Lorrana Assis Santana de Souza' : 'Tatuapé',
                'Pamela Sabrina do Nascimento Lima' : 'Vila Mascote',
                'Ethel Castro Flexa Ribeiro Bastos' : 'Tijuca',
                'Isadora Cristina Harder de Almeida' : 'Sorocaba',
                'Vanessa Trajano Lopes' : 'Santos',
                'Luana Rodrigues Parrillo' : 'Santos',
                'Jessica Oliveira de Azevedo' : 'Santo Amaro',
                'Eskarlete Kloh Matos' : 'Osasco',
                'Lara Goncalo Aparicio' : 'Jardins',
                'Alessandra Araújo de Oliveira' : 'Tatuapé',
                'Yasmin Emanuele Dal-Bó Teixeira' : 'Londrina',
                'Jacqueline Santos da Silva' : 'Guarulhos',
                'Sabrina Caroline de Jesus Ferreira' : 'Moema',
                'Kathelyn Matoso Pinheiro dos Santos' : 'Santo Amaro',
                'Thais Silva Lima' : 'Itaim Bibi',
                'Lawany Fernanda dos Santos' : 'São Bernardo',
                'Beatriz Abrantes Duarte' : 'Ipiranga'
            }

            # Add location and shift info:
            for consultora, local in consultoras_manha.items():
                mask = df_entries['Consultora de Vendas'] == consultora
                if mask.any():
                    df_entries.loc[mask, 'Unidade'] = local
                    df_entries.loc[mask, 'Turno'] = 'Manhã'
            
            for consultora, local in consultoras_tarde.items():
                mask = df_entries['Consultora de Vendas'] == consultora
                if mask.any():
                    df_entries.loc[mask, 'Unidade'] = local
                    df_entries.loc[mask, 'Turno'] = 'Tarde'
            
            df_entries['Tam'] = 'P'  # Default team
            
            # Rename columns
            df_entries = df_entries.rename(columns={
                'name': 'Consultora de Vendas',
                'follow_ups_count': 'Novos Pós-Vendas',
                'customer_ids': 'ID dos Clientes'
            })
            
            # Define display columns order
            display_columns = ['Consultora de Vendas', 'Unidade', 'Turno', 'Tam',
                                'Novos Pós-Vendas'] #, 'ID dos Clientes']
            
            # Display dataframes
            df_entries = df_entries[display_columns]

            # Filter to show only consultoras from the lists
            df_entries_consultoras_manha = df_entries[df_entries['Consultora de Vendas'].isin(consultoras_manha.keys())]
            df_entries_consultoras_tarde = df_entries[df_entries['Consultora de Vendas'].isin(consultoras_tarde.keys())]
            
            # Sort by Consultora de Vendas
            df_entries_consultoras_manha = df_entries_consultoras_manha.sort_values(by='Novos Pós-Vendas', ascending=False)
            df_entries_consultoras_tarde = df_entries_consultoras_tarde.sort_values(by='Novos Pós-Vendas', ascending=False)
            
            # Display dataframes
            st.subheader("Consultoras do Turno da Manhã")
            st.dataframe(df_entries_consultoras_manha[display_columns], hide_index=True)
            
            st.subheader("Consultoras do Turno da Tarde")
            st.dataframe(df_entries_consultoras_tarde[display_columns], hide_index=True)
            
            st.markdown("---")
            st.subheader("Comentários de Pós-Vendas")

            comments_columns = ['name', 'comments_count', 'comments_customer_ids']
            df_comments = df_comments[comments_columns]
            df_comments = df_comments.rename(columns={
                'name': 'Consultora de Vendas',
                'comments_count': 'Comentários de Pós-Vendas',
                'comments_customer_ids': 'ID dos Clientes'
            })
            
            # Add location and shift info:
            for consultora, local in consultoras_manha.items():
                mask = df_comments['Consultora de Vendas'] == consultora
                if mask.any():
                    df_comments.loc[mask, 'Unidade'] = local
                    df_comments.loc[mask, 'Turno'] = 'Manhã'
            
            for consultora, local in consultoras_tarde.items():
                mask = df_comments['Consultora de Vendas'] == consultora
                if mask.any():
                    df_comments.loc[mask, 'Unidade'] = local
                    df_comments.loc[mask, 'Turno'] = 'Tarde'
            
            df_comments['Tam'] = 'P'  # Default team
            
            # Define display columns order
            display_columns = ['Consultora de Vendas', 'Unidade', 'Turno', 'Tam',
                                'Comentários de Pós-Vendas'] #, 'ID dos Clientes']
            
            # Display dataframes
            df_comments = df_comments[display_columns]

            # Filter to show only consultoras from the lists
            df_comments_consultoras_manha = df_comments[df_comments['Consultora de Vendas'].isin(consultoras_manha.keys())]
            df_comments_consultoras_tarde = df_comments[df_comments['Consultora de Vendas'].isin(consultoras_tarde.keys())]
            
            # Reset index
            df_comments_consultoras_manha = df_comments_consultoras_manha.reset_index(drop=True)
            df_comments_consultoras_tarde = df_comments_consultoras_tarde.reset_index(drop=True)
            
            # Sort by Consultora de Vendas
            df_comments_consultoras_manha = df_comments_consultoras_manha.sort_values(by='Comentários de Pós-Vendas', ascending=False)
            df_comments_consultoras_tarde = df_comments_consultoras_tarde.sort_values(by='Comentários de Pós-Vendas', ascending=False)
            
            # Display dataframes
            st.subheader("Consultoras do Turno da Manhã")
            st.dataframe(df_comments_consultoras_manha[display_columns], hide_index=True)
            
            st.subheader("Consultoras do Turno da Tarde")
            st.dataframe(df_comments_consultoras_tarde[display_columns], hide_index=True)

            # Special treatments
            sales_display_columns = ['createdBy', 'chargableTotal', 'statusLabel']
            df_gross_sales['chargableTotal'] = pd.to_numeric(df_gross_sales['chargableTotal'], errors='coerce').fillna(0)
            df_gross_sales['chargableTotal'] = df_gross_sales['chargableTotal'] / 100
            df_gross_sales['chargableTotal'] = df_gross_sales['chargableTotal'].astype(float)
            df_gross_sales_filtered = df_gross_sales[sales_display_columns]
            
            # Groupbying Sales per Consultora
            df_gross_sales_grouped = df_gross_sales_filtered.groupby('createdBy').agg({'chargableTotal': 'sum'}).reset_index()
            df_gross_sales_grouped = df_gross_sales_grouped.rename(columns={'createdBy': 'Consultora de Vendas', 'chargableTotal': 'Valor líquido'})
            
            # Filter to show only consultoras from the lists
            df_gross_sales_manha = df_gross_sales_grouped[df_gross_sales_grouped['Consultora de Vendas'].isin(consultoras_manha.keys())]
            df_gross_sales_tarde = df_gross_sales_grouped[df_gross_sales_grouped['Consultora de Vendas'].isin(consultoras_tarde.keys())]
            
            # Sort by Consultora de Vendas
            df_gross_sales_manha = df_gross_sales_manha.sort_values(by='Valor líquido', ascending=False)
            df_gross_sales_tarde = df_gross_sales_tarde.sort_values(by='Valor líquido', ascending=False)
            
            # Display dataframes
            st.subheader("Consultoras do Turno da Manhã - Total de Vendas")
            st.dataframe(df_gross_sales_manha, hide_index=True)
            
            st.subheader("Consultoras do Turno da Tarde - Total de Vendas")
            st.dataframe(df_gross_sales_tarde, hide_index=True)

            # TOTAL SUM OF DF_GROSS_SALES:
            total_sum = df_gross_sales['Valor líquido'].sum()
            st.subheader(f"Total de Vendas: R$ {total_sum:.2f}")

            # TODO Merge total sum with total sum of df_entries + df_comments
            