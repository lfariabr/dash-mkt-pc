import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
from datetime import datetime, timedelta
import time
import requests
import json
from typing import List, Dict, Any, Optional
import io

# Support functions
from data.stores import stores_to_remove
from data.procedures import aesthetic_procedures_aval
from data.appointment_status import status_agendamentos_marketing, status_comparecimentos_marketing
from views.appointments.appointment_columns import appointments_clean_columns
from views.sales.sale_columns import sales_clean_columns
from views.marketing.marketing_columns import marketing_clean_columns
from views.leads.lead_columns import lead_clean_columns
from views.leads.lead_category import process_lead_categories
from helpers.cleaner import clean_telephone, rename_columns_df_leads_with_purchases
from helpers.date import (transform_date_from_sales,
                         transform_date_from_leads,
                         transform_date_from_appointments)
from views.marketing.marketing_grouper import ( groupby_marketing_by_category,
                                                groupby_marketing_by_source,
                                                groupby_marketing_by_category_and_comprou,
                                                groupby_marketing_by_source_and_comprou,
                                                pivot_table_marketing_by_category_and_comprou,
                                                pivot_table_marketing_by_source_and_comprou
                                            )
from views.marketing.worker import *
from views.marketing.sales_checker import check_if_lead_has_purchased
from helpers.cleaner import columns_to_hide_from_final_df_leads_appointments_sales
import sys

import logging
logging.basicConfig(level=logging.INFO)

# Add backend to sys.path if needed
sys.path.append('/Users/luisfaria/Desktop/sEngineer/dash')

# Import database components
from backend.database import SessionLocal, engine
from backend.models.mkt_lead import MktLead

def load_data():
    """Load and preprocess sales data. test"""
    leads = 'db/leads.xlsx'
    appointments = 'db/appointments.xlsx'
    sales = 'db/sales.xlsx'
    
    df_leads = pd.read_excel(leads)
    df_leads = df_leads.loc[~df_leads['Unidade'].isin(stores_to_remove)]
    df_leads = transform_date_from_leads(df_leads)
    
    df_appointments = pd.read_excel(appointments)
    df_appointments = df_appointments.loc[~df_appointments['Unidade do agendamento'].isin(stores_to_remove)]
    df_appointments = transform_date_from_appointments(df_appointments)

    df_sales = pd.read_excel(sales)
    df_sales = df_sales.loc[~df_sales['Unidade'].isin(stores_to_remove)]
    df_sales = transform_date_from_sales(df_sales)
    
    return df_leads, df_appointments, df_sales

def create_time_filtered_df(df, days=None):
    """Create a time-filtered dataframe."""
    if days:
        cutoff_date = datetime.now() - timedelta(days=days)
        return df[df['Dia da entrada'] >= cutoff_date]
    return df

def load_page_marketing():
    """Main function to display sales data."""

    st.title("📊 0 - Marketing ")

    df_leads, df_appointments, df_sales = None, None, None  # Initialize variables
    col1, col2, col3 = st.columns(3)    

    with col1:
        upload_leads_file = st.file_uploader("Upload Leads File", type=["xlsx"])
        if upload_leads_file is not None:
            df_leads = pd.read_excel(upload_leads_file)
            df_leads = df_leads.loc[~df_leads['Unidade'].isin(stores_to_remove)]
            df_leads = transform_date_from_leads(df_leads)
            df_leads = process_lead_categories(df_leads)
    
    with col2:
        upload_appointments_file = st.file_uploader("Upload Appointments File", type=["xlsx"])
        if upload_appointments_file is not None:
            df_appointments = pd.read_excel(upload_appointments_file)
            df_appointments = clean_agd_df(df_appointments)
            df_appointments = transform_date_from_appointments(df_appointments)

            # Filter appointments for comparecimentos and agendamentos
            df_appointments_comparecimentos = df_appointments[df_appointments['Status'].isin(status_comparecimentos_marketing)]
            df_appointments_comparecimentos = df_appointments_comparecimentos[df_appointments_comparecimentos['Procedimento'].isin(aesthetic_procedures_aval)]
            
            df_appointments_agendamentos = df_appointments[df_appointments['Status'].isin(status_agendamentos_marketing)]
            df_appointments_agendamentos = df_appointments_agendamentos[df_appointments_agendamentos['Procedimento'].isin(aesthetic_procedures_aval)]

    with col3:
        upload_sales_file = st.file_uploader("Upload Sales File", type=["xlsx"])
        if upload_sales_file is not None:
            df_sales = pd.read_excel(upload_sales_file)
            df_sales = clean_sales_df(df_sales)
            df_sales = transform_date_from_sales(df_sales)
    
    if df_leads is None or df_appointments is None or df_sales is None:
        st.warning("⚠️  Faça upload dos 3 arquivos para começar a análise!")
        return

    try:
        ##############
        ##### df_leads
        
        with st.spinner("Carregando dados..."):
        
            with st.expander("✅ Dados Carregados... Clique para conferir as bases 👇"):
                st.header("Leads por Fonte")
                col1, col2, col3 = st.columns(3)
            
                with col1:
                    df_leads_google = df_leads[df_leads['Fonte'] == 'Google Pesquisa']
                    df_leads_google = process_lead_categories(df_leads_google)

                    groupby_leads_por_mes = df_leads_google.groupby(['Mês']).size().reset_index(name='ID do lead')
                    st.write("Leads Google Pesquisa")
                    st.dataframe(groupby_leads_por_mes)
                    
                with col2:
                    df_leads_facebook = df_leads[df_leads['Fonte'] == 'Facebook Leads']
                    df_leads_facebook = process_lead_categories(df_leads_facebook)

                    groupby_leads_por_mes = df_leads_facebook.groupby(['Mês']).size().reset_index(name='ID do lead')
                    st.write("Leads Facebook Leads")
                    st.dataframe(groupby_leads_por_mes)
                
                with col3:
                    df_leads_google_and_facebook = df_leads[df_leads['Fonte'].isin(['Google Pesquisa', 'Facebook Leads'])]
                    df_leads_google_and_facebook = process_lead_categories(df_leads_google_and_facebook)

                    groupby_leads_por_mes = df_leads_google_and_facebook.groupby(['Mês']).size().reset_index(name='ID do lead')
                    st.write("Leads Google e Facebook Leads")
                    st.dataframe(groupby_leads_por_mes)

                ###############
                ###### df_marketing_data
                # Cleaning data
                # df_leads_cleaned = df_leads_google_and_facebook[lead_clean_columns]
                df_leads_cleaned = df_leads[lead_clean_columns]
                df_leads_cleaned['Telefone do lead'] = df_leads_cleaned['Telefone do lead'].astype(str)
                df_leads_cleaned['Telefone do lead'] = df_leads_cleaned['Telefone do lead'].apply(clean_telephone)
                
                st.markdown("---")
                st.write("Leads que vamos conferir:")
                df_leads_cleaned = process_lead_categories(df_leads_cleaned)
                st.dataframe(df_leads_cleaned.sample(n=5, random_state=123))

                st.markdown("---")
                st.write("Agendamentos que vamos conferir:")
                st.dataframe(df_appointments.sample(n=5, random_state=123))

                st.markdown("---")
                st.write("Vendas que vamos conferir:")
                st.dataframe(df_sales.sample(n=5, random_state=123))
        
        st.markdown("---")
        st.write("""
                    Está tudo certo com os dados? \n
                    Se sim, clique no botão abaixo para cruzar os dados!!
                """)
        if st.button(
                    "Play",
                    icon="🎲", 
                    type="primary"):

            st.markdown("---")

            progress_bar = st.progress(0)
            spinner_container = st.empty()

            with spinner_container.container():
                # Simulating progress (0-60%):
                for i in range (0, 61, 20):
                    progress_bar.progress(i)
                    time.sleep(0.5)

                # First check: All leads for 'Atendido' status
                df_leads_cleaned = check_if_lead_has_atendido_status(
                    df_leads_cleaned,
                    df_appointments_comparecimentos
                )

                for i in range(60, 91, 10):
                    progress_bar.progress(i)
                    time.sleep(0.3)
                
                # Final progress update to 100%
                progress_bar.progress(100)
                time.sleep(3)
            
            # Clear spinner and display results
            spinner_container.empty()

            st.write("### Cruzamento Leads x Agenda:")
            with st.expander("🔧 Dados em processamento... Clique se quiser conferir os detalhes 👇"):

                st.write("### 1. Leads Atendidos:")
                df_atendidos = df_leads_cleaned[df_leads_cleaned['status'] == 'Atendido'].copy()

                st.dataframe(df_atendidos)

                st.write("### 2. Leads na Agenda, com outros status:")
                df_leads_nao_atendidos = df_leads_cleaned[df_leads_cleaned['procedimento'].isna()].copy()
                
                df_leads_nao_atendidos = check_if_lead_has_other_status(
                    df_leads_nao_atendidos,
                    df_appointments_agendamentos
            )    
                # Identify which rows were processed (by index)
                processed_indices = df_leads_nao_atendidos.index
                # Update just those rows in the original DataFrame
                df_leads_cleaned.loc[processed_indices] = df_leads_nao_atendidos
                df_outros_status = df_leads_cleaned[df_leads_cleaned['status'].isin(status_agendamentos_marketing)]
                
                st.dataframe(df_outros_status)
                
                # Display leads not found in any check
                st.write("### 3. Leads não encontrados na Agenda:")
                df_nao_encontrados = df_leads_cleaned[df_leads_cleaned['status'].isna()].copy()

                st.dataframe(df_nao_encontrados)

            df_leads_cleaned['status'] = df_leads_cleaned['status'].fillna('Não está na agenda')
            df_leads_cleaned_final = df_leads_cleaned[marketing_clean_columns]
            df_leads_cleaned_final = df_leads_cleaned_final.fillna('')
            
            st.markdown("---")

            df_leads_with_purchases = check_if_lead_has_purchased(
                                        df_leads_cleaned_final, 
                                        df_sales
                                    )
            
            df_leads_with_purchases['Valor líquido'] = pd.to_numeric(
                                                            df_leads_with_purchases['Valor líquido']
                                                            .astype(str).str.replace(',', '.'), 
                                                            errors='coerce'
                                                        )
            
            # PREP COOL STATISTICS:
            total_leads_compraram = df_leads_with_purchases[df_leads_with_purchases['comprou'] == True]['ID do lead'].nunique()
            total_comprado = df_leads_with_purchases[df_leads_with_purchases['comprou'] == True]['Valor líquido'].sum()
            total_leads = len(df_leads_with_purchases)

            with st.container(border=True):
                st.write("### Resumo da Análise:")
                summary_data = {
                    "Indicador": ["👀 -> Total de Leads", 
                                 "⚠️ -> Não encontrados na Agenda", 
                                 "📅 -> Agendado, Confirmado, Falta e Cancelado", 
                                 "✅ -> Atendidos", 
                                 "🎉 -> Total de leads que compraram", 
                                 "💰 -> Total comprado pelos leads"],
                    "Valor": [
                        f"{total_leads}",
                        f"{len(df_nao_encontrados)} ({(len(df_nao_encontrados)/total_leads*100):.1f}%)",
                        f"{len(df_outros_status)} ({(len(df_outros_status)/total_leads*100):.1f}%)",
                        f"{len(df_atendidos)} ({(len(df_atendidos)/total_leads*100):.1f}%)",
                        f"{total_leads_compraram} ({(total_leads_compraram/total_leads*100):.1f}%)",
                        f"R$ {total_comprado:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
                    ]
                }
                
                df_summary = pd.DataFrame(summary_data)
                st.dataframe(
                    df_summary, 
                    use_container_width=True,  
                    hide_index=True
                )

            with st.container(border=True):
                st.write("### Tabela: Leads x Agenda x Vendas") 
        
                # Special treatments before showing data
                df_leads_with_purchases = rename_columns_df_leads_with_purchases(df_leads_with_purchases)
                df_leads_with_purchases = df_leads_with_purchases.drop(columns=columns_to_hide_from_final_df_leads_appointments_sales)
                df_leads_with_purchases['intervalo da compra'] = (df_leads_with_purchases['Data Venda'] - df_leads_with_purchases['Dia da entrada']).dt.days
                df_leads_with_purchases = df_leads_with_purchases.fillna("")
                df_leads_with_purchases['ID lead'] = pd.to_numeric(df_leads_with_purchases['ID lead'], errors='coerce')
                df_leads_with_purchases['Valor primeiro orçamento'] = pd.to_numeric(df_leads_with_purchases['Valor primeiro orçamento'], errors='coerce')

                #TODO create new columns. Waiting for Lili's database
                # Problem of duplicate leads... Valor primeiro orçamento will also be duplicate... need to think about this.

                st.session_state['leads_data'] = df_leads_with_purchases
                st.dataframe(df_leads_with_purchases, hide_index=True)

                # Create a download button for the Excel file
                buffer = io.BytesIO()
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                    df_leads_with_purchases.to_excel(writer, index=False, sheet_name='Leads')
                    
                excel_data = buffer.getvalue()
                today = datetime.now().strftime("%d-%m-%Y")
                st.download_button(
                    label="Baixar Excel",
                    data=excel_data,
                    file_name=f"df_mkt_{today}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            
            with st.expander("🔍 Detalhes Dinâmica Marketing:"):
                tab1, tab2 = st.tabs(["Visão por Categoria", "Visão por Fonte"])

                with tab1:
                    st.write("Leads por Categoria e Unidade:")
                    df_leads_by_category = groupby_marketing_by_category(df_leads_with_purchases)
                    pivot_table_lead_categoria_loja = df_leads_by_category.pivot_table(index='Categoria', columns='Unidade do lead', values='ID lead')
                    pivot_table_lead_categoria_loja = pivot_table_lead_categoria_loja.fillna(0)

                    st.dataframe(pivot_table_lead_categoria_loja)

                    st.write("Leads por Categoria e Status Agenda:")
                    df_leads_by_category_and_status = df_leads_with_purchases.groupby(['Categoria', 'Status Agenda']).agg({'ID lead': 'count'})
                    pivot_table_lead_categoria_status = df_leads_by_category_and_status.pivot_table(index='Categoria', columns='Status Agenda', values='ID lead')
                    pivot_table_lead_categoria_status = pivot_table_lead_categoria_status.fillna(0)

                    st.dataframe(pivot_table_lead_categoria_status)

                    st.write("Leads por Categoria (Compradores x Soma Valor primeiro orçamento)")
                    df_leads_by_category_and_comprou = groupby_marketing_by_category_and_comprou(df_leads_with_purchases)
                    pivot_table_lead_categoria_comprou = pivot_table_marketing_by_category_and_comprou(df_leads_by_category_and_comprou)
                    
                    pivot_table_lead_categoria_comprou.columns = ['Compradores', 'Soma Valor primeiro orçamento']
                    pivot_table_lead_categoria_comprou = pivot_table_lead_categoria_comprou.fillna(0)

                    st.dataframe(pivot_table_lead_categoria_comprou)
                
                with tab2:
                    st.write("Leads por Fonte e Unidade:")
                    df_leads_by_source = groupby_marketing_by_source(df_leads_with_purchases)
                    pivot_table_lead_source_loja = df_leads_by_source.pivot_table(index='Fonte', columns='Unidade do lead', values='ID lead')
                    pivot_table_lead_source_loja = pivot_table_lead_source_loja.fillna(0)

                    st.dataframe(pivot_table_lead_source_loja)

                    st.write("Leads por Fonte e Status Agenda:")
                    df_leads_by_source_and_status = df_leads_with_purchases.groupby(['Fonte', 'Status Agenda']).agg({'ID lead': 'count'})
                    pivot_table_lead_source_status = df_leads_by_source_and_status.pivot_table(index='Fonte', columns='Status Agenda', values='ID lead')
                    pivot_table_lead_source_status = pivot_table_lead_source_status.fillna(0)

                    st.dataframe(pivot_table_lead_source_status)

                    st.write("Leads por Fonte (Compradores x Soma Valor primeiro orçamento)")
                    df_leads_by_source_and_comprou = groupby_marketing_by_source_and_comprou(df_leads_with_purchases)
                    pivot_table_lead_source_comprou = pivot_table_marketing_by_source_and_comprou(df_leads_by_source_and_comprou)
                    
                    pivot_table_lead_source_comprou.columns = ['Compradores', 'Soma Valor primeiro orçamento']
                    pivot_table_lead_source_comprou = pivot_table_lead_source_comprou.fillna(0)

                    st.dataframe(pivot_table_lead_source_comprou)

            st.write("""
                    Está tudo certo com os dados? \n
                    Se sim, clique no botão abaixo para salvar os Dados!
                """)

            st.button(
                    "Salvar os dados no banco", 
                    on_click=save_data, 
                    type="primary", 
                    icon="✅"
                )

    except Exception as e:
        st.error(f"An error occurred: {str(e)}")

# Function to save data to database
def save_data():
    """
    Save the processed leads data to the database when the 'Salvar Dados' button is clicked.
    This function acts as a bridge between the UI and the data_wrestler module.
    """
    try:
        # Check if data exists in session state
        if 'leads_data' in st.session_state and not st.session_state['leads_data'].empty:
            from helpers.data_wrestler import save_data_to_db
            
            success, message = save_data_to_db(st.session_state['leads_data'])
            
            if success:
                st.success(message)
            else:
                st.error(message)
        else:
            st.warning("Não há dados para salvar. Processe os dados primeiro.")
    except Exception as e:
        st.error(f"Erro ao salvar dados: {str(e)}")