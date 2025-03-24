import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
from datetime import datetime, timedelta

# Support functions
from data.stores import stores_to_remove
from data.procedures import aesthetic_procedures_aval
from data.appointment_status import status_agendamentos_marketing, status_comparecimentos_marketing
from views.appointments.appointment_columns import appointments_clean_columns
from views.sales.sale_columns import sales_clean_columns
from views.marketing.marketing_columns import marketing_clean_columns
from views.leads.lead_columns import lead_clean_columns
from views.leads.lead_category import process_lead_categories
from helpers.cleaner import clean_telephone
from helpers.date import (transform_date_from_sales,
                         transform_date_from_leads,
                         transform_date_from_appointments)
from views.marketing.worker import *
from views.marketing.sales_checker import check_if_lead_has_purchased

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
        return  # Stop execution until data is uploaded

    try:
        ##############
        ##### df_leads
        
        with st.spinner("Carregando dados..."):
        
            with st.expander("Dados Carregados... Clique para expandir 👇"):
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
                df_leads_cleaned = df_leads_google_and_facebook[lead_clean_columns]
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
                    Se sim, clique no botão abaixo para a magia acontecer!
                """)
        if st.button("Play", icon="🔥"):
            progress_bar = st.progress(0)
            st.spinner("A magia acontece...")

            # First check: All leads for 'Atendido' status
            df_leads_cleaned = check_if_lead_has_atendido_status(
                df_leads_cleaned,
                df_appointments_comparecimentos
            )

            st.write("### Cruzamento Leads x Agenda:")
            with st.expander("Dados Carregados... Clique para expandir 👇"):

                # Display leads with 'Atendido' status
                ######################################
                st.write("### 1. Leads Atendidos:")
                
                df_atendidos = df_leads_cleaned[df_leads_cleaned['status'] == 'Atendido'].copy()
                st.dataframe(df_atendidos)

                # Display leads with other statuses
                ##########################################
                st.write("### 2. Leads na Agenda, com outros status:")
                
                df_leads_nao_atendidos = df_leads_cleaned[df_leads_cleaned['procedimento'].isna()].copy()
                
                df_leads_nao_atendidos = check_if_lead_has_other_status(
                    df_leads_nao_atendidos,
                    df_appointments_agendamentos
            )    
                # First, update the non-attended leads in the original DataFrame
                df_leads_cleaned = pd.concat([df_leads_cleaned, df_leads_nao_atendidos])

                df_outros_status = df_leads_cleaned[df_leads_cleaned['status'].isin(status_agendamentos_marketing)]
                
                st.dataframe(df_outros_status)
                
                # Display leads not found in any check
                ######################################
                st.write("### 3. Leads não encontrados na Agenda:")

                df_nao_encontrados = df_leads_cleaned[df_leads_cleaned['status'].isna()].copy()
                st.dataframe(df_nao_encontrados)
            

            # with st.container(border=True):
            #     col1, col2 = st.columns(2)
            #     total_leads = len(df_leads_cleaned)
                
            #     with col1:
            #         st.write("\n### Resumo da Análise - Leads x Agenda:")
            #         st.write(
            #             f"👀 | {total_leads} -> Total de Leads")
            #         st.write(
            #             f"✅ | {len(df_atendidos)} ({(len(df_atendidos)/total_leads*100):.1f}%) -> Atendidos")
               
            #     with col2:
            #         st.write("\n###")
            #         st.write(
            #             f"📅 | {len(df_outros_status)} ({(len(df_outros_status)/total_leads*100):.1f}%) -> Agendado, Confirmado, Falta e Cancelado")
            #         st.write(
            #             f"⚠️ | {len(df_nao_encontrados)} ({(len(df_nao_encontrados)/total_leads*100):.1f}%) ->Não foram encontrados na Agenda")
                
            # st.write("### Debug: Leads x Agenda Final:")

            # Fill column "Status" NA with "Não está na agenda"
            df_leads_cleaned['status'] = df_leads_cleaned['status'].fillna('Não está na agenda')
            df_leads_cleaned_final = df_leads_cleaned[marketing_clean_columns]
            df_leads_cleaned_final = df_leads_cleaned_final.fillna('')
            # st.dataframe(df_leads_cleaned_final)
            
            st.markdown("---")

            # TODO here onwards...

            df_leads_with_purchases = check_if_lead_has_purchased(
                                        df_leads_cleaned_final, 
                                        df_sales
                                    )
            
            df_leads_with_purchases['Valor líquido'] = pd.to_numeric(
                                                            df_leads_with_purchases['Valor líquido']
                                                            .astype(str).str.replace(',', '.'), 
                                                            errors='coerce'
                                                        )
            # st.dataframe(df_leads_with_purchases)
            
            # COOL STATISTICS:
            total_leads_compraram = df_leads_with_purchases[df_leads_with_purchases['comprou'] == True]['ID do lead'].nunique()
            total_comprado = df_leads_with_purchases[df_leads_with_purchases['comprou'] == True]['Valor líquido'].sum()
            total_leads = len(df_leads_cleaned)

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
                
                # Create the DataFrame and display it as a table
                df_summary = pd.DataFrame(summary_data)
                st.dataframe(
                    df_summary, 
                    use_container_width=True,  
                    hide_index=True
                )

            st.write("### Tabela - Leads x Agenda x Vendas") 

            # Adjusting column names
            df_leads_with_purchases.columns = [
                                                "ID do lead", "Email do lead", "Telefone do lead", 
                                                "Mensagem", "Unidade do lead", "Fonte", "Dia da entrada", 
                                                "Source", "Medium", "Term", "Content", "Campaign", 
                                                "Mês do lead", "Categoria", 

                                                # Appointment
                                                "data_agenda", "procedimento", "status", "unidade na agenda", 
                                                
                                                # sales
                                                "Telefones Limpos", "Telefone(s) do cliente", "ID orçamento",
                                                "Data venda", "Unidade da venda", "Valor primeiro orçamento", 
                                                "Total comprado pelo cliente", "Número de orçamentos do cliente",
                                                "Dia", "Mês da venda", "Dia da Semana", "comprou"]

            df_leads_with_purchases['intervalo da compra'] = (df_leads_with_purchases['Data venda'] - df_leads_with_purchases['Dia da entrada']).dt.days
            st.dataframe(df_leads_with_purchases)

            st.write("""
                    Está tudo certo com os dados? \n
                    Se sim, clique no botão abaixo para salvar os Dados!
                """)
            if st.button("Salvar no Banco de Dados", icon="💾"): 
                    st.balloons()

    except Exception as e:
        st.error(f"An error occurred: {str(e)}")