import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
from datetime import datetime, timedelta
import time
import requests
import json
from typing import List, Dict, Any, Optional

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

            # Fill column "Status" NA with "Não está na agenda"
            df_leads_cleaned['status'] = df_leads_cleaned['status'].fillna('Não está na agenda')
            df_leads_cleaned_final = df_leads_cleaned[marketing_clean_columns]
            df_leads_cleaned_final = df_leads_cleaned_final.fillna('')
            # st.dataframe(df_leads_cleaned_final)
            
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
            with st.container(border=True):
                st.write("### Tabela: Leads x Agenda x Vendas") 
                # Adjusting column names
                df_leads_with_purchases.columns = [
                                                    "ID lead", "Email do lead", "Telefone do lead", 
                                                    "Mensagem", "Unidade do lead", "Fonte", "Dia da entrada", 
                                                    "Source", "Medium", "Term", "Content", "Campaign", 
                                                    "Mês do lead", "Categoria", 

                                                    # Appointment
                                                    "Data Na Agenda", "Procedimento", "Status Agenda", "Unidade da Agenda", 
                                                    
                                                    # sales
                                                    "Telefones Limpos", "Telefone(s) do cliente", "ID orçamento",
                                                    "Data Venda", "Unidade da Venda", "Valor primeiro orçamento", 
                                                    "Total comprado pelo cliente", "Número de orçamentos do cliente",
                                                    "Dia", "Mês da Venda", "Dia da Semana", "comprou"]
                
                df_leads_with_purchases = df_leads_with_purchases.drop(columns=columns_to_hide_from_final_df_leads_appointments_sales)
                df_leads_with_purchases['intervalo da compra'] = (df_leads_with_purchases['Data Venda'] - df_leads_with_purchases['Dia da entrada']).dt.days
                df_leads_with_purchases = df_leads_with_purchases.fillna("")

                st.dataframe(df_leads_with_purchases)

            st.write("""
                    Está tudo certo com os dados? \n
                    Se sim, clique no botão abaixo para salvar os Dados!
                """)

                # TODO: re-think this process.. maybe it is better if we just
                # create a new file for the DB connection and then with the click, save it.
            
            # Initialize the session state variables if they don't exist
            if 'save_button_clicked' not in st.session_state:
                st.session_state.save_button_clicked = False
            if 'processing_complete' not in st.session_state:
                st.session_state.processing_complete = False
            if 'processing_error' not in st.session_state:
                st.session_state.processing_error = None
            if 'api_connected' not in st.session_state:
                st.session_state.api_connected = False
                
            # Add a reset button to clear the session state
            if st.button("Resetar", key="reset_button"):
                st.session_state.save_button_clicked = False
                st.session_state.processing_complete = False
                st.session_state.processing_error = None
                st.session_state.api_connected = False
                logging.info("Estado resetado pelo usuário")
                st.experimental_rerun()
            
            # Button to trigger saving data to database
            if st.button("Salvar no Banco de Dados", icon="💾", disabled=st.session_state.save_button_clicked):
                logging.info("Botão 'Salvar no Banco de Dados' clicado")
                st.session_state.save_button_clicked = True
                st.experimental_rerun()
            
            # Display status messages based on session state
            if st.session_state.processing_complete:
                st.success("✅ Dados salvos com sucesso!")
            
            if st.session_state.processing_error:
                st.error(f"❌ Erro ao salvar os dados: {st.session_state.processing_error}")
                logging.error(f"Erro ao salvar os dados: {st.session_state.processing_error}")
            
            # Only process if button clicked and not already processed
            if st.session_state.save_button_clicked and not st.session_state.processing_complete:
                # Debug message to confirm button click
                st.write("Botão clicado - iniciando processamento...")
                logging.info("Iniciando processamento após clique no botão")
                
                # Create progress bar for data saving
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                try:
                    # Debug the request we're about to make
                    st.write("Conectando ao banco de dados diretamente...")
                    logging.info("Conectando ao banco de dados diretamente...")
                    
                    # Check if the database is accessible
                    try:
                        # Verify database connection
                        inspector = inspect(engine)
                        tables = inspector.get_table_names()
                        st.write(f"Conexão com banco de dados bem sucedida. Tabelas disponíveis: {tables}")
                        logging.info(f"Conexão com banco de dados bem sucedida. Tabelas disponíveis: {tables}")
                        
                        if "mkt_leads" not in tables:
                            error_msg = "Tabela 'mkt_leads' não encontrada no banco de dados"
                            st.error(error_msg)
                            st.error("Certifique-se de que as migrações foram executadas corretamente")
                            st.session_state.processing_error = error_msg
                            st.session_state.processing_complete = True  # Mark as complete even with error
                            logging.error(error_msg)
                            return
                        
                        st.session_state.api_connected = True
                        
                    except Exception as e:
                        error_msg = f"Não foi possível conectar ao banco de dados: {str(e)}"
                        st.error("⚠️ " + error_msg)
                        st.error("Certifique-se de que as configurações do banco de dados estão corretas")
                        st.session_state.processing_error = error_msg
                        st.session_state.processing_complete = True  # Mark as complete even with error
                        logging.error(f"Erro de conexão com banco de dados: {error_msg}")
                        return
                    
                    # Debug: Check if DataFrame exists and has data
                    st.write(f"DataFrame tem {len(df_leads_with_purchases)} registros para processar")
                    logging.info(f"DataFrame tem {len(df_leads_with_purchases)} registros para processar")
                    
                    if len(df_leads_with_purchases) > 0:
                        # Show sample of first row (as dict) for debugging
                        sample_row = df_leads_with_purchases.iloc[0].to_dict()
                        st.write("Amostra de dados (primeira linha):", sample_row)
                    
                    # Process each row in the dataframe to save to the database
                    total_rows = len(df_leads_with_purchases)
                    saved_count = 0
                    errors = []
                    
                    status_text.text(f"Processando {total_rows} registros...")
                    logging.info(f"Iniciando processamento de {total_rows} registros...")
                    
                    # Create a database session
                    db = SessionLocal()
                    
                    try:
                        for index, row in df_leads_with_purchases.iterrows():
                            try:
                                from helpers.data import mkt_lead_data
                                
                                # Remove None values to avoid validation errors
                                mkt_lead_data = {k: v for k, v in mkt_lead_data.items() if v is not None}
                                
                                # Debug: Show first row data being sent
                                if index == 0:
                                    st.write("Dados que serão inseridos no banco (primeira linha):", mkt_lead_data)
                                
                                # Create a new MktLead instance
                                st.write(f"Inserindo registro {index+1} no banco de dados...")
                                
                                # Check if record with this lead_id already exists
                                existing_lead = db.query(MktLead).filter(MktLead.lead_id == mkt_lead_data.get("lead_id")).first()
                                
                                if existing_lead:
                                    # Update existing record
                                    for key, value in mkt_lead_data.items():
                                        setattr(existing_lead, key, value)
                                    db.commit()
                                    st.write(f"Registro {index+1} atualizado com sucesso.")
                                else:
                                    # Create new record
                                    mkt_lead = MktLead(**mkt_lead_data)
                                    db.add(mkt_lead)
                                    db.commit()
                                    st.write(f"Registro {index+1} inserido com sucesso.")
                                
                                saved_count += 1
                                
                            except SQLAlchemyError as e:
                                db.rollback()  # Rollback on error
                                error_msg = f"Erro de banco de dados ao salvar registro {index+1}: {str(e)}"
                                errors.append(error_msg)
                                st.error(error_msg)  # Show error immediately
                                logging.error(error_msg)
                                
                            except Exception as e:
                                db.rollback()  # Rollback on error
                                error_msg = f"Erro no processamento do registro {index+1}: {str(e)}"
                                errors.append(error_msg)
                                st.error(error_msg)  # Show error immediately
                                logging.error(error_msg)
                            
                            # Update progress
                            progress = int((index + 1) / total_rows * 100)
                            progress_bar.progress(progress)
                            status_text.text(f"Processados {index+1} de {total_rows} registros...")
                    
                    finally:
                        # Always close the db session
                        db.close()
                    
                    # Show results
                    if saved_count == total_rows:
                        st.success(f"✅ Todos os {total_rows} registros foram salvos com sucesso!")
                        st.balloons()
                    else:
                        st.warning(f"⚠️ {saved_count} de {total_rows} registros foram salvos. {total_rows - saved_count} registros apresentaram erros.")
                        if errors:
                            with st.expander("Ver detalhes dos erros"):
                                for error in errors:
                                    st.error(error)
                    
                    # Set processing complete status regardless of outcome
                    st.session_state.processing_complete = True
                    logging.info("Processamento concluído")
                
                except Exception as e:
                    st.error(f"❌ Erro durante o processo de salvamento: {str(e)}")
                    st.write("Verifique se o servidor da API está em execução e tente novamente.")
                    import traceback
                    st.code(traceback.format_exc(), language="python")  # Show full traceback
                    st.session_state.processing_error = str(e)
                    st.session_state.processing_complete = True  # Mark as complete even with error
                    logging.error(f"Erro durante o processo de salvamento: {str(e)}")

    except Exception as e:
        st.error(f"An error occurred: {str(e)}")