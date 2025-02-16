import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
from datetime import datetime, timedelta

# Support functions
from data.stores import stores_to_remove
from data.date_intervals import available_periods, days_map
from views.leads.lead_category import process_lead_categories
from views.marketing.checker import check_appointments_status
from helpers.cleaner import clean_telephone
from data.procedures import aesthetic_procedures_aval
from data.appointment_status import status_agendamentos_dash, status_comparecimentos_dash
from helpers.date import (transform_date_from_sales,
                         transform_date_from_leads,
                         transform_date_from_appointments)

def load_data():
    """Load and preprocess sales data."""
    sales = 'db/sales.xlsx'
    leads = 'db/leads.xlsx'
    appointments = 'db/appointments.xlsx'

    df_sales = pd.read_excel(sales)
    df_sales = df_sales.loc[~df_sales['Unidade'].isin(stores_to_remove)]
    df_sales = transform_date_from_sales(df_sales)
    
    df_leads = pd.read_excel(leads)
    df_leads = df_leads.loc[~df_leads['Unidade'].isin(stores_to_remove)]
    df_leads = transform_date_from_leads(df_leads)
    
    df_appointments = pd.read_excel(appointments)
    df_appointments = df_appointments.loc[~df_appointments['Unidade do agendamento'].isin(stores_to_remove)]
    df_appointments = transform_date_from_appointments(df_appointments)

    return df_sales, df_leads, df_appointments


def create_time_filtered_df(df, days=None):
    """Create a time-filtered dataframe."""
    if days:
        cutoff_date = datetime.now() - timedelta(days=days)
        return df[df['Dia da entrada'] >= cutoff_date]
    return df

def load_page_marketing():
    """Main function to display sales data."""

    st.title("📊 0 - Marketing ")
    
    # Original Code
    # df_sales, df_leads, df_appointments = load_data()

    #################
    # Tests are here
    #################
    df_leads, df_appointments, df_sales = None, None, None  # Initialize variables

    # df_sales = upload_sales_file()
    col1, col2, col3 = st.columns(3)
    
    # df_leads = upload_leads_file()
    with col1:
        upload_leads_file = st.file_uploader("Upload Leads File", type=["xlsx"])
        if upload_leads_file is not None:
            df_leads = pd.read_excel(upload_leads_file)
            df_leads = df_leads.loc[~df_leads['Unidade'].isin(stores_to_remove)]
            df_leads = transform_date_from_leads(df_leads)
            df_leads = process_lead_categories(df_leads)
            # df_leads = check_appointments_status(df_leads, df_appointments_comparecimentos, df_appointments_agendamentos)
    
    # df_appointments = upload_appointments_file()
    with col2:
        upload_appointments_file = st.file_uploader("Upload Appointments File", type=["xlsx"])
        if upload_appointments_file is not None:
            df_appointments = pd.read_excel(upload_appointments_file)
            df_appointments = df_appointments.loc[~df_appointments['Unidade do agendamento'].isin(stores_to_remove)]
            df_appointments = transform_date_from_appointments(df_appointments)
            
            # Clean phone numbers
            df_appointments['Telefone'] = df_appointments['Telefone'].fillna('Cliente sem telefone')
            df_appointments['Telefone'] = df_appointments['Telefone'].astype(str)
            df_appointments['Telefones Limpos'] = df_appointments['Telefone'].apply(clean_telephone)
            
            # Filter appointments for comparecimentos and agendamentos
            df_appointments_comparecimentos = df_appointments[df_appointments['Status'].isin(status_comparecimentos_dash)]
            df_appointments_comparecimentos = df_appointments_comparecimentos[df_appointments_comparecimentos['Procedimento'].isin(aesthetic_procedures_aval)]
            
            df_appointments_agendamentos = df_appointments[df_appointments['Status'].isin(status_agendamentos_dash)]
            df_appointments_agendamentos = df_appointments_agendamentos[df_appointments_agendamentos['Procedimento'].isin(aesthetic_procedures_aval)]

    with col3:
        upload_sales_file = st.file_uploader("Upload Sales File", type=["xlsx"])
        if upload_sales_file is not None:
            df_sales = pd.read_excel(upload_sales_file)
            df_sales = df_sales.loc[~df_sales['Unidade'].isin(stores_to_remove)]
            df_sales = transform_date_from_sales(df_sales)
    
    ### df_leads
    # Leads por Mês
    # No funil, apenas Qtd de Leads por coluna mês
    # ter também no df_leads fonte, unidade, mensagem, content, email, phone tratado, utm_source, utm_medium, utm_campaign

    # Check if data is available before proceeding
    if df_leads is None or df_appointments is None or df_sales is None:
        st.warning("⚠️  Faça upload dos 3 arquivos para começar a análise!")
        return  # Stop execution until data is uploaded

    try:
        ##############
        ##### df_leads
        ##############
        st.markdown("---")
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
        ###############
        
        # # Div groupby Google
        df_leads_cleaned_columns = ['ID do lead', 'Nome do lead', 'Email do lead', 'Telefone do lead',
                                    'Mensagem', 'Unidade', 'Fonte', 'Dia da entrada',
                                    'Status', 'Source', 'Medium', 'Term', 'Content', 'Campaign', 'Mês']
        df_leads_cleaned = df_leads_google_and_facebook[df_leads_cleaned_columns]

        # Cleaning lead telephone
        df_leads_cleaned['Telefone do lead'] = df_leads_cleaned['Telefone do lead'].astype(str)
        df_leads_cleaned['Telefone do lead'] = df_leads_cleaned['Telefone do lead'].apply(clean_telephone)

        df_appointments_cleaned_columns = ['ID agendamento', 'ID cliente', 'Unidade do agendamento', 'Procedimento', 'Status', 'Data', 'Dia', 'Mês', 'Dia da Semana']
        df_appointments_cleaned = df_appointments[df_appointments_cleaned_columns]

        df_sales_cleaned_columns = ['ID orçamento', 'Unidade', 'Data venda', 'Mês', 'Dia', 'Dia da Semana', 'ID cliente', 'Valor líquido', 'Procedimento', 'Data nascimento cliente', 'Profissão cliente']
        df_sales_cleaned = df_sales[df_sales_cleaned_columns]
        
        st.markdown("---")
        st.write("Leads que vamos conferir:")
        df_leads_cleaned = process_lead_categories(df_leads_cleaned)
        st.dataframe(df_leads_cleaned.sample(n=5, random_state=123))

        st.markdown("---")
        st.write("Agendamentos que vamos conferir:")
        st.dataframe(df_appointments_cleaned.sample(n=5, random_state=123))

        st.markdown("---")
        st.write("Vendas que vamos conferir:")
        st.dataframe(df_sales_cleaned.sample(n=5, random_state=123))
        
        st.markdown("---")
        st.write("""
                    Está tudo certo com os dados? \n
                    Se sim, clique no botão abaixo para a magia acontecer!
                """)
        if st.button("Play", icon="🔥"):
            # Check appointment status for all leads at once
            df_leads_cleaned = check_appointments_status(
                df_leads_cleaned,
                df_appointments_comparecimentos,  # Already filtered for 'Atendido' status and aesthetic procedures
                df_appointments_agendamentos      # Already filtered for other statuses and aesthetic procedures
            )

            st.dataframe(df_leads_cleaned)
        
        # st.markdown("---")
        # st.header("Google")
        # df_leads_google = df_leads_cleaned[df_leads_cleaned['Fonte'] == 'Google Pesquisa']
        # df_leads_google = process_lead_categories(df_leads_google)
        
        # df_leads_google_by_month = df_leads_google.groupby(['Mês']).agg({'ID do lead': 'nunique'}).reset_index()
        # df_leads_google_by_month_and_store = df_leads_google.groupby(['Mês', 'Unidade']).agg({'ID do lead': 'nunique'}).reset_index()
        # st.dataframe(df_leads_google_by_month)

        # # Div groupby Instagram
        # st.markdown("---")
        # st.header("Facebook Leads")
        # df_leads_facebook_leads = df_leads_cleaned[df_leads_cleaned['Fonte'] == 'Facebook Leads']
        # df_leads_facebook_leads = process_lead_categories(df_leads_facebook_leads)
        
        # df_leads_facebook_leads_by_month = df_leads_facebook_leads.groupby(['Mês']).agg({'ID do lead': 'nunique'}).reset_index()
        # df_leads_facebook_leads_by_month_and_store = df_leads_facebook_leads.groupby(['Mês', 'Unidade']).agg({'ID do lead': 'nunique'}).reset_index()
        # st.dataframe(df_leads_facebook_leads_by_month)

        # # Check if leads_facebook and leads_google are in

        # st.dataframe(df_leads_facebook_leads)
        # st.dataframe(df_leads_google)
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")


# Valor das vendas dentro do mês
# Ticket médio do cliente no mês
# Valor das vendas no período completo selecionado
# Ticket médio do cliente no período completo selecionado
# Quantidade orcamentos que o cliente tem