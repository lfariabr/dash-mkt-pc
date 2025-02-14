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

    st.title("📊 11 - Marketing ")
    df_sales, df_leads, df_appointments = load_data()

    st.sidebar.header("Filtros")
    time_filter = st.sidebar.selectbox(
        "Período", available_periods
    )
    if time_filter != "Todos os dados":
        df_sales = create_time_filtered_df(df_sales, days_map[time_filter])
    
    unidades = ["Todas"] + sorted(df_sales['Unidade'].unique().tolist())
    selected_store = st.sidebar.selectbox("Unidade", unidades)
    
    if selected_store != "Todas":
        df_sales = df_sales[df_sales['Unidade'] == selected_store]
    
    ########
    # Header
    # header_sales(df_sales)

    # Tratativas especiais:
    # df_sales = df_sales.loc[df_sales['Status'] == 'Finalizado']
    # df_sales = df_sales.loc[df_sales['Consultor'] != 'BKO VENDAS']

    #####
    # Funil de Vendas:
    # Em uma única tabela, queremos ter uma coluna para cada Mês do período disponível da base de Leads
    
    ### df_leads
    # Leads por Mês
    # No funil, apenas Qtd de Leads por coluna mês
    # ter também no df_leads fonte, unidade, mensagem, content, email, phone tratado, utm_source, utm_medium, utm_campaign

    ##############
    ##### df_leads
    ##############
    st.markdown("---")
    st.header("Leads")
    col1, col2 = st.columns(2)
    with col1:

        groupby_leads_por_mes = df_leads.groupby(['Mês', 'Unidade']).size().reset_index(name='ID do lead')
        st.dataframe(groupby_leads_por_mes)
        
    with col2:
        fig_month = px.line(
                groupby_leads_por_mes,
                x='Mês',
                y='ID do lead',
                title='Leads por Mês',
                labels={'ID do lead': 'Quantidade de Leads', 'Mês': 'Mês'},
                markers=True
            )
        st.plotly_chart(fig_month, use_container_width=True)

    ###############
    ###### df_appointments
    ###############
    st.markdown("---")
    st.header("Agendamentos")
    col3, col4 = st.columns(2)

    with col3:
        

        # df_appointments
        # Agendamentos por mês
        # No final, apenas Contagem Unique ID agendamento com os
        
        df_appointments_agendamentos = df_appointments[df_appointments['Status'].isin(status_agendamentos_dash)]
        df_appointments_agendamentos = df_appointments_agendamentos[df_appointments_agendamentos['Procedimento'].isin(aesthetic_procedures_aval)]

        groupby_appointments_por_mes = df_appointments_agendamentos.groupby(['Mês', 'Unidade do agendamento']).agg({'ID agendamento': 'nunique'}).reset_index()
        st.dataframe(groupby_appointments_por_mes)
    
    with col4:
        fig_month = px.line(
                groupby_appointments_por_mes,
                x='Mês',
                y='ID agendamento',
                title='Agendamentos por Mês',
                labels={'ID agendamento': 'Quantidade de Agendamentos', 'Mês': 'Mês'},
                markers=True
            )
        st.plotly_chart(fig_month, use_container_width=True)

    st.markdown("---")
    st.header("Comparecimentos")
    col5, col6 = st.columns(2)
    
    with col5:
        df_appointments_comparecimentos = df_appointments[df_appointments['Status'].isin(status_comparecimentos_dash)]
        df_appointments_comparecimentos = df_appointments_comparecimentos[df_appointments_comparecimentos['Procedimento'].isin(aesthetic_procedures_aval)]

        groupby_appointments_por_mes = df_appointments_comparecimentos.groupby(['Mês', 'Unidade do agendamento']).agg({'ID agendamento': 'nunique'}).reset_index()
        st.dataframe(groupby_appointments_por_mes)
    
    with col6:
        fig_month = px.line(
                groupby_appointments_por_mes,
                x='Mês',
                y='ID agendamento',
                title='Comparecimentos por Mês',
                labels={'ID agendamento': 'Quantidade de Comparecimentos', 'Mês': 'Mês'},
                markers=True
            )
        st.plotly_chart(fig_month, use_container_width=True)

    ###############
    ###### df_sales
    ###############
    st.markdown("---")
    st.header("Pedidos/Vendas")
    df_sales = df_sales.loc[df_sales['Status'] == 'Finalizado']
    df_sales = df_sales.loc[df_sales['Consultor'] != 'BKO VENDAS']
    df_sales = df_sales.loc[df_sales['Valor líquido'] > 0]
    
    col7, col8 = st.columns(2)
    
    with col7:
        groupby_orcamentos_por_mes = df_sales.groupby(['Mês', 'Unidade']).agg({'ID orçamento': 'nunique', 'Valor líquido': 'sum'}).reset_index()
        st.dataframe(groupby_orcamentos_por_mes)
    
    with col8:
        fig_month = px.line(
                groupby_orcamentos_por_mes,
                x='Mês',
                y=['ID orçamento', 'Valor líquido'],
                title='Orcamentos por Mês',
                labels={'ID orçamento': 'Quantidade de Orcamentos', 'Valor líquido': 'Valor Liquido', 'Mês': 'Mês'},
                markers=True
            )
        st.plotly_chart(fig_month, use_container_width=True)

    ###############
    ###### df_marketing_data
    ###############
    
    # Div groupby Google
    ### Dados Iniciais
    df_leads_cleaned_columns = ['ID do lead', 'Nome do lead', 'Email do lead', 'Telefone do lead', 
                                'Mensagem', 'Unidade', 'Fonte', 'Dia da entrada',
                                'Status', 'Source', 'Medium', 'Term', 'Content', 'Campaign', 'Mês']
    
    df_leads_cleaned = df_leads[df_leads_cleaned_columns]

    st.write("motherfucking dataframe")

    # Adding Category function to dataframes
    df_leads_cleaned = process_lead_categories(df_leads_cleaned)

    df_appointments['Telefones Limpos'] = df_appointments['Telefone'].apply(clean_telephone)

    # Check appointment status for all leads at once
    df_leads_cleaned = check_appointments_status(
        df_leads_cleaned,
        df_appointments_comparecimentos,  # Already filtered for 'Atendido' status and aesthetic procedures
        df_appointments_agendamentos      # Already filtered for other statuses and aesthetic procedures
    )

    st.dataframe(df_leads_cleaned)
    
    st.markdown("---")
    st.header("Google")
    df_leads_google = df_leads_cleaned[df_leads_cleaned['Fonte'] == 'Google Pesquisa']
    df_leads_google = process_lead_categories(df_leads_google)
    
    df_leads_google_by_month = df_leads_google.groupby(['Mês']).agg({'ID do lead': 'nunique'}).reset_index()
    df_leads_google_by_month_and_store = df_leads_google.groupby(['Mês', 'Unidade']).agg({'ID do lead': 'nunique'}).reset_index()
    st.dataframe(df_leads_google_by_month)

    # Div groupby Instagram
    st.markdown("---")
    st.header("Facebook Leads")
    df_leads_facebook_leads = df_leads_cleaned[df_leads_cleaned['Fonte'] == 'Facebook Leads']
    df_leads_facebook_leads = process_lead_categories(df_leads_facebook_leads)
    
    df_leads_facebook_leads_by_month = df_leads_facebook_leads.groupby(['Mês']).agg({'ID do lead': 'nunique'}).reset_index()
    df_leads_facebook_leads_by_month_and_store = df_leads_facebook_leads.groupby(['Mês', 'Unidade']).agg({'ID do lead': 'nunique'}).reset_index()
    st.dataframe(df_leads_facebook_leads_by_month)

    # Check if leads_facebook and leads_google are in

    st.dataframe(df_leads_facebook_leads)
    st.dataframe(df_leads_google)



# Valor das vendas dentro do mês
# Ticket médio do cliente no mês
# Valor das vendas no período completo selecionado
# Ticket médio do cliente no período completo selecionado
# Quantidade orcamentos que o cliente tem