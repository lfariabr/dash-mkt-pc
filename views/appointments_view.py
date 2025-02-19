import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
from datetime import datetime, timedelta
from data.sources import paid_sources, organic_sources
from data.stores import stores_to_remove
from data.date_intervals import days_map, available_periods
from components.headers import header_appointments
from helpers.date import transform_date_from_appointments 

def load_data():
    """Load and preprocess leads data."""
    appointments = 'db/appointments.xlsx' #TODO

    df = pd.read_excel(appointments)
    df = df.loc[~df['Unidade do agendamento'].isin(stores_to_remove)]
    df = transform_date_from_appointments(df)
    
    return df

def create_time_filtered_df(df, days=None):
    """Create a time-filtered dataframe."""
    if days:
        cutoff_date = datetime.now() - timedelta(days=days)
        return df[df['Data'] >= cutoff_date]
    return df

def load_page_appointments():
    """Main function to display leads data."""
    

    st.title("📊 11 - Agendamentos")
    df_appointments = load_data()
    
    st.sidebar.header("Filtros")
    time_filter = st.sidebar.selectbox(
        "Período", available_periods
    )
    if time_filter != "Todos os dados":
        df_appointments = create_time_filtered_df(df_appointments, days_map[time_filter])
    
    unidades = ["Todas"] + sorted(df_appointments['Unidade do agendamento'].unique().tolist())
    selected_store = st.sidebar.selectbox("Unidade", unidades)
    
    if selected_store != "Todas":
        df_appointments = df_appointments[df_appointments['Unidade do agendamento'] == selected_store]
    
    ########
    # Header
    header_appointments(df_appointments)
    
    #######
    # Div 1 Análise Detalhada: Leads por Dia do Mês e Leads por Unidade
    col1, col2 = st.columns(2)
    comparecimento_status = ['Atendido']
    comparecimento_procedimento = ['AVALIAÇÃO ESTÉTICA',
                                    'AVALIAÇÃO INJETÁVEIS E INVASIVOS']
    
    df_appointments_comparecimentos = df_appointments[
                                        (df_appointments['Status'].isin(comparecimento_status)) 
                                        & (df_appointments['Procedimento'].isin(comparecimento_procedimento))]

    
    with col1:
        st.dataframe(df_appointments_comparecimentos.sample(n=5, random_state=123))
    with col2:
        st.dataframe(df_appointments_comparecimentos.sample(n=5, random_state=123))   