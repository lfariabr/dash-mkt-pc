import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
from datetime import datetime, timedelta
import asyncio
from data.stores import stores_to_remove
from data.date_intervals import days_map, available_periods
from components.headers import header_appointments
from helpers.date import transform_date_from_appointments
from apiCrm.resolvers.fetch_appointmentReport import fetch_and_process_appointment_report 
from views.appointments.appointment_columns import appointments_api_clean_columns
from views.appointments.appointment_cleaner import appointment_crm_columns_reorganizer
from views.appointments.appointment_types import comparecimento_status, procedimento_avaliacao, agendamento_status

def load_data(start_date=None, end_date=None, use_api=False):
    """Load and preprocess leads data."""
    appointments = 'db/appointments.xlsx'

    # TODO Just like what we did @ lead_view.py we're going to adapt the code here too!
    if use_api and start_date and end_date:
        try:
            # Run the async function using asyncio
            appointments_data = asyncio.run(fetch_and_process_appointment_report(start_date, end_date))

            if not appointments_data:
                st.error("Não foi possível obter dados da API. Usando dados locais.")
                return load_data(use_api=False)
            
            df = pd.DataFrame(appointments_data)
            
            # Map API field names to match the excel structure
            df = df.rename(columns={
                'id': 'ID agendamento',
                'client_id': 'ID cliente',
                'startDate': 'Data',
                'endDate': 'Data término',
                'status_code': 'Status Código',
                'status_label': 'Status',
                'name': 'Nome cliente',
                'email': 'Email',
                'telephones': 'Telefone',
                'addressLine': 'Endereço',
                'taxvatFormatted': 'CPF',
                'source': 'Fonte de cadastro do cliente',
                'store': 'Unidade do agendamento',
                'procedure': 'Procedimento',
                'procedure_groupLabel': 'Grupo do procedimento',
                'employee': 'Prestador',
                'oldestParent_createdBy_group': 'Grupo da primeira atendente',
                'comments': 'Observação (mais recente)', # TODO pending from this on...
                'updatedAt': 'Data de atualização',
                'updatedBy_name': 'Atualizado por',
                'latest_comment': 'Último comentário',
                'latest_comment_createdAt': 'Data do último comentário',
                'latest_comment_user': 'Usuário do último comentário',
                'oldestParent_createdAt': 'Data do primeiro comentário',
                'oldestParent_createdBy_name': 'Primeiro comentário',
                'beforePhotoUrl': 'Antes',
                'batchPhotoUrl': 'Em processo',
                'afterPhotoUrl': 'Depois',
                
            })
            
            # Convert startDate to datetime
            df['Data'] = pd.to_datetime(df['Data'])
            
            # Format the date for 'Dia' column (single step)
            df['Dia'] = df['Data'].dt.strftime('%d-%m-%Y')
            
            st.success(f"Dados obtidos com sucesso via API: {len(df)} agendamentos carregados.")
            
        except Exception as e:
            st.error(f"Erro ao buscar dados da API: {str(e)}")
            return load_data(use_api=False)
    else:
        # Use the original Excel data source
        appointments = 'db/appointments.xlsx'
        df = pd.read_excel(appointments)
        
    # Apply common transformations
    df = df.loc[~df['Unidade do agendamento'].isin(stores_to_remove)]

    # Only apply date transformation if not using API
    if not use_api:
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

    st.sidebar.header("Filtros")
    use_date_range = st.sidebar.checkbox("Usar intervalo de datas personalizado", False)
    
    use_api = False
    start_date = None
    end_date = None
    
    if use_date_range:
        col1, col2 = st.sidebar.columns(2)
        with col1:
            start_date = st.date_input(
                "Data Inicial",
                value=datetime.now() - timedelta(days=30),
                max_value=datetime.now()
            ).strftime('%Y-%m-%d')
        with col2:
            end_date = st.date_input(
                "Data Final",
                value=datetime.now(),
                max_value=datetime.now()
            ).strftime('%Y-%m-%d')
        
        use_api = st.sidebar.checkbox("Usar API para buscar dados", True)

        if use_api:
            st.sidebar.info("Os dados serão buscados da API usando o intervalo de datas selecionado.")
    
    df_appointments = load_data(start_date, end_date, use_api)

    if not use_date_range:
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
    
    
    df_appointments_agendamentos = df_appointments[
                                        (df_appointments['Status'].isin(agendamento_status)) 
                                        & (df_appointments['Procedimento'].isin(procedimento_avaliacao))]

    df_appointments_comparecimentos = df_appointments[
                                        (df_appointments['Status'].isin(comparecimento_status)) 
                                        & (df_appointments['Procedimento'].isin(procedimento_avaliacao))]

    # Display df_appointments columns in text:
    df_appointments_clean = df_appointments[appointments_api_clean_columns]
    df_appointments_clean = appointment_crm_columns_reorganizer(df_appointments_clean)
    st.dataframe(df_appointments_clean)

    # Display df_appointments_agendamentos columns in text:
    df_appointments_agendamentos_clean = df_appointments_agendamentos[appointments_api_clean_columns]
    df_appointments_agendamentos_clean = appointment_crm_columns_reorganizer(df_appointments_agendamentos_clean)
    st.dataframe(df_appointments_agendamentos_clean)

    # Display df_appointments_comparecimentos columns in text:
    df_appointments_comparecimentos_clean = df_appointments_comparecimentos[appointments_api_clean_columns]
    df_appointments_comparecimentos_clean = appointment_crm_columns_reorganizer(df_appointments_comparecimentos_clean)
    st.dataframe(df_appointments_comparecimentos_clean)