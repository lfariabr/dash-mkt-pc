import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
import asyncio
from apiCrm.resolvers.fetch_appointmentReport import fetch_and_process_appointment_report_created_at 
from views.appointments.appointment_columns import appointments_api_clean_columns
from views.appointments.appointment_cleaner import appointment_crm_columns_reorganizer
from views.appointments.appointment_types import comparecimento_status, procedimento_avaliacao, agendamento_status_por_atendente

def load_data(start_date=None, end_date=None, use_api=False):
    """
    Load and preprocess appointments data.
    
    Args:
        start_date (str, optional): Start date in YYYY-MM-DD format for API fetch
        end_date (str, optional): End date in YYYY-MM-DD format for API fetch
        use_api (bool): Whether to use the API or local Excel file
        
    Returns:
        DataFrame: Processed appointments dataframe
    """
    
    if start_date and end_date:
        try:
            # Run the async function using asyncio
            appointments_data = asyncio.run(fetch_and_process_appointment_report_created_at(start_date, end_date))

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
                'updatedBy': 'Atualizado por',
                'createdBy': 'Nome da primeira atendente',
                'createdBy_group': 'Grupo da Primeira Atendente',
                'createdAt': 'Data de criação',
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
        st.warning("Por favor, selecione um intervalo de datas.")
        return pd.DataFrame()
        
    # Apply common transformations
    # df = df.loc[~df['Unidade do agendamento'].isin(stores_to_remove)]
    
    return df

def create_time_filtered_df(df, days=None):
    """Create a time-filtered dataframe."""
    if days:
        cutoff_date = datetime.now() - timedelta(days=days)
        return df[df['Data'] >= cutoff_date]
    return df

def load_page_appointments_CreatedAt():
    """Main function to display leads data."""
    
    st.title("📊 2 - Agendamentos")
    st.markdown("---")
    st.subheader("Selecione o intervalo de datas para o relatório:")

    start_date = None
    end_date = None
    
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "Data Inicial",
            value=datetime.now() - timedelta(days=2),
            max_value=datetime.now()
        ).strftime('%Y-%m-%d')
    with col2:
        end_date = st.date_input(
            "Data Final",
            value=datetime.now(),
            max_value=datetime.now() + timedelta(days=5)
        ).strftime('%Y-%m-%d')
        
    if st.button("Carregar"):
        with st.spinner("Carregando dados..."):
            df_appointments = load_data(start_date, end_date)

            ########               
            df_appointments['Data'] = pd.to_datetime(df_appointments['Data']).dt.date
            
            # Filter for appointments (agendamentos)
            df_appointments_agendamentos = df_appointments[
                                            (df_appointments['status'].isin(agendamento_status_por_atendente)) 
                                            & (df_appointments['Procedimento'].isin(procedimento_avaliacao))]

            # Filter for comparecimentos
            df_appointments_comparecimentos = df_appointments[
                                            (df_appointments['status'].isin(comparecimento_status)) 
                                            & (df_appointments['Procedimento'].isin(procedimento_avaliacao))]

            # Removing the hour from "Data"
            df_appointments_agendamentos['Data'] = pd.to_datetime(df_appointments_agendamentos['Data']).dt.date
            df_appointments_comparecimentos['Data'] = pd.to_datetime(df_appointments_comparecimentos['Data']).dt.date


            ########
            # TODO move onwards
            # DEBUGGING:
            st.dataframe(df_appointments, hide_index=True)
            
            # df_appointments_clean = df_appointments[appointments_api_clean_columns]        
            # df_appointments_clean = appointment_crm_columns_reorganizer(df_appointments_clean)
            st.write("Debugging: df_appointments_agendamentos")
            st.dataframe(df_appointments_agendamentos)

            # Groupby df_appointments_agendamentos per 'Nome da primeira atendente'
            df_appointments_agendamentos_grouped = df_appointments_agendamentos.groupby('Nome da primeira atendente').size().reset_index(name='Count').sort_values(by='Count', ascending=False)
            st.write("Debugging: df_appointments_agendamentos_grouped")
            st.dataframe(df_appointments_agendamentos_grouped, hide_index=True)
