import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
from datetime import datetime, timedelta
import asyncio
from components.headers import header_appointments
from apiCrm.resolvers.dashboard.fetch_appointmentReport import fetch_and_process_appointment_report 
from frontend.appointments.appointment_columns import appointments_api_clean_columns
from frontend.appointments.appointment_cleaner import appointment_crm_columns_reorganizer
from frontend.appointments.appointment_types import (
                                                comparecimento_status, 
                                                procedimento_avaliacao, 
                                                agendamento_status, 
                                                agendamentos_do_dia_status)
from frontend.appointments.appointments_grouper import (
                                                    groupby_agendamentos_por_dia,
                                                    groupby_agendamentos_por_unidade,
                                                    groupby_comparecimentos_por_dia,
                                                    groupby_comparecimentos_por_unidade,
                                                    groupby_agendamentos_por_dia_pivoted,
                                                    groupby_agendamentos_por_dia_e_status_transposed)
from components.date_input import date_input
from helpers.discord import send_discord_message

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
            appointments_data = asyncio.run(fetch_and_process_appointment_report(start_date, end_date))

            if not appointments_data:
                st.error("Não foi possível obter dados da API. Usando dados locais.")
                return load_data(use_api=False)
            
            df = pd.DataFrame(appointments_data)
            
            # Format the date for 'Dia' column (single step)
            df['Dia'] = pd.to_datetime(df['Data']).dt.strftime('%d-%m-%Y')
            
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

def load_page_appointments():
    """Main function to display leads data."""
    
    st.title("📊 2 - Agendamentos")
    st.markdown("---")
    st.subheader("Selecione o intervalo de datas para o relatório:")

    start_date, end_date = date_input()
        
    if st.button("Carregar"):
        send_discord_message(f"Loading data in page appointments_view")
        with st.spinner("Carregando dados..."):
            df_appointments = load_data(start_date, end_date)

            ########
            # Header
            header_appointments(df_appointments)

            df_appointments['Data'] = pd.to_datetime(df_appointments['Data']).dt.date
            remove_pg_store = 'PRAIA GRANDE'
            df_appointments = df_appointments.loc[df_appointments['Unidade do agendamento'] != remove_pg_store]
            
            # Filter for appointments (agendamentos)
            df_appointments_agendamentos = df_appointments[
                                            (df_appointments['Status'].isin(agendamento_status)) 
                                            & (df_appointments['Procedimento'].isin(procedimento_avaliacao))]

            # Filter for comparecimentos
            df_appointments_comparecimentos = df_appointments[
                                            (df_appointments['Status'].isin(comparecimento_status)) 
                                            & (df_appointments['Procedimento'].isin(procedimento_avaliacao))]

            # Filter for appointments (agendamento do dia / futuro)
            df_appointments_agendamentos_futuros = df_appointments[
                                            (df_appointments['Status'].isin(agendamentos_do_dia_status)) 
                                            & (df_appointments['Procedimento'].isin(procedimento_avaliacao))]

            # Removing the hour from "Data"
            df_appointments_agendamentos['Data'] = pd.to_datetime(df_appointments_agendamentos['Data']).dt.date
            df_appointments_comparecimentos['Data'] = pd.to_datetime(df_appointments_comparecimentos['Data']).dt.date
            
            #######
            # Div 1 Análise Detalhada: Agendamentos por Dia do Mês e Agendamentos por Unidade
            col1, col2 = st.columns(2)
            
            with col1:
                groupby_agendamentos_by_day = groupby_agendamentos_por_dia(df_appointments_agendamentos)
                
                fig_day = px.line(
                    groupby_agendamentos_by_day,
                    x='Data',
                    y='ID agendamento',
                    title='Agendamentos por Dia do Mês',
                    labels={'ID agendamento': 'Quantidade de Agendamentos', 'Data': 'Dia do mês'},
                    markers=True
                )
                st.plotly_chart(fig_day, use_container_width=True, key='fig_day_agendamentos')
            
            with col2:
                groupby_agendamentos_by_store = groupby_agendamentos_por_unidade(df_appointments_agendamentos)
                
                fig_store = px.bar(
                    groupby_agendamentos_by_store,
                    x='Unidade do agendamento',
                    y='ID agendamento',
                    title='Agendamentos por Unidade',
                    labels={'ID agendamento': 'Quantidade de Agendamentos', 'Unidade do agendamento': 'Unidade'}
                )
                st.plotly_chart(fig_store, use_container_width=True, key='fig_store_agendamentos')
            
            #######
            # Div 2 Análise Detalhada: Comparecimentos por Dia do Mês e Comparecimentos por Unidade
            col1, col2 = st.columns(2)
            
            with col1:
                groupby_comparecimentos_by_day = groupby_comparecimentos_por_dia(df_appointments_comparecimentos)
                
                fig_day = px.line(
                    groupby_comparecimentos_by_day,
                    x='Data',
                    y='ID agendamento',
                    title='Comparecimentos por Dia do Mês',
                    labels={'ID agendamento': 'Quantidade de Comparecimentos', 'Data': 'Dia do mês'},
                    markers=True,
                )
                st.plotly_chart(fig_day, use_container_width=True, key='fig_day_comparecimentos')
            
            with col2:
                groupby_comparecimentos_by_store = groupby_comparecimentos_por_unidade(df_appointments_comparecimentos)
                
                fig_store = px.bar(
                    groupby_comparecimentos_by_store,
                    x='Unidade do agendamento',
                    y='ID agendamento',
                    title='Comparecimentos por Unidade',
                    labels={'ID agendamento': 'Quantidade de Comparecimentos', 'Unidade do agendamento': 'Unidade'}
                )
                st.plotly_chart(fig_store, use_container_width=True, key='fig_store_comparecimentos')

            ########
            # Div 3: Tabela de Comparecimentos por Dia e por Unidade
            today = datetime.now()
            last_day = today - timedelta(days=1)
            mask_date_interval = (df_appointments_comparecimentos['Data'] >= last_day.date()) & (df_appointments_comparecimentos['Data'] <= today.date())
            df_appointments_comparecimentos_by_day = groupby_agendamentos_por_dia_pivoted(df_appointments_comparecimentos[mask_date_interval])
            st.write("Comparecimentos por Dia e por Unidade:")
            st.dataframe(df_appointments_comparecimentos_by_day)

            ########
            # Div 4: Agenda do dia:
            mask_date_interval_next_4_days = (df_appointments_agendamentos_futuros['Data'] >= today.date()) & (df_appointments_agendamentos_futuros['Data'] <= (today.date() + timedelta(days=3)))
            df_appointments_today = df_appointments_agendamentos_futuros[mask_date_interval_next_4_days]
            df_appointments_today_by_day_and_store = groupby_agendamentos_por_dia_e_status_transposed(df_appointments_today)

            st.write(f"Agenda dos próximos 4 dias")
            st.dataframe(df_appointments_today_by_day_and_store)

            # # DEBUGGING:
            # df_appointments_clean = df_appointments[appointments_api_clean_columns]        
            # df_appointments_clean = appointment_crm_columns_reorganizer(df_appointments_clean)
            # st.write("Debugging: df_appointments")
            # st.dataframe(df_appointments_clean)

            # df_appointments_agendamentos_clean = df_appointments_agendamentos[appointments_api_clean_columns]
            # df_appointments_agendamentos_clean = appointment_crm_columns_reorganizer(df_appointments_agendamentos_clean)
            # st.write("Debugging: df_appointments_agendamentos")
            # st.dataframe(df_appointments_agendamentos_clean)

            # df_appointments_comparecimentos_clean = df_appointments_comparecimentos[appointments_api_clean_columns]
            # df_appointments_comparecimentos_clean = appointment_crm_columns_reorganizer(df_appointments_comparecimentos_clean)
            # st.write("Debugging: df_appointments_comparecimentos")
            # st.dataframe(df_appointments_comparecimentos_clean)    