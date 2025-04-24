import asyncio
import pandas as pd
import streamlit as st
from datetime import datetime

from apiCrm.resolvers.fetch_leadsByUserReport import fetch_and_process_leadsByUserReport
from apiCrm.resolvers.fetch_appointmentReport import fetch_and_process_appointment_report_created_at
from components.date_input import date_input

from views.appointments.appointment_types import procedimento_avaliacao, agendamento_status_por_atendente
from views.coc.atendentes import atendentes_puxadas_manha, atendentes_puxadas_tarde
                

async def fetch_leads_and_appointments(start_date, end_date):
    """
    Run both API calls concurrently to improve performance.
    """
    leads_data = fetch_and_process_leadsByUserReport(start_date, end_date)
    appointments_data = fetch_and_process_appointment_report_created_at(start_date, end_date)

    # Executing both fetches
    leads_data, appointments_data = await asyncio.gather(leads_data, appointments_data)
    return leads_data, appointments_data


def load_data(start_date=None, end_date=None):
    if start_date and end_date:
        try:
            leads_data, appointments_data = asyncio.run(fetch_leads_and_appointments(start_date, end_date))
            return pd.DataFrame(leads_data), pd.DataFrame(appointments_data)
        except Exception as e:
            st.error(f"Erro ao carregar dados: {str(e)}")
            return pd.DataFrame(), pd.DataFrame()
    else:
        st.warning("Por favor, selecione um intervalo de datas.")
        return pd.DataFrame(), pd.DataFrame()

def load_page_leadsByUser():
    """Main function to display leads by user data."""

    st.title("📊 1 - Puxada de Leads")
    st.markdown("---")
    st.subheader("Selecione o intervalo de datas para o relatório:")
    
    start_date, end_date = date_input()
    
    if st.button("Carregar"):
        with st.spinner("Carregando dados..."):
            df_leadsByUser, df_appointments = load_data(start_date, end_date)
            
            if df_leadsByUser.empty or df_appointments.empty:
                st.warning("Não foram encontrados dados para o período selecionado.")
            else:
                st.write("Debugging first...")
                st.dataframe(df_leadsByUser)
                st.write("---")
                # Treating data columns:
                leadsByUserColumns = ['name', 'messages_count', 'unique_messages_count', 'messages_count_by_status', 'success_rate']
                agendamento_por_lead_column = ['agd', 'jag'] # to filter messages_count_by_status column    
                
                # Select basic columns and rename them
                df_leadsByUser = df_leadsByUser[leadsByUserColumns]
                
                # Process messages_count_by_status to extract agendamentos data
                df_leadsByUser['agendamentos_por_lead'] = 0
                
                # Check if messages_count_by_status is a dictionary and extract values
                def extract_agendamentos(status_dict):
                    if isinstance(status_dict, dict):
                        return sum(status_dict.get(status, 0) for status in agendamento_por_lead_column)
                    return 0
                
                df_leadsByUser['agendamentos_por_lead'] = df_leadsByUser['messages_count_by_status'].apply(extract_agendamentos)
                                
                # Add location from dictionaries
                df_leadsByUser['local'] = ''
                df_leadsByUser['turno'] = ''
                
                # Add location and shift info
                for atendente, local in atendentes_puxadas_manha.items():
                    mask = df_leadsByUser['name'] == atendente
                    if mask.any():
                        df_leadsByUser.loc[mask, 'local'] = local
                        df_leadsByUser.loc[mask, 'turno'] = 'Manhã'
                
                for atendente, local in atendentes_puxadas_tarde.items():
                    mask = df_leadsByUser['name'] == atendente
                    if mask.any():
                        df_leadsByUser.loc[mask, 'local'] = local
                        df_leadsByUser.loc[mask, 'turno'] = 'Tarde'
                
                # Add a default team column (can be customized later)
                df_leadsByUser['Tam'] = 'P'  # Default team
                
                # Rename columns
                df_leadsByUser = df_leadsByUser.rename(columns={
                    'name': 'Atendente',
                    'messages_count': 'Leads Puxados',
                    'unique_messages_count': 'Leads Puxados (únicos)',
                    'agendamentos_por_lead': 'Agendamentos por lead',
                    'local': 'Unidade',
                    'Tam': 'Tam',
                    'turno': 'Turno',
                    'success_rate': 'Conversão'
                })
                
                # Reset index and sort
                df_leadsByUser = df_leadsByUser.reset_index(drop=True)
                df_leadsByUser = df_leadsByUser.sort_values(by='Leads Puxados (únicos)', ascending=False)
                
                # Filter to show only attendants from the lists
                all_atendentes = list(atendentes_puxadas_manha.keys()) + list(atendentes_puxadas_tarde.keys())
                df_atendentes_manha = df_leadsByUser[df_leadsByUser['Atendente'].isin(atendentes_puxadas_manha.keys())]
                df_atendentes_tarde = df_leadsByUser[df_leadsByUser['Atendente'].isin(atendentes_puxadas_tarde.keys())]
                
                # Define display columns order
                display_columns = ['Atendente', 'Unidade', 'Turno', 'Tam',
                                    'Leads Puxados', 'Leads Puxados (únicos)',
                                    'Agendamentos por lead', 'Conversão']
                
                # Display dataframes
                st.subheader("Atendentes do Turno da Manhã")
                st.dataframe(df_atendentes_manha[display_columns], hide_index=True)
                
                st.subheader("Atendentes do Turno da Tarde")
                st.dataframe(df_atendentes_tarde[display_columns], hide_index=True)

                # Filtering for Appointments COC will consider valid
                df_appointments_agendamentos = df_appointments[
                                            (df_appointments['Status'].isin(agendamento_status_por_atendente)) 
                                            & (df_appointments['Procedimento'].isin(procedimento_avaliacao))]

                # Matching "Data primeira atendente" with start_date @ graphql request
                df_appointments_agendamentos['Data primeira atendente'] = pd.to_datetime(df_appointments_agendamentos['Data primeira atendente']).dt.date
                start_date = pd.to_datetime(start_date).date()

                # Doing last filter to match start_date
                df_appointments_agendamentos['data_primeira_atendente_is_start_date?'] = df_appointments_agendamentos['Data primeira atendente'] == start_date
                df_appointments_agendamentos_filtered = df_appointments_agendamentos[df_appointments_agendamentos['data_primeira_atendente_is_start_date?'] == True]
                
                st.subheader("Agendamentos do Turno da Manhã")
                df_appointments_atendentes_manha = df_appointments_agendamentos_filtered[
                    df_appointments_agendamentos_filtered['Nome da primeira atendente']
                    .isin(atendentes_puxadas_manha.keys())]
                df_appointments_atendentes_manha_grouped = df_appointments_atendentes_manha.groupby('Nome da primeira atendente').agg({'ID agendamento': 'nunique'}).reset_index()

                st.dataframe(df_appointments_atendentes_manha_grouped, hide_index=True)
                
                st.subheader("Agendamentos do Turno da Tarde")
                df_appointments_atendentes_tarde = df_appointments_agendamentos_filtered[
                    df_appointments_agendamentos_filtered['Nome da primeira atendente']
                    .isin(atendentes_puxadas_tarde.keys())
                ]
                df_appointments_atendentes_tarde_grouped = df_appointments_atendentes_tarde.groupby('Nome da primeira atendente').agg({'ID agendamento': 'nunique'}).reset_index()

                st.dataframe(df_appointments_atendentes_tarde_grouped, hide_index=True)

                # Debugging appointments of Atendente "Ingrid Caroline Santos Andrade"
                st.subheader("Debug dados Atendente 'Ingrid Caroline Santos Andrade'")
                df_appointments_atendentes_ingrid = df_appointments_agendamentos[df_appointments_agendamentos['Nome da primeira atendente'] == 'Ingrid Caroline Santos Andrade']
                df_appointments_atendentes_ingrid_valid = df_appointments_atendentes_ingrid[df_appointments_atendentes_ingrid['data_primeira_atendente_is_start_date?'] == True]

                st.dataframe(df_appointments_atendentes_ingrid_valid, hide_index=True)

                # Merging Puxadas Manhã with Agendamentos Manhã
                df_leads_and_appointments_manha = pd.merge(df_atendentes_manha, df_appointments_atendentes_manha_grouped, left_on='Atendente', right_on='Nome da primeira atendente', how='left')

                # Merging Puxadas Tarde with Agendamentos Tarde
                df_leads_and_appointments_tarde = pd.merge(df_atendentes_tarde, df_appointments_atendentes_tarde_grouped, left_on='Atendente', right_on='Nome da primeira atendente', how='left')

                # Displaying merged dataframes
                st.subheader("Agendamentos e Leads do Turno da Manhã")
                st.dataframe(df_leads_and_appointments_manha, hide_index=True)

                st.subheader("Agendamentos e Leads do Turno da Tarde")
                st.dataframe(df_leads_and_appointments_tarde, hide_index=True)

                # JOB TO BE DONE

                

                

                
                
                