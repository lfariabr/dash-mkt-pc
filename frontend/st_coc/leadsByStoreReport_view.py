import asyncio
import pandas as pd
import streamlit as st
from datetime import datetime
from components.date_input import date_input
from frontend.coc.atendentes import get_atendente_from_spreadsheet
from frontend.coc.stores import get_stores_from_spreadsheet
from apiCrm.resolvers.coc.fetch_leadsByUserReport import fetch_and_process_leadsByUserReport
from apiCrm.resolvers.dashboard.fetch_appointmentReport import fetch_and_process_appointment_report_created_at
from helpers.coc_worker import normalize_name, apply_formatting_leadsByUser
from helpers.data_wrestler import (
    extract_agendamentos,
    append_total_rows_leadsByUser,
    highlight_total_row_leadsByUser
)
from frontend.coc.columns import leadsByUserColumns, leadsByUser_display_columns
from frontend.appointments.appointment_types import procedimento_avaliacao, agendamento_status_por_atendente

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

def load_page_leadsByStore():
    """Main function to display leads by user data."""

    st.title("📊 3 - Puxada de Leads por Loja")
    st.markdown("---")
    st.subheader("Selecione o intervalo de datas para o relatório:")
    
    start_date, end_date = date_input()
    
    if st.button("Carregar"):
        from utils.discord import send_discord_message
        send_discord_message(f"Loading data in page leadsByStoreReport_view")
        with st.spinner("Carregando dados..."):
            df_leadsByUser, df_appointments = load_data(start_date, end_date)
            
            if df_leadsByUser.empty or df_appointments.empty:
                st.warning("Não foram encontrados dados para o período selecionado.")

            else:
                # Select basic columns and rename them
                df_leadsByUser = df_leadsByUser[leadsByUserColumns]
                # Process messages_count_by_status to extract agendamentos data
                df_leadsByUser['agendamentos_por_lead'] = 0
                df_leadsByUser['agendamentos_por_lead'] = df_leadsByUser['messages_count_by_status'].apply(extract_agendamentos)
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
                df_leadsByUser = df_leadsByUser.reset_index(drop=True)
                df_leadsByUser = df_leadsByUser.sort_values(by='Leads Puxados', ascending=False)
                
                pro_corpo_stores = get_stores_from_spreadsheet()
                st.dataframe(pro_corpo_stores)

                # TODO here onwards

                atendentes_puxadas_manha, atendentes_puxadas_tarde = get_atendente_from_spreadsheet()
                atendentes_puxadas_manha['Atendente'] = atendentes_puxadas_manha['Atendente'].apply(normalize_name)
                atendentes_puxadas_tarde['Atendente'] = atendentes_puxadas_tarde['Atendente'].apply(normalize_name)
                df_leadsByUser['Atendente'] = df_leadsByUser['Atendente'].apply(normalize_name)
                
                # Step 1: filter leadsByUser where 'Atendente' matches the 'Atendente' in atendentes_puxadas_manha && atendentes_puxadas_tarde 
                df_leadsByUser_manha = df_leadsByUser[df_leadsByUser['Atendente'].isin(atendentes_puxadas_manha['Atendente'])]
                df_leadsByUser_tarde = df_leadsByUser[df_leadsByUser['Atendente'].isin(atendentes_puxadas_tarde['Atendente'])]

                # Step 2: merge to enrich with additional info (like Unidade, Turno, Tam)
                df_leadsByUser_manha = df_leadsByUser_manha.merge(
                    atendentes_puxadas_manha,
                    how='left',
                    left_on='Atendente',
                    right_on='Atendente'
                )
                df_leadsByUser_tarde = df_leadsByUser_tarde.merge(
                    atendentes_puxadas_tarde,
                    how='left',
                    left_on='Atendente',
                    right_on='Atendente'
                )

                # Extract Attendants from the pre-defined lists
                df_leadsByUser_manha = df_leadsByUser_manha[leadsByUser_display_columns]
                df_leadsByUser_tarde = df_leadsByUser_tarde[leadsByUser_display_columns]
                                
                # COC rules:
                # 1) Status = agendamento_status_por_atendente
                # 2) Procedimento = procedimento_avaliacao
                df_appointments_agendamentos = df_appointments[
                                            (df_appointments['Status'].isin(agendamento_status_por_atendente)) 
                                            & (df_appointments['Procedimento'].isin(procedimento_avaliacao))]
                
                # 3) Data primeira atendente = start_date
                df_appointments_agendamentos['Data primeira atendente'] = pd.to_datetime(
                    df_appointments_agendamentos['Data primeira atendente'],
                    dayfirst=True,
                    errors='coerce'
                )
                
                start_date = pd.to_datetime(start_date).date()
                df_appointments_agendamentos['data_primeira_atendente_is_start_date?'] = df_appointments_agendamentos['Data primeira atendente'].dt.date == start_date
                
                # filtered = agendamentos that match start_date - rule #3
                df_appointments_agendamentos_filtered_coc_rules = df_appointments_agendamentos[df_appointments_agendamentos['data_primeira_atendente_is_start_date?'] == True]
                
                # manhã
                df_appointments_atendentes_manha = df_appointments_agendamentos_filtered_coc_rules[
                    df_appointments_agendamentos_filtered_coc_rules['Nome da primeira atendente']
                    .isin(atendentes_puxadas_manha['Atendente'])]
                df_appointments_atendentes_manha_grouped = df_appointments_atendentes_manha.groupby('Nome da primeira atendente').agg({'ID agendamento': 'nunique'}).reset_index()
                
                # tarde
                df_appointments_atendentes_tarde = df_appointments_agendamentos_filtered_coc_rules[
                    df_appointments_agendamentos_filtered_coc_rules['Nome da primeira atendente']
                    .isin(atendentes_puxadas_tarde['Atendente'])
                ]
                df_appointments_atendentes_tarde_grouped = df_appointments_atendentes_tarde.groupby('Nome da primeira atendente').agg({'ID agendamento': 'nunique'}).reset_index()
                
                # Merging Puxadas with Appointments + Final Touch
                df_leadsByUser_and_appointments_manha = pd.merge(
                    df_leadsByUser_manha,
                    df_appointments_atendentes_manha_grouped,
                    left_on='Atendente',
                    right_on='Nome da primeira atendente',
                    how='left'
                )
                df_leadsByUser_and_appointments_manha = df_leadsByUser_and_appointments_manha.drop(columns=['Nome da primeira atendente'])
                df_leadsByUser_and_appointments_manha = df_leadsByUser_and_appointments_manha.fillna(0)
                df_leadsByUser_and_appointments_manha = df_leadsByUser_and_appointments_manha.rename(columns={'ID agendamento': 'Agendamentos na Agenda'})

                df_leadsByUser_and_appointments_tarde = pd.merge(
                    df_leadsByUser_tarde,
                    df_appointments_atendentes_tarde_grouped,
                    left_on='Atendente',
                    right_on='Nome da primeira atendente',
                    how='left'
                )
                df_leadsByUser_and_appointments_tarde = df_leadsByUser_and_appointments_tarde.drop(columns=['Nome da primeira atendente'])
                df_leadsByUser_and_appointments_tarde = df_leadsByUser_and_appointments_tarde.fillna(0)
                df_leadsByUser_and_appointments_tarde = df_leadsByUser_and_appointments_tarde.rename(columns={'ID agendamento': 'Agendamentos na Agenda'})

                # --- Adding TOTALS before displaying ---
                df_leadsByUser_and_appointments_manha_totals = append_total_rows_leadsByUser(df_leadsByUser_and_appointments_manha)
                df_leadsByUser_and_appointments_tarde_totals = append_total_rows_leadsByUser(df_leadsByUser_and_appointments_tarde)
                
                st.subheader("Leads e Agendamentos - Manhã")
                st.dataframe(
                    apply_formatting_leadsByUser(df_leadsByUser_and_appointments_manha_totals),
                    hide_index=True,
                    height=len(df_leadsByUser_and_appointments_manha)* 45, 
                    use_container_width=True)

                st.subheader("Leads e Agendamentos - Tarde")
                
                st.dataframe(
                    apply_formatting_leadsByUser(df_leadsByUser_and_appointments_tarde_totals),
                    hide_index=True,
                    height=len(df_leadsByUser_and_appointments_tarde) * 45,
                    use_container_width=True)

                # --- Merge both dataframes without total just yet ---
                df_leadsByUser_and_appointments_all = pd.concat(
                    [df_leadsByUser_and_appointments_manha, df_leadsByUser_and_appointments_tarde],
                    ignore_index=True
                )
                df_leadsByUser_and_appointments_all.sort_values(by='Leads Puxados (únicos)', ascending=False, inplace=True)
                df_leadsByUser_and_appointments_all = append_total_rows_leadsByUser(df_leadsByUser_and_appointments_all)

                st.subheader("Leads e Agendamentos - Fechamento")
                st.dataframe(
                    apply_formatting_leadsByUser(df_leadsByUser_and_appointments_all),
                    hide_index=True,
                    height=len(df_leadsByUser_and_appointments_all) * 45,
                    use_container_width=True)

                # Debugging appointments of Atendente "Ingrid Caroline Santos Andrade"
                # df_appointments_atendentes_ingrid = df_appointments_agendamentos_filtered_coc_rules[df_appointments_agendamentos_filtered_coc_rules['Nome da primeira atendente'] == 'Ingrid Caroline Santos Andrade']
                # df_appointments_atendentes_ingrid_valid = df_appointments_atendentes_ingrid[df_appointments_atendentes_ingrid['data_primeira_atendente_is_start_date?'] == True]
                # st.subheader(f"Debugging appointments from 'Ingrid Caroline Santos Andrade': {len(df_appointments_atendentes_ingrid_valid)}")
                # st.dataframe(df_appointments_atendentes_ingrid_valid, hide_index=True)
                