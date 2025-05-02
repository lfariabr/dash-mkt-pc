import asyncio
import pandas as pd
import streamlit as st
from datetime import datetime

from apiCrm.resolvers.fetch_leadsByUserReport import fetch_and_process_leadsByUserReport
from apiCrm.resolvers.fetch_appointmentReport import fetch_and_process_appointment_report_created_at
from helpers.data_wrestler import extract_agendamentos, enrich_leadsByUser_df, append_total_rows_leadsByUser, highlight_total_row_leadsByUser
from components.date_input import date_input

from views.appointments.appointment_types import procedimento_avaliacao, agendamento_status_por_atendente
from views.coc.atendentes import atendentes_puxadas_manha, atendentes_puxadas_tarde
from views.coc.columns import leadsByUserColumns, leadsByUser_display_columns
from views.coc.atendentes import get_atendente_from_spreadsheet

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
        from utils.discord import send_discord_message
        send_discord_message(f"Loading data in page leadsByUserReport_view")
        with st.spinner("Carregando dados..."):
            df_leadsByUser, df_appointments = load_data(start_date, end_date)
            
            if df_leadsByUser.empty or df_appointments.empty:
                st.warning("Não foram encontrados dados para o período selecionado.")

            else:

                # Debugging...
                # st.write(f"dfleadsByUser raw: {len(df_leadsByUser)} rows")
                # st.dataframe(df_leadsByUser)
                # st.write("---")
                
                # Select basic columns and rename them
                df_leadsByUser = df_leadsByUser[leadsByUserColumns]
                
                # Process messages_count_by_status to extract agendamentos data
                df_leadsByUser['agendamentos_por_lead'] = 0
                df_leadsByUser['agendamentos_por_lead'] = df_leadsByUser['messages_count_by_status'].apply(extract_agendamentos)
                                
                # Add location and shift info
                atendentes_puxadas_manha_test, atendentes_puxadas_tarde_test = get_atendente_from_spreadsheet()

                df_leadsByUser = enrich_leadsByUser_df(df_leadsByUser, atendentes_puxadas_manha, atendentes_puxadas_tarde)
                
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
                
                df_leadsByUser = df_leadsByUser.reset_index(drop=True)
                df_leadsByUser = df_leadsByUser.sort_values(by='Leads Puxados', ascending=False)
                
                # Extract Attendants from the pre-defined lists
                df_leadsByUser_manha = df_leadsByUser[df_leadsByUser['Atendente'].isin(atendentes_puxadas_manha.keys())]
                df_leadsByUser_manha_filtered = df_leadsByUser_manha[leadsByUser_display_columns]
                df_leadsByUser_tarde = df_leadsByUser[df_leadsByUser['Atendente'].isin(atendentes_puxadas_tarde.keys())]
                df_leadsByUser_tarde_filtered = df_leadsByUser_tarde[leadsByUser_display_columns]
                                
                # Display dataframes leadsByUser -> for testing
                # st.subheader("Atendentes do Turno da Manhã")
                # st.dataframe(df_leadsByUser_manha_filtered, hide_index=True)
                # st.subheader("Atendentes do Turno da Tarde")
                # st.dataframe(df_leadsByUser_tarde_filtered, hide_index=True)

                # COC rules:
                # 1) Status = agendamento_status_por_atendente
                # 2) Procedimento = procedimento_avaliacao
                # 3) Data primeira atendente = start_date
                df_appointments_agendamentos = df_appointments[
                                            (df_appointments['Status'].isin(agendamento_status_por_atendente)) 
                                            & (df_appointments['Procedimento'].isin(procedimento_avaliacao))]
                df_appointments_agendamentos['Data primeira atendente'] = pd.to_datetime(df_appointments_agendamentos['Data primeira atendente']).dt.date
                
                start_date = pd.to_datetime(start_date).date()
                df_appointments_agendamentos['data_primeira_atendente_is_start_date?'] = df_appointments_agendamentos['Data primeira atendente'] == start_date
                
                # filtered = agendamentos that match start_date - rule #3
                df_appointments_agendamentos_filtered_coc_rules = df_appointments_agendamentos[df_appointments_agendamentos['data_primeira_atendente_is_start_date?'] == True]
                
                # manhã
                df_appointments_atendentes_manha = df_appointments_agendamentos_filtered_coc_rules[
                    df_appointments_agendamentos_filtered_coc_rules['Nome da primeira atendente']
                    .isin(atendentes_puxadas_manha.keys())]
                df_appointments_atendentes_manha_grouped = df_appointments_atendentes_manha.groupby('Nome da primeira atendente').agg({'ID agendamento': 'nunique'}).reset_index()
                
                # tarde
                df_appointments_atendentes_tarde = df_appointments_agendamentos_filtered_coc_rules[
                    df_appointments_agendamentos_filtered_coc_rules['Nome da primeira atendente']
                    .isin(atendentes_puxadas_tarde.keys())
                ]
                df_appointments_atendentes_tarde_grouped = df_appointments_atendentes_tarde.groupby('Nome da primeira atendente').agg({'ID agendamento': 'nunique'}).reset_index()
                
                # Display dataframes AppointmentsCreatedAtRange -> for testing
                # st.subheader("Agendamentos do Turno da Manhã")
                # st.dataframe(df_appointments_atendentes_manha_grouped, hide_index=True)
                # st.subheader("Agendamentos do Turno da Tarde")
                # st.dataframe(df_appointments_atendentes_tarde_grouped, hide_index=True)

                # Merging Puxadas with Appointments + Final Touch
                df_leadsByUser_and_appointments_manha = pd.merge(
                    df_leadsByUser_manha_filtered,
                    df_appointments_atendentes_manha_grouped,
                    left_on='Atendente',
                    right_on='Nome da primeira atendente',
                    how='left'
                )
                df_leadsByUser_and_appointments_manha = df_leadsByUser_and_appointments_manha.drop(columns=['Nome da primeira atendente'])
                df_leadsByUser_and_appointments_manha = df_leadsByUser_and_appointments_manha.fillna(0)
                df_leadsByUser_and_appointments_manha = df_leadsByUser_and_appointments_manha.rename(columns={'ID agendamento': 'Agendamentos na Agenda'})

                df_leadsByUser_and_appointments_tarde = pd.merge(
                    df_leadsByUser_tarde_filtered,
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
                    df_leadsByUser_and_appointments_manha_totals.style
                    .apply(highlight_total_row_leadsByUser, axis=1)
                    .format({
                        'Leads Puxados': '{:.0f}',
                        'Leads Puxados (únicos)': '{:.0f}',
                        'Agendamentos por lead': '{:.0f}',
                        'Agendamentos na Agenda': '{:.0f}'
                    }),
                    hide_index=True,
                    height=len(df_leadsByUser_and_appointments_manha)* 45, 
                    use_container_width=True)

                st.subheader("Leads e Agendamentos - Tarde")
                st.dataframe(
                    df_leadsByUser_and_appointments_tarde_totals.style
                    .apply(highlight_total_row_leadsByUser, axis=1)
                    .format({
                        'Leads Puxados': '{:.0f}',
                        'Leads Puxados (únicos)': '{:.0f}',
                        'Agendamentos por lead': '{:.0f}',
                        'Agendamentos na Agenda': '{:.0f}'
                    }),
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
                    df_leadsByUser_and_appointments_all.style
                    .apply(highlight_total_row_leadsByUser, axis=1)
                    .format({
                        'Leads Puxados': '{:.0f}',
                        'Leads Puxados (únicos)': '{:.0f}',
                        'Agendamentos por lead': '{:.0f}',
                        'Agendamentos na Agenda': '{:.0f}'
                    }),
                    hide_index=True,
                    height=len(df_leadsByUser_and_appointments_all) * 45,
                    use_container_width=True)

                # Debugging appointments of Atendente "Ingrid Caroline Santos Andrade"
                # df_appointments_atendentes_ingrid = df_appointments_agendamentos_filtered_coc_rules[df_appointments_agendamentos_filtered_coc_rules['Nome da primeira atendente'] == 'Ingrid Caroline Santos Andrade']
                # df_appointments_atendentes_ingrid_valid = df_appointments_atendentes_ingrid[df_appointments_atendentes_ingrid['data_primeira_atendente_is_start_date?'] == True]
                # st.subheader(f"Debugging appointments from 'Ingrid Caroline Santos Andrade': {len(df_appointments_atendentes_ingrid_valid)}")
                # st.dataframe(df_appointments_atendentes_ingrid_valid, hide_index=True)

                # Debugging appointments of Atendente "Ingrid Caroline Santos Andrade"
                st.dataframe(atendentes_puxadas_manha_test)
                st.dataframe(atendentes_puxadas_tarde_test)
                st.dataframe(atendentes_puxadas_manha)
                st.dataframe(atendentes_puxadas_tarde)


                