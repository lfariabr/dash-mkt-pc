import asyncio
import pandas as pd
import streamlit as st
from datetime import datetime
from components.date_input import date_input
from frontend.coc.atendentes import get_atendente_from_spreadsheet
from frontend.coc.stores import get_stores_from_spreadsheet
from apiCrm.resolvers.coc.fetch_leadsByUserReport import fetch_and_process_leadsByUserReport
from apiCrm.resolvers.dashboard.fetch_appointmentReport import fetch_and_process_appointment_report_created_at
from helpers.coc_worker import normalize_name, apply_formatting_leadsByStore
from helpers.data_wrestler import (
    extract_agendamentos,
    append_total_rows_leadsByStore,
)
from frontend.coc.columns import leadsByUserColumns, leadsByUser_display_columns
from frontend.appointments.appointment_types import procedimento_avaliacao, agendamento_status_por_atendente
from helpers.discord import send_discord_message

async def fetch_leads_and_appointments(start_date, end_date):
    """
    Run both API calls concurrently to improve performance.
    """
    
    leads_data = fetch_and_process_leadsByUserReport(start_date, end_date)
    appointments_data = fetch_and_process_appointment_report_created_at(start_date, end_date)
    
    def extract_start_date_and_end_date_of_month(start_date):
        start_date_custom = pd.to_datetime(start_date)
        first_day = start_date_custom.replace(day=1)
        last_day = first_day + pd.offsets.MonthEnd(0)
        return first_day.strftime('%Y-%m-%d'), last_day.strftime('%Y-%m-%d')
    month_start_date, month_end_date = extract_start_date_and_end_date_of_month(start_date)

    leads_data_task = asyncio.create_task(fetch_and_process_leadsByUserReport(start_date, end_date))
    appointments_data_task = asyncio.create_task(fetch_and_process_appointment_report_created_at(start_date, end_date))
    leads_data_complete_month_task = asyncio.create_task(fetch_and_process_leadsByUserReport(month_start_date, month_end_date))

    leads_data, appointments_data, leads_data_complete_month = await asyncio.gather(
        leads_data_task,
        appointments_data_task,
        leads_data_complete_month_task
    )

    return leads_data, appointments_data, leads_data_complete_month

def load_data(start_date=None, end_date=None):
    if start_date and end_date:
        try:
            leads_data, appointments_data, leads_data_complete_month = asyncio.run(fetch_leads_and_appointments(start_date, end_date))
            return pd.DataFrame(leads_data), pd.DataFrame(appointments_data), pd.DataFrame(leads_data_complete_month)
        except Exception as e:
            st.error(f"Erro ao carregar dados: {str(e)}")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    else:
        st.warning("Por favor, selecione um intervalo de datas.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

def load_page_leadsByStore():
    """Main function to display leads by user data."""

    st.title("📊 2 - Puxada de Leads por Loja")
    st.markdown("---")
    st.subheader("Selecione o intervalo de datas para o relatório:")
    
    start_date, end_date = date_input()
    
    if st.button("Carregar"):
        send_discord_message(f"Loading data in page leadsByStoreReport_view")
        with st.spinner("Carregando dados..."):
            df_leadsByUser, df_appointments, df_leadsByUser_complete_month = load_data(start_date, end_date)

            if df_leadsByUser.empty or df_appointments.empty or df_leadsByUser_complete_month.empty:
                st.warning("Não foram encontrados dados para o período selecionado.")

            else:
                df_leadsByUser = df_leadsByUser[leadsByUserColumns]
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
                pro_corpo_stores['Unidade'] = pro_corpo_stores['Unidade'].apply(normalize_name)
                
                atendentes_puxadas_manha, atendentes_puxadas_tarde = get_atendente_from_spreadsheet()
                atendentes_puxadas_total = pd.concat([atendentes_puxadas_manha, atendentes_puxadas_tarde])

                atendentes_puxadas_total['Atendente'] = atendentes_puxadas_total['Atendente'].apply(normalize_name)
                atendentes_puxadas_total['Unidade'] = atendentes_puxadas_total['Unidade'].apply(normalize_name)
                df_leadsByUser['Atendente'] = df_leadsByUser['Atendente'].apply(normalize_name)

                # Step 1: filter leadsByUser where 'Atendente' matches the 'Atendente' in atendentes_puxadas_total 
                df_leadsByUser_total = df_leadsByUser[df_leadsByUser['Atendente'].isin(atendentes_puxadas_total['Atendente'])]

                # Step 2: merge to enrich with additional info (like Unidade, Turno, Tam)
                df_leadsByUser_total = df_leadsByUser_total.merge(
                    atendentes_puxadas_total,
                    how='left',
                    left_on='Atendente',
                    right_on='Atendente'
                )
                # Extract Attendants from the pre-defined lists
                df_leadsByUser_total = df_leadsByUser_total[leadsByUser_display_columns]
                                
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
                
                df_appointments_atendentes = df_appointments_agendamentos_filtered_coc_rules[
                    df_appointments_agendamentos_filtered_coc_rules['Nome da primeira atendente']
                    .isin(atendentes_puxadas_total['Atendente'])]
                df_appointments_atendentes_grouped = df_appointments_atendentes.groupby('Nome da primeira atendente').agg({'ID agendamento': 'nunique'}).reset_index()
                
                # Merging Puxadas with Appointments + Final Touch
                df_leadsByUser_and_appointments = pd.merge(
                    df_leadsByUser_total,
                    df_appointments_atendentes_grouped,
                    left_on='Atendente',
                    right_on='Nome da primeira atendente',
                    how='left'
                )
                df_leadsByUser_and_appointments = df_leadsByUser_and_appointments.drop(columns=['Nome da primeira atendente'])
                df_leadsByUser_and_appointments = df_leadsByUser_and_appointments.fillna(0)
                df_leadsByUser_and_appointments = df_leadsByUser_and_appointments.rename(columns={'ID agendamento': 'Agendamentos na Agenda'})

                df_leadsByUser_and_appointments_totals = append_total_rows_leadsByStore(df_leadsByUser_and_appointments)
                
                # Grouping by "Unidade"
                df_leadsByStore_and_appointments_totals = df_leadsByUser_and_appointments_totals.groupby('Unidade').agg({
                    'Leads Puxados': 'sum',
                    'Agendamentos por lead': 'sum',
                    'Agendamentos na Agenda': 'sum',
                    'Atendente': 'count',
                    'Tam': 'first'
                }).reset_index()
                
                df_leadsByStore_and_appointments_totals_P = df_leadsByStore_and_appointments_totals[df_leadsByStore_and_appointments_totals['Tam'] == 'P']
                df_leadsByStore_and_appointments_totals_M = df_leadsByStore_and_appointments_totals[df_leadsByStore_and_appointments_totals['Tam'] == 'M']
                df_leadsByStore_and_appointments_totals_G = df_leadsByStore_and_appointments_totals[df_leadsByStore_and_appointments_totals['Tam'] == 'G']
                
                # st.subheader("Leads e Agendamentos por Loja")
                # st.dataframe(
                #     apply_formatting_leadsByStore(df_leadsByStore_and_appointments_totals),
                #     hide_index=True,
                #     height=len(df_leadsByStore_and_appointments_totals)* 45, 
                #     )
                
                st.subheader("Leads e Agendamentos por Loja (P)")
                st.dataframe(
                    apply_formatting_leadsByStore(df_leadsByStore_and_appointments_totals_P),
                    hide_index=True,
                    )
                
                st.subheader("Leads e Agendamentos por Loja (M)")
                st.dataframe(
                    apply_formatting_leadsByStore(df_leadsByStore_and_appointments_totals_M),
                    hide_index=True,
                    )
                
                st.subheader("Leads e Agendamentos por Loja (G)")
                st.dataframe(
                    apply_formatting_leadsByStore(df_leadsByStore_and_appointments_totals_G),
                    hide_index=True,
                    )
                
                df_leadsByUser_complete_month = df_leadsByUser_complete_month[leadsByUserColumns]
                df_leadsByUser_complete_month['agendamentos_por_lead'] = 0
                df_leadsByUser_complete_month['agendamentos_por_lead'] = df_leadsByUser_complete_month['messages_count_by_status'].apply(extract_agendamentos)
                df_leadsByUser_complete_month = df_leadsByUser_complete_month.rename(columns={ 
                    'name': 'AtendenteCRM',
                    'messages_count': 'Leads Puxados',
                    'unique_messages_count': 'Leads Puxados (únicos)',
                    'agendamentos_por_lead': 'Agendamentos por lead',
                    'local': 'UnidadeCRM',
                    'Tam': 'Tam',
                    'turno': 'Turno',
                    'success_rate': 'Conversão'
                })
                df_leadsByUser_complete_month = df_leadsByUser_complete_month.reset_index(drop=True)
                df_leadsByUser_complete_month = df_leadsByUser_complete_month.sort_values(by='Leads Puxados', ascending=False)

                # (store_info = Unidade, Turno, Tam)
                df_leadsByUser_complete_month_with_store_info = pd.merge(
                    df_leadsByUser_complete_month,
                    atendentes_puxadas_total,
                    left_on='AtendenteCRM',
                    right_on='Atendente',
                    how='left'
                ) # extra merge to deal with other users COC wants to track which are not "Atendente"
                
                st.subheader("DEBUGGING df_leadsByUser_complete_month + store_info")
                st.dataframe(df_leadsByUser_complete_month_with_store_info)

                desired_columns = ['Unidade', 'Leads Puxados', 'Agendamentos por lead', 'Tam']
                df_leadsByUser_complete_month_with_appointments_reduced = df_leadsByUser_complete_month_with_store_info[desired_columns]
                
                df_leadsByUser_complete_month_with_appointments_groupedByStore = df_leadsByUser_complete_month_with_appointments_reduced.groupby('Unidade').agg({
                    # needs to sum / running days which are not sunday
                    'Leads Puxados': 'mean', 
                    'Agendamentos por lead': 'mean' 
                }).reset_index()

                st.subheader("DEBUGGING df_leadsByUser_complete_month_with_appointments grouped by store")
                df_leadsByUser_complete_month_with_appointments_groupedByStore = df_leadsByUser_complete_month_with_appointments_groupedByStore.rename(columns={
                    'Leads Puxados': 'Leads Puxados (média do mês)',
                    'Agendamentos por lead': 'Agendamentos por lead (média do mês)'
                })
                st.dataframe(df_leadsByUser_complete_month_with_appointments_groupedByStore)

                # merge this info back to df_leadsByStore_and_appointments_totals
                df_leadsByStore_and_appointments_totals = pd.merge(
                    df_leadsByStore_and_appointments_totals,
                    df_leadsByUser_complete_month_with_appointments_groupedByStore,
                    on='Unidade',
                    how='left'
                )                

                st.subheader("DEBUGGING df_leadsByStore_and_appointments_totals")
                df_leadsByStore_and_appointments_totals = df_leadsByStore_and_appointments_totals.rename(columns={
                    'Atendente' : 'Recepcionistas'
                })
                df_leadsByStore_and_appointments_totals = df_leadsByStore_and_appointments_totals[
                    [
                        'Unidade',
                        'Recepcionistas',
                        'Leads Puxados',
                        'Leads Puxados (média do mês)',
                        'Agendamentos por lead',
                        'Agendamentos por lead (média do mês)',
                        'Tam'
                    ]
                ]
                st.dataframe(df_leadsByStore_and_appointments_totals)

                df_leadsByStore_and_appointments_totals_stores_p = df_leadsByStore_and_appointments_totals[df_leadsByStore_and_appointments_totals['Tam'] == 'P']
                df_leadsByStore_and_appointments_totals_stores_m = df_leadsByStore_and_appointments_totals[df_leadsByStore_and_appointments_totals['Tam'] == 'M']
                df_leadsByStore_and_appointments_totals_stores_g = df_leadsByStore_and_appointments_totals[df_leadsByStore_and_appointments_totals['Tam'] == 'G']

                st.subheader("DEBUGGING df_leadsByStore_and_appointments_totals_stores_p")
                st.dataframe(df_leadsByStore_and_appointments_totals_stores_p)

                st.subheader("DEBUGGING df_leadsByStore_and_appointments_totals_stores_m")
                st.dataframe(df_leadsByStore_and_appointments_totals_stores_m)

                st.subheader("DEBUGGING df_leadsByStore_and_appointments_totals_stores_g")
                st.dataframe(df_leadsByStore_and_appointments_totals_stores_g)

