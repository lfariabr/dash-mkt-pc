"""
worker.py
responsible for doing the hard work of checking if leads are appointments and if leads are sales.
"""

import re
import pandas as pd
from datetime import datetime

from helpers.cleaner import clean_telephone
from data.procedures import procedimentos_que_vamos_olhar
from data.procedures import aesthetic_procedures_aval
from views.appointments.appointment_columns import appointments_clean_columns
from views.appointments.appointment_cleaner import ( 
    filter_relevant_appointments_to_mkt,
    filter_appointments_aval_comparecimentos, 
    filter_appointments_aval_agendamentos)
from views.sales.sale_columns import colunas_reduzido
from views.sales.sales_cleaner import filter_relevant_sales_to_mkt
from views.leads.leads_cleaner import filter_relevant_leads_to_mkt

def clean_lead_df(df_leads):

    df_leads = filter_relevant_leads_to_mkt(df_leads)

    return df_leads

def clean_agd_df(df_agd):

    df_agd = filter_relevant_appointments_to_mkt(df_agd)

    return df_agd

def clean_sales_df(df_sales):

    df_sales = filter_relevant_sales_to_mkt(df_sales)

    return df_sales

def check_if_lead_has_atendido_status(df_leads_cleaned, df_appointments_comparecimentos):
    """
    Function to check if a lead has an appointment with a status of 'Atendido'
    and add new columns in the df_leads coming from df_appointments_comparecimentos
    """
    # Create copies to avoid modifying original dataframes
    df_leads_cleaned = df_leads_cleaned.copy()
    df_appointments_comparecimentos = df_appointments_comparecimentos.copy()

    # Since Leads and Appointments are cleaned, we can check which leads are appointments_comparecimentos
    # TODO @apt_cleaner
    def eh_comparecimento(row):
        telefone = row['Telefone do lead']
        email = row['Email do lead']

        # Find matches in df_appointments_comparecimentos
        match = df_appointments_comparecimentos[
            (df_appointments_comparecimentos['Telefones Limpos'].apply(lambda x: telefone in x)) |
            (df_appointments_comparecimentos['Email'] == email)
        ]

        if not match.empty:
            data = match.iloc[0]['Data']
            procedimento = match.iloc[0]['Procedimento']
            status = match.iloc[0]['Status']
            unidade = match.iloc[0]['Unidade do agendamento']

            return pd.Series({
                'data_agenda': data,
                'procedimento': procedimento,
                'status': status,
                'unidade': unidade
            })

        return pd.Series({'data_agenda': None, 'procedimento': None, 'status': None, 'unidade': None})

    # Apply function to df_leads_cleaned
    df_leads_cleaned[['data_agenda', 'procedimento', 'status', 'unidade']] = df_leads_cleaned.apply(eh_comparecimento, axis=1)

    return df_leads_cleaned
    
def check_if_lead_has_other_status(df_leads_nao_atendidos, df_appointments_agendamentos):
    """
    Function to check if a lead has an appointment with a status of 'Agendado'
    """
    # Create copies to avoid modifying original dataframes
    df_leads_nao_atendidos = df_leads_nao_atendidos.copy()
    df_appointments_agendamentos = df_appointments_agendamentos.copy()

    # TODO @apt_cleaner
    def eh_agendamento(row):
        telefone = row['Telefone do lead']
        email = row['Email do lead']

        # Find matches in df_appointments_agendamentos
        match = df_appointments_agendamentos[
            (df_appointments_agendamentos['Telefones Limpos'].apply(lambda x: telefone in x)) |
            (df_appointments_agendamentos['Email'] == email)
        ]

        if not match.empty:
            data = match.iloc[0]['Data']
            procedimento = match.iloc[0]['Procedimento']
            status = match.iloc[0]['Status']
            unidade = match.iloc[0]['Unidade do agendamento']

            return pd.Series({
                'data_agenda': data,
                'procedimento': procedimento,
                'status': status,
                'unidade': unidade
            })

        return pd.Series({'data_agenda': None, 'procedimento': None, 'status': None, 'unidade': None})

    # Apply function to df_leads_nao_atendidos
    df_leads_nao_atendidos[['data_agenda', 'procedimento', 'status', 'unidade']] = df_leads_nao_atendidos.apply(eh_agendamento, axis=1)

    return df_leads_nao_atendidos

# tetntar concecntrar em um unico arquivo
    

