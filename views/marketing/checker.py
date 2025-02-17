import pandas as pd
import re
from datetime import datetime

from helpers.cleaner import clean_telephone
from data.procedures import procedimentos_que_vamos_olhar
from data.procedures import aesthetic_procedures_aval

def explode_phone_numbers(df, phone_col='Telefones Limpos'):
    """
    Explode a dataframe with list of phone numbers into multiple rows
    """
    df_copy = df.copy()
    if df_copy[phone_col].dtype == 'object' and isinstance(df_copy[phone_col].iloc[0], list):
        return df_copy.explode(phone_col)
    return df_copy

def check_if_lead_has_atendido_status(df_leads, df_appointments_comparecimentos):
    """
    Function to check if a lead has an appointment with a status of 'Atendido'
    and add new columns in the df_leads coming from df_appointments_comparecimentos
    """
    # Create copies to avoid modifying original dataframes
    leads = df_leads.copy()
    df_agd = df_appointments_comparecimentos.copy()
    
    # Classify appointments
    df_agd["proced_avaliação"] = df_agd['Procedimento'].str.contains(r'.*AVALIAÇÃO.*', case=False)
    df_agd["agendamento"] = df_agd['Status'].str.contains(r'.*Atendido|Falta.*', case=False)
    df_agd["comparecimento"] = df_agd['Status'].str.contains(r'.*Atendido.*', case=False)
    df_agd["esta agendado?"] = df_agd['Status'].str.contains("Agendado", case=False)
    df_agd["falta ou cancelado?"] = df_agd['Status'].str.contains(r'.*Falta|Cancelado.*', case=False)

    # Filter for relevant appointments
    df_agendamentos_aval = df_agd[(df_agd['proced_avaliação'] == True) & (df_agd['agendamento'] == True)]
    df_comparecimentos_aval = df_agd[(df_agd['proced_avaliação'] == True) & (df_agd['comparecimento'] == True)]

    
    df_agendamentos_aval = df_agendamentos_aval[df_agendamentos_aval['Procedimento'].isin(procedimentos_que_vamos_olhar)]
    df_comparecimentos_aval = df_comparecimentos_aval[df_comparecimentos_aval['Procedimento'].isin(procedimentos_que_vamos_olhar)]

    # Data processing
    df_comparecimentos_aval['Telefone'] = df_comparecimentos_aval['Telefone'].fillna('Cliente sem telefone')
    df_comparecimentos_aval['Email'] = df_comparecimentos_aval['Email'].fillna('NA')
    df_comparecimentos_aval['Telefone'] = df_comparecimentos_aval['Telefone'].astype(str)

    # Create clean phone numbers list
    df_comparecimentos_aval['Telefones Limpos'] = df_comparecimentos_aval['Telefone'].apply(
        lambda x: [clean_telephone(num) for num in str(x).split('/')]
    )

    # Convert phone and email lists to sets for fast lookup
    telefones_agendados = set(num for sublist in df_comparecimentos_aval['Telefones Limpos'] for num in sublist)
    emails_agendados = set(df_comparecimentos_aval['Email'])

    # Function to check if a lead has an appointment
    def eh_comparecimento(row):
        telefone = row['Telefone do lead']
        email = row['Email do lead']

        # Find matches in df_comparecimentos_aval
        match = df_comparecimentos_aval[
            (df_comparecimentos_aval['Telefones Limpos'].apply(lambda x: telefone in x)) |
            (df_comparecimentos_aval['Email'] == email)
        ]

        if not match.empty:
            data = match.iloc[0]['Data']
            proced_avaliação = match.iloc[0]['proced_avaliação']
            agendamento = match.iloc[0]['agendamento']
            comparecimento = match.iloc[0]['comparecimento']
            procedimento = match.iloc[0]['Procedimento']
            status = match.iloc[0]['Status']
            unidade = match.iloc[0]['Unidade do agendamento']

            return pd.Series({
                'data_agenda': data,
                'proced_avaliação': proced_avaliação,
                'agendamento': agendamento,
                'comparecimento': comparecimento,
                'procedimento': procedimento,
                'status': status,
                'unidade': unidade
            })

        return pd.Series({'data_agenda': None, 'proced_avaliação': None, 'agendamento': None, 'comparecimento': None, 'procedimento': None, 'status': None, 'unidade': None})

    # Apply function to leads
    leads[['data_agenda', 'proced_avaliação', 'agendamento', 'comparecimento', 'procedimento', 'status', 'unidade']] = leads.apply(eh_comparecimento, axis=1)

    return leads

def check_if_lead_has_other_status(df_leads_que_nao_compareceram, df_appointments_agendamentos):
    """
    Function to check if a lead has an appointment with a status other than 'Atendido'
    Only checks leads where agendamento is False from previous check
    """
    # Create copies to avoid modifying original dataframes
    leads = df_leads_que_nao_compareceram.copy()
    agendamentos = df_appointments_agendamentos.copy()
    
    # Limpar telefones no df_agd, incluindo separar múltiplos telefones
    agendamentos['Telefone'] = agendamentos['Telefone'].fillna('Cliente sem telefone')
    agendamentos['Telefone'] = agendamentos['Telefone'].astype(str)
    agendamentos['Telefones Limpos'] = agendamentos['Telefone'].apply(lambda x: [clean_telephone(num) for num in str(x).split('/')])

    # Verificação de status
    agendamentos["proced_avaliação"] = agendamentos['Procedimento'].str.contains(r'.*AVALIAÇÃO.*', case=False)
    agendamentos["falta ou cancelado?"] = agendamentos['Status'].str.contains(r'.*Falta|Cancelado.*', case=False, na=False)
    agendamentos["esta agendado?"] = agendamentos['Status'].str.contains(r'(?<!Re)Agendado', case=False, na=False)

    # Filtrar os leads que estão agendados ou falta/cancelado
    df_agd_filtrado = agendamentos[(agendamentos["esta agendado?"] == True) | (agendamentos["falta ou cancelado?"] == True)]
    df_agd_filtrado = df_agd_filtrado.loc[df_agd_filtrado['Procedimento'].isin(procedimentos_que_vamos_olhar)]

    # Função para verificar se o lead está no df_agd_filtrado e trazer as informações adicionais
    def verificar_status(row):
        telefone = row['Telefone do lead']
        email = row['Email do lead']
        match = df_agd_filtrado[(df_agd_filtrado['Telefones Limpos'].apply(lambda x: telefone in x)) | (df_agd_filtrado['Email'] == email)]

        if not match.empty:
            data = match.iloc[0]['Data']
            proced_avaliação = match.iloc[0]['proced_avaliação']
            agendamento = match.iloc[0]['esta agendado?']
            comparecimento = match.iloc[0]['falta ou cancelado?']
            status = match.iloc[0]['Status']
            procedimento = match.iloc[0]['Procedimento']
            unidade = match.iloc[0]['Unidade do agendamento']

            return pd.Series({
                'data_agenda_novo': data,
                'proced_avaliação_novo': proced_avaliação,
                'agendamento_novo': agendamento,
                'comparecimento_novo': comparecimento,
                'status_novo': status,
                'procedimento_novo': procedimento,
                'unidade_novo': unidade
            })

        return pd.Series({
            'data_agenda_novo': None,
            'proced_avaliação_novo': None,
            'agendamento_novo': None,
            'comparecimento_novo': None,
            'status_novo': None,
            'procedimento_novo': None,
            'unidade_novo': None
        })

    # Aplicar a função aos leads
    leads[['data_agenda_novo', 'proced_avaliação_novo', 'agendamento_novo', 'comparecimento_novo', 'status_novo', 'procedimento_novo', 'unidade_novo']] = leads.apply(verificar_status, axis=1)
    
    return leads

def check_appointments_status(df_leads, df_appointments_comparecimentos, df_appointments_agendamentos):
    """
    Main function to check all appointment statuses for leads in phases:
    1. First check for 'Atendido' status
    2. Then check other statuses for remaining leads
    """
    # Pre-process leads data
    leads = df_leads.copy()
    leads['Telefone do lead'] = leads['Telefone do lead'].fillna('Cliente sem telefone')
    leads['Email do lead'] = leads['Email do lead'].fillna('None')
    leads['Telefone do lead'] = leads['Telefone do lead'].astype(str)
    leads['Telefone do lead'] = leads['Telefone do lead'].apply(clean_telephone)
    
    # Phase 1: Check for 'Atendido' status
    leads = check_if_lead_has_atendido_status(leads, df_appointments_comparecimentos)
    
    # Fill NaN values in comparecimento column
    leads['comparecimento'] = leads['comparecimento'].fillna('Não compareceu')
    
    # Phase 2: For leads that weren't 'Atendido', check other statuses
    leads_not_atendido = leads[leads['comparecimento'] == 'Não compareceu'].copy()
    leads_not_atendido = check_if_lead_has_other_status(leads_not_atendido, df_appointments_agendamentos)
    
    # Update the original dataframe with results from other status checks
    leads.update(leads_not_atendido)
    
    return leads