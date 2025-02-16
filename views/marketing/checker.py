import pandas as pd
import re
from datetime import datetime

from helpers.cleaner import clean_telephone
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

    # Filter for only relevant procedures
    procedimentos_que_vamos_olhar = [
        'AVALIAÇÃO TATUAGEM',
        'AVALIAÇÃO INJETÁVEIS E INVASIVOS',
        'AVALIAÇÃO ESTÉTICA',
        'AVALIAÇÃO DEPILAÇÃO',
        'AVALIAÇÃO MEDICINA ESTÉTICA'
    ]
    
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

            return pd.Series({
                'data_agenda': data,
                'proced_avaliação': proced_avaliação,
                'agendamento': agendamento,
                'comparecimento': comparecimento,
                'procedimento': procedimento,
                'status': status
            })

        return pd.Series({'data_agenda': None, 'proced_avaliação': None, 'agendamento': None, 'comparecimento': None, 'procedimento': None, 'status': None})

    # Apply function to leads
    leads[['data_agenda', 'proced_avaliação', 'agendamento', 'comparecimento', 'procedimento', 'status']] = leads.apply(eh_comparecimento, axis=1)

    return leads

def check_if_lead_has_other_status(df_leads, df_appointments_agendamentos):
    """
    Function to check if a lead has an appointment with a status other than 'Atendido'
    Only checks leads where agendamento is False from previous check
    """
    # Create copies to avoid modifying original dataframes
    leads = df_leads.copy()
    agendamentos = df_appointments_agendamentos.copy()
    
    # Only process leads that don't have an 'Atendido' status
    # Use boolean indexing with isnull or equals False
    leads_to_check = leads[leads['agendamento'].isnull() | (leads['agendamento'] == False)].copy()
    
    # Pre-process agendamentos data
    agendamentos['Telefone'] = agendamentos['Telefone'].fillna('Cliente sem telefone')
    agendamentos['Email'] = agendamentos['Email'].fillna('NA')
    agendamentos['Telefone'] = agendamentos['Telefone'].astype(str)
    
    # Create clean phone numbers list
    agendamentos['Telefones Limpos'] = agendamentos['Telefone'].apply(
        lambda x: [clean_telephone(num) for num in str(x).split('/')]
    )
    
    agendamentos.loc[:, 'proced_avaliação'] = agendamentos['Procedimento'].isin(aesthetic_procedures_aval)
    agendamentos.loc[:, 'agendamento'] = True
    agendamentos.loc[:, 'comparecimento'] = False  # Since this is for non-'Atendido' status
    
    # Explode phone numbers into separate rows
    agendamentos_exploded = explode_phone_numbers(agendamentos)
    
    # Merge on email
    other_merge_email = pd.merge(
        leads_to_check,
        agendamentos_exploded,
        left_on='Email do lead',
        right_on='Email',
        how='left',
        suffixes=('', '_agend')
    )
    
    # Merge on phone
    other_merge_phone = pd.merge(
        leads_to_check,
        agendamentos_exploded,
        left_on='Telefone do lead',
        right_on='Telefones Limpos',
        how='left',
        suffixes=('', '_agend')
    )
    
    # Combine results and drop duplicates
    other_results = pd.concat([other_merge_email, other_merge_phone]).drop_duplicates(subset=['ID do lead'])
    
    # Update the original leads DataFrame only for the rows we checked
    mask = leads['ID do lead'].isin(other_results['ID do lead'])
    leads.loc[mask, 'data_agenda'] = other_results['Data'].fillna(pd.NA)
    leads.loc[mask, 'procedimento'] = other_results['Procedimento'].fillna(pd.NA)
    leads.loc[mask, 'store'] = other_results['Unidade do agendamento'].fillna(pd.NA)
    leads.loc[mask, 'agendamento'] = other_results['agendamento'].fillna(False)
    leads.loc[mask, 'comparecimento'] = other_results['comparecimento'].fillna(False)
    leads.loc[mask, 'status_agendamento'] = other_results['Status_agend'].fillna(pd.NA)
    
    return leads

def check_appointments_status(df_leads, df_appointments_comparecimentos, df_appointments_agendamentos):
    """
    Main function to check all appointment statuses for leads
    """
    # Pre-process leads data
    leads = df_leads.copy()
    leads['Telefone do lead'] = leads['Telefone do lead'].fillna('Cliente sem telefone')
    leads['Email do lead'] = leads['Email do lead'].fillna('None')
    leads['Telefone do lead'] = leads['Telefone do lead'].astype(str)
    leads['Telefone do lead'] = leads['Telefone do lead'].apply(clean_telephone)
    
    # First check for 'Atendido' status
    leads = check_if_lead_has_atendido_status(leads, df_appointments_comparecimentos)
    
    # Then check for other statuses
    # leads = check_if_lead_has_other_status(leads, df_appointments_agendamentos)
    
    return leads