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
    comparecimentos = df_appointments_comparecimentos.copy()
    
    # Pre-process comparecimentos data
    comparecimentos['Telefone'] = comparecimentos['Telefone'].fillna('Cliente sem telefone')
    comparecimentos['Email'] = comparecimentos['Email'].fillna('NA')
    comparecimentos['Telefone'] = comparecimentos['Telefone'].astype(str)
    
    # Create clean phone numbers list
    comparecimentos['Telefones Limpos'] = comparecimentos['Telefone'].apply(
        lambda x: [clean_telephone(num) for num in str(x).split('/')]
    )
    
    comparecimentos.loc[:, 'proced_avaliação'] = comparecimentos['Procedimento'].isin(aesthetic_procedures_aval)
    comparecimentos.loc[:, 'agendamento'] = True
    comparecimentos.loc[:, 'comparecimento'] = True  # Since this is only for 'Atendido' status
    
    # Explode phone numbers into separate rows
    comparecimentos_exploded = explode_phone_numbers(comparecimentos)
    
    # Merge on email
    served_merge_email = pd.merge(
        leads,
        comparecimentos_exploded,
        left_on='Email do lead',
        right_on='Email',
        how='left',
        suffixes=('', '_comp')
    )
    
    # Merge on phone
    served_merge_phone = pd.merge(
        leads,
        comparecimentos_exploded,
        left_on='Telefone do lead',
        right_on='Telefones Limpos',
        how='left',
        suffixes=('', '_comp')
    )
    
    # Combine results and drop duplicates
    served_results = pd.concat([served_merge_email, served_merge_phone]).drop_duplicates(subset=['ID do lead'])
    
    # Create result DataFrame
    result_df = leads.copy()
    
    # Add appointment columns
    result_df.loc[:, 'data_agenda'] = served_results['Data'].fillna(pd.NA)
    result_df.loc[:, 'procedimento'] = served_results['Procedimento'].fillna(pd.NA)
    result_df.loc[:, 'store'] = served_results['Unidade do agendamento'].fillna(pd.NA)
    result_df.loc[:, 'agendamento'] = served_results['agendamento'].fillna(False)
    result_df.loc[:, 'comparecimento'] = served_results['comparecimento'].fillna(False)
    result_df.loc[:, 'status_agendamento'] = served_results['Status_comp'].fillna(pd.NA)
    
    return result_df

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
    leads = check_if_lead_has_other_status(leads, df_appointments_agendamentos)
    
    return leads