import pandas as pd
from .appointment_columns import avaliacao_procedures, appointments_clean_columns
from helpers.cleaner import clean_telephone

def filter_relevant_appointments_to_mkt(df_agd):
    """
    Add relevant columns so we're able to apply further filtering for data vis.
    """
    # Create new boolean columns based on conditions
    df_agd['eh_avaliacao'] = df_agd['Procedimento'].str.contains(r'AVALIAÇÃO', case=False, na=False)
    df_agd['eh_agendamento'] = df_agd['Status'].str.contains(r'Atendido|Falta', case=False, na=False)
    df_agd['eh_comparecimento'] = df_agd['Status'].str.contains(r'Atendido', case=False, na=False)
    df_agd['eh_agendado'] = df_agd['Status'].str.contains(r'Agendado', case=False, na=False)
    df_agd['eh_falta_ou_cancelado'] = df_agd['Status'].str.contains(r'Falta|Cancelado', case=False, na=False)

    # Filter for relevant procedures
    df_agd = filter_appointments_that_are_avaliacao(df_agd)
    
    # Filter for relevant appointments
    df_agd = df_agd[appointments_clean_columns]

    df_agd['Telefone'] = df_agd['Telefone'].fillna('Cliente sem telefone')
    df_agd['Telefone'] = df_agd['Telefone'].astype(str)
    df_agd['Telefones Limpos'] = df_agd['Telefone'].apply(clean_telephone)

    return df_agd

def filter_appointments_aval_comparecimentos(df_agd):
    """
    Filter appointments that have status = Atendido in AVALIAÇÃO
    """
    
    df_agd = df_agd.loc[df_agd['proced_avaliação'] == True]
    df_agd = df_agd.loc[df_agd['agendamento'] == True]
    df_agd = df_agd.loc[df_agd['comparecimento'] == True]

    return df_agd

def filter_appointments_aval_agendamentos(df_agd):
    """
    Filter appointments that have status = Agendado in AVALIAÇÃO
    """
    
    df_agd = df_agd.loc[df_agd['proced_avaliação'] == True]
    df_agd = df_agd.loc[df_agd['comparecimento'] == True]

    return df_agd

def filter_appointments_that_are_avaliacao(df_agd):
    """
    Filter appointments that are in AVALIAÇÃO
    """
    df_agd = df_agd.loc[df_agd['Procedimento'].isin(avaliacao_procedures)]
    
    return df_agd

def clean_phone_numbers(df_agd):
    """
    Clean phone numbers
    """
    df_agd['Telefone'] = df_agd['Telefone'].fillna('Cliente sem telefone')
    df_agd['Telefone'] = df_agd['Telefone'].astype(str)
    df_agd['Telefones Limpos'] = df_agd['Telefone'].apply(clean_telephone)

    return df_agd