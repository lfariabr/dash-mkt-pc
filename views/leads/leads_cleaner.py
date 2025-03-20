import pandas as pd
from helpers.cleaner import clean_telephone
from .lead_columns import lead_clean_columns, undesired_stores

def filter_relevant_leads_to_mkt(df_leads):
    
    # Initial filtering
    df_leads = df_leads.loc[~df_leads['Unidade'].isin(undesired_stores)]

    # Tratamento de dados no df_leads
    df_leads['Telefone do lead'] = df_leads['Telefone do lead'].fillna('Cliente sem telefone')
    df_leads['Email do lead'] = df_leads['Email do lead'].fillna('None')
    df_leads['Telefone do lead'] = df_leads['Telefone do lead'].astype(str).apply(clean_telephone)

    # Columns
    df_leads = df_leads[lead_clean_columns]

    return df_leads