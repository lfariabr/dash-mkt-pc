import pandas as pd
import re
from datetime import datetime
from helpers.cleaner import clean_telephone

def check_if_lead_has_purchased(df_leads_cleaned_final, df_sales):
    """
    # Merge the exploded DataFrame with df_leads
    df_leads_compras = pd.merge(
        df_leads_copy,
        df_vendas_teste,
        left_on='Telefone do lead',
        right_on='Telefones Limpos',
        how='left'
    )

    # Fill NaNs with 'Não é lead' for cases where no match is found
    df_leads_compras['Unidade_y'] = df_leads_compras['Unidade_y'].fillna('Não é lead')

    # Droping duplicated ID do lead
    df_leads_compras = df_leads_compras.drop_duplicates(subset='ID do lead', keep='first')
    df_leads_compras = df_leads_compras.drop_duplicates(subset='Email do lead', keep='first')
    df_leads_compras['Valor líquido'].sum()
    """
    leads = df_leads_cleaned_final.copy()
    sales = df_sales.copy()
    
    leads['Telefone do lead'] = leads['Telefone do lead'].fillna('Cliente sem telefone')
    leads['Telefone do lead'] = leads['Telefone do lead'].astype(str)
    leads['Telefone do lead'] = leads['Telefone do lead'].apply(clean_telephone)
    
    sales['Telefones Limpos'] = sales['Telefones Limpos'].fillna('Cliente sem telefone')
    sales['Telefones Limpos'] = sales['Telefones Limpos'].astype(str)
    sales['Telefones Limpos'] = sales['Telefones Limpos'].apply(clean_telephone)
    
    # Merge the exploded DataFrame with df_leads
    df_leads_compras = pd.merge(
        leads,
        sales,
        left_on='Telefone do lead',
        right_on='Telefones Limpos',
        how='left'
    )

    # Fill NaNs with 'Não é lead' for cases where no match is found
    df_leads_compras['Unidade_y'] = df_leads_compras['Unidade_y'].fillna('Não comprou')
    df_leads_compras['comprou'] = df_leads_compras['Unidade_y'] != 'Não comprou'

    # Droping duplicated ID do lead
    df_leads_compras = df_leads_compras.drop_duplicates(subset='ID do lead', keep='first')
    # df_leads_compras = df_leads_compras.drop_duplicates(subset='Email do lead', keep='first')
    # df_leads_compras['Valor líquido'].sum()
    
    return df_leads_compras
    