# Dicionário de mapeamento: chave é a palavra-chave, valor é a categoria
category_mapping = {
    'Preenchimento': 'Preenchimento',
    'Botox': 'Botox',
    'Ultraformer': 'Ultraformer',
    'Enzimas': 'Enzimas',
    'Lavieen': 'Lavieen',
    'Sculptra': 'Bioestimulador',
    'Bioestimulador': 'Bioestimulador',
    'Institucional': 'Institucional',
    'Crio': 'Crio',
    'Limpeza': 'Limpeza',
    'olheiras': 'Preenchimento',
    'prolipo' : 'Enzimas',
    'Prolipo' : 'Enzimas',
    'rugas' : 'Botox'
}

def categorize(text):
    """Categorize text based on keywords."""
    if not isinstance(text, str):
        text = str(text)
    
    for keyword, category in category_mapping.items():
        if keyword.lower() in text.lower():
            return category
    return 'Indefinido'

def process_lead_categories(df_leads):
    """Process and categorize leads in the DataFrame."""
    # Ensure columns are string type
    df_leads['Content'] = df_leads['Content'].astype(str)
    df_leads['Mensagem'] = df_leads['Mensagem'].astype(str)
    
    # Categorize based on Content
    df_leads['Categoria'] = df_leads['Content'].apply(categorize)
    
    # Categorize based on Mensagem for Indefinido cases
    df_leads.loc[df_leads['Categoria'] == 'Indefinido', 'Categoria'] = \
        df_leads.loc[df_leads['Categoria'] == 'Indefinido', 'Mensagem'].apply(categorize)
    
    # Extra categories
    df_leads.loc[(df_leads['Fonte'] == 'Indique e Multiplique') & (df_leads['Categoria'] == 'Indefinido'), 'Categoria'] = 'Cortesia Indique'
    df_leads.loc[(df_leads['Fonte'] == 'CRM BÔNUS') & (df_leads['Categoria'] == 'Indefinido'), 'Categoria'] = 'Cortesia CRM Bônus'
    df_leads.loc[(df_leads['Mensagem'] == 'Lead Pop Up de Saída. Ganhou Peeling Diamante.') & (df_leads['Categoria'] == 'Indefinido'), 'Categoria'] = 'Cortesia PopUpSaida Peeling_D'
    df_leads.loc[(df_leads['Mensagem'] == 'Lead Pop Up de Saída. Ganhou Massagem Modeladora.') & (df_leads['Categoria'] == 'Indefinido'), 'Categoria'] = 'Cortesia PopUpSaida Mass_Mod'
    df_leads.loc[(df_leads['Mensagem'] == 'Lead salvo pelo modal de WhatsApp da Isa') & (df_leads['Categoria'] == 'Indefinido'), 'Categoria'] = 'Quer Falar no Whatsapp'
    
    return df_leads
