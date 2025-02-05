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

# Transforming ['Content'] to str
df_leads['Content'] = df_leads['Content'].astype(str)
df_leads['Mensagem'] = df_leads['Mensagem'].astype(str)


# Função para categorizar com base nas palavras-chave
def categorize(text):
    for keyword, category in category_mapping.items():
        if pd.notna(text) and keyword.lower() in text.lower():  # Verifica se a palavra-chave está presente
            return category
    return 'Indefinido'  # Categoria padrão se não encontrar correspondência

# Passo 1: Aplicar a função de categorização na coluna 'Content'
df_leads['Categoria'] = df_leads['Content'].apply(categorize)

# Passo 2: Aplicar a função de categorização na coluna 'Mensagem', apenas para os 'Indefinidos'
df_leads.loc[df_leads['Categoria'] == 'Indefinido', 'Categoria'] = \
df_leads['Mensagem'].apply(categorize)

    # Extra categories
df_leads.loc[(df_leads['Fonte'] == 'Indique e Multiplique') & (df_leads['Categoria'] == 'Indefinido'), 'Categoria'] = 'Cortesia Indique'
df_leads.loc[(df_leads['Fonte'] == 'CRM BÔNUS') & (df_leads['Categoria'] == 'Indefinido'), 'Categoria'] = 'Cortesia CRM Bônus'
df_leads.loc[(df_leads['Mensagem'] == 'Lead Pop Up de Saída. Ganhou Peeling Diamante.') & (df_leads['Categoria'] == 'Indefinido'), 'Categoria'] = 'Cortesia PopUpSaida Peeling_D'
df_leads.loc[(df_leads['Mensagem'] == 'Lead Pop Up de Saída. Ganhou Massagem Modeladora.') & (df_leads['Categoria'] == 'Indefinido'), 'Categoria'] = 'Cortesia PopUpSaida Mass_Mod'
df_leads.loc[(df_leads['Mensagem'] == 'Lead salvo pelo modal de WhatsApp da Isa') & (df_leads['Categoria'] == 'Indefinido'), 'Categoria'] = 'Quer Falar no Whatsapp'
