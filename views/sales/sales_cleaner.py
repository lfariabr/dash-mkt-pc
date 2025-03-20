import pandas as pd
from helpers.cleaner import clean_telephone
from .sale_columns import colunas_reduzido

def filter_relevant_sales_to_mkt(df_sales):
    df_sales = df_sales.loc[df_sales['Status'] == 'Finalizado']
    df_sales = df_sales.loc[df_sales['Consultor'] != 'BKO VENDAS']
    df_sales = df_sales.loc[df_sales['Unidade'] != 'PRAIA GRANDE']

    # Convert all values to strings, replace commas with dots, handle non-numeric values, and then convert to float
    df_sales['Valor líquido'] = pd.to_numeric(df_sales['Valor líquido'].astype(str).str.replace(',', '.'), errors='coerce')

    # df_sales.groupby('Unidade').agg({'Valor líquido': 'sum'})
    df_sales['Valor líquido'].sum()

    # df_sales treatment
    df_sales['Telefone(s) do cliente'] = df_sales['Telefone(s) do cliente'].fillna('Cliente sem telefone')
    df_sales['Email do cliente'] = df_sales['Email do cliente'].fillna('Cliente sem e-mail')
    df_sales['Telefone(s) do cliente'] = df_sales['Telefone(s) do cliente'].astype(str)
    df_sales['Telefones Limpos'] = df_sales['Telefone(s) do cliente'].apply(lambda x: [clean_telephone(num) for num in str(x).split('/')])

    # Apply reduced columns
    df_sales = df_sales[colunas_reduzido]

    return df_sales