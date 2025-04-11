import pandas as pd

def groupby_sales_por_dia(df_sales):
    """Group sales by day."""
    return (
        df_sales
        .groupby('Dia')
        .agg({'Valor líquido': 'sum'})
        .reset_index()
    )

def groupby_sales_por_unidade(df_sales):
    """Group sales by unit."""
    return (
        df_sales
        .groupby(['Dia', 'Unidade'])
        .agg({'Valor líquido': 'sum'})
        .reset_index()
        .fillna(0)
    )

def groupby_sales_por_profissao(df_sales):
    """Group sales by profession."""
    return (
        df_sales
        .groupby('Profissão cliente')
        .agg({'Valor líquido': 'sum'})
        .reset_index()
        .sort_values('Valor líquido', ascending=False)
        .head(10) # top 10
    )
    
def groupby_sales_por_vendedoras(df_sales):
    """Group sales by salesman."""
    return (
        df_sales
        .groupby('Consultor')
        .agg({'Valor líquido': 'sum'})
        .reset_index()
        .sort_values('Valor líquido', ascending=False)
        .head(10) # top 10
    )

def groupby_sales_por_procedimento(df_sales):
    """Group sales by procedure."""
    return (
        df_sales
        .groupby('bill_items')
        .agg({'Valor líquido': 'sum'})
        .reset_index()
        .sort_values('Valor líquido', ascending=False)
        .head(10) # top 10
    )