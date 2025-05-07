import pandas as pd

def groupby_leads_por_dia(df_leads):
    """Group leads by day."""
    return (
        df_leads
        .groupby('Dia')                 
        .agg({'ID do lead': 'nunique'})  
        .reset_index()                   
)

def groupby_leads_por_unidade(df_leads):
    """Group leads by unit."""
    return (
        df_leads
        .groupby('Unidade')                 
        .agg({'ID do lead': 'nunique'})  
        .reset_index()                   
    )

def groupby_leads_por_fonte(df_leads):
    """Group leads by source."""
    return (
        df_leads
        .groupby('Fonte')
        .agg({'ID do lead': 'nunique'})
        .reset_index()
    )

def groupby_leads_por_status(df_leads):
    """Group leads by status."""
    return (
        df_leads
        .groupby('Status')
        .agg({'ID do lead': 'nunique'})
        .reset_index()
    )

def groupby_unidade_fonte_paga(df_leads_fontes_pagas):
    """Group paid leads by unit and source."""
    return (
        df_leads_fontes_pagas
        .groupby(['Unidade','Fonte'])
        .agg({
            'ID do lead': 'nunique',
            'Status': lambda x: (x == 'Convertido').mean() * 100
        })
        .round(2)
        .reset_index()
    )

def groupby_unidade_fonte_organica(df_leads_fontes_organicas):
    """Group organic leads by unit and source."""
    return (
        df_leads_fontes_organicas
        .groupby(['Unidade', 'Fonte'])
        .agg({
            'ID do lead': 'nunique',
            'Status': lambda x: (x == 'Convertido').mean() * 100
        })
        .round(2)
        .reset_index()
    )