def get_leads_por_dia(df_leads):
    """Group leads by day."""
    return (
        df_leads.groupby('Dia')
        .agg({'ID do lead': 'nunique'})
        .reset_index()
    )

def get_leads_por_unidade(df_leads):
    """Group leads by unit."""
    return (
        df_leads.groupby('Unidade')
        .agg({'ID do lead': 'nunique'})
        .reset_index()
    )

def get_leads_por_fonte(df_leads):
    """Group leads by source."""
    return (
        df_leads.groupby('Fonte')
        .agg({'ID do lead': 'nunique'})
        .reset_index()
    )

def get_leads_por_status(df_leads):
    """Group leads by status."""
    return (
        df_leads.groupby('Status')
        .agg({'ID do lead': 'nunique'})
        .reset_index()
    )