import pandas as pd

def groupby_agendamentos_por_dia(df_agendamentos):
    """Group appointments by day."""
    return (
        df_agendamentos
        .groupby('Data')
        .agg({'ID agendamento': 'nunique'})
        .reset_index()
    )

def groupby_agendamentos_por_unidade(df_agendamentos):
    """Group appointments by unit."""
    return (
        df_agendamentos
        .groupby('Unidade do agendamento')
        .agg({'ID agendamento': 'nunique'})
        .reset_index()
    )

def groupby_comparecimentos_por_dia(df_agendamentos):
    """Group comparecimentos by day."""
    return (
        df_agendamentos
        .groupby('Data')
        .agg({'ID agendamento': 'nunique'})
        .reset_index()
    )

def groupby_comparecimentos_por_unidade(df_agendamentos):
    """Group comparecimentos by unit."""
    return (
        df_agendamentos
        .groupby('Unidade do agendamento')
        .agg({'ID agendamento': 'nunique'})
        .reset_index()
    )

def groupby_agendamentos_por_dia_pivoted(df_agendamentos):
    """Group appointments by day and pivot."""
    return (
        df_agendamentos
        .groupby(['Data', 'Unidade do agendamento'])
        .agg({'ID agendamento': 'nunique'})
        .reset_index()
        .pivot(index='Data', columns='Unidade do agendamento', values='ID agendamento')
        .fillna(0)
        .astype(int)
    )