# Receive create_date and return day, month and year
import pandas as pd

def transform_date_from_leads(df):
    """
    Treat data as a date and extract day, month and year.
    
    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame with a 'create_date' column.
    
    Returns
    -------
    pandas.DataFrame
        DataFrame with 'Dia', 'Mês' and 'Dia da Semana' columns.
    """
    df['Dia da entrada'] = pd.to_datetime(df['Dia da entrada'])
    df['Dia'] = df['Dia da entrada'].dt.day
    df['Mês'] = df['Dia da entrada'].dt.month
    df['Dia da Semana'] = df['Dia da entrada'].dt.day_name()
    return df

def transform_date_from_sales(df):
    """
    Treat data as a date and extract day, month and year.
    
    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame with a 'create_date' column.
    
    Returns
    -------
    pandas.DataFrame
        DataFrame with 'Dia', 'Mês' and 'Dia da Semana' columns.
    """
    df['Data venda'] = pd.to_datetime(df['Data venda'])
    df['Dia'] = df['Data venda'].dt.day
    df['Mês'] = df['Data venda'].dt.month
    df['Dia da Semana'] = df['Data venda'].dt.day_name()
    return df