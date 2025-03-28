import pandas as pd

def groupby_marketing_by_category(df_leads_by_category):
    """Group marketing by category."""
    return (
        df_leads_by_category
        .groupby(['Categoria', 'Unidade do lead'])
        .agg({'ID lead': 'count'})
        .reset_index()
    )

def groupby_marketing_by_source(df_leads_with_purchases):
    return (
        df_leads_with_purchases
        .groupby(['Fonte', 'Unidade do lead'])
        .agg({'ID lead': 'count'})
        .reset_index()
    )

def groupby_marketing_by_category_and_comprou(df_leads_with_purchases):
    return (
        df_leads_with_purchases[
            df_leads_with_purchases['comprou'] == True].groupby(
                ['Categoria', 'comprou'
                    ]).agg(
                        {'ID lead': 'count',
                        'Valor primeiro orçamento': 'sum'
                        }
                    )
    )

def groupby_marketing_by_source_and_comprou(df_leads_with_purchases):
    return (
        df_leads_with_purchases[
            df_leads_with_purchases['comprou'] == True].groupby(
                ['Fonte', 'comprou'
                    ]).agg(
                        {'ID lead': 'count',
                        'Valor primeiro orçamento': 'sum'
                        }
                    )
    )

def pivot_table_marketing_by_category_and_comprou(df_leads_by_category_and_comprou):
    return (
        df_leads_by_category_and_comprou
        .pivot_table(
            index='Categoria',
            columns='comprou',
            values=[
                'ID lead',
                'Valor primeiro orçamento'
            ]
        )
    )
    
def pivot_table_marketing_by_source_and_comprou(df_leads_by_source_and_comprou):
    return (
        df_leads_by_source_and_comprou
        .pivot_table(
            index='Fonte',
            columns='comprou',
            values=[
                'ID lead',
                'Valor primeiro orçamento'
            ]
        )
    )