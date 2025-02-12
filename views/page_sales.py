import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
from datetime import datetime, timedelta
from data.stores import stores_to_remove
from data.date_intervals import available_periods, days_map
from components.headers import header_sales
from helpers.date import transform_date_from_sales
from views.sales.sales_grouper import (
                                        groupby_sales_por_dia,
                                        groupby_sales_por_unidade,
                                        groupby_sales_por_profissao,
                                        groupby_sales_por_vendedoras)

def load_data():
    """Load and preprocess sales data."""
    sales = 'db/sales.xlsx' #TODO

    df = pd.read_excel(sales)
    df = df.loc[~df['Unidade'].isin(stores_to_remove)]
    df = transform_date_from_sales(df)
    
    return df

def create_time_filtered_df(df, days=None):
    """Create a time-filtered dataframe."""
    if days:
        cutoff_date = datetime.now() - timedelta(days=days)
        return df[df['Dia da entrada'] >= cutoff_date]
    return df

def load_page_sales():
    """Main function to display sales data."""

    st.title("📊 2 - Sales")
    df_sales = load_data()

    st.sidebar.header("Filtros")
    time_filter = st.sidebar.selectbox(
        "Período", available_periods
    )
    if time_filter != "Todos os dados":
        df_sales = create_time_filtered_df(df_sales, days_map[time_filter])
    
    unidades = ["Todas"] + sorted(df_sales['Unidade'].unique().tolist())
    selected_store = st.sidebar.selectbox("Unidade", unidades)
    
    if selected_store != "Todas":
        df_sales = df_sales[df_sales['Unidade'] == selected_store]
    
    ########
    # Header
    header_sales(df_sales)

    # Tratativas especiais:
    df_sales = df_sales.loc[df_sales['Status'] == 'Finalizado']
    df_sales = df_sales.loc[df_sales['Consultor'] != 'BKO VENDAS']

    #####
    # Div 1: Vendas por Dia
    # TODO: Adicionar uma linha de tendência no gráfico
    groupby_vendas_por_dia = groupby_sales_por_dia(df_sales)

    grafico_vendas_por_dia = px.bar(
        groupby_vendas_por_dia,
        x='Dia',
        y='Valor líquido',
        title='Venda Diária',
        labels={'Valor líquido': 'Valor Líquido', 'Dia': 'Dia do Mês'},
    )
    st.plotly_chart(grafico_vendas_por_dia)

    #####
    # Div 2: Vendas por Loja

    groupby_vendas_dia_loja = groupby_sales_por_unidade(df_sales)
    
    pivot_vendas_dia_loja = groupby_vendas_dia_loja.pivot(
                            index='Dia',
                            columns='Unidade',
                            values='Valor líquido')
    pivot_vendas_dia_loja = pivot_vendas_dia_loja.fillna(0)
    st.markdown("###### Venda Diária Detalhada")
    st.dataframe(pivot_vendas_dia_loja)

    #####
    # Div 3: Vendas por Profissão e Consultor

    col1, col2 = st.columns(2)

    with col1:
        groupby_vendas_por_profissao = groupby_sales_por_profissao(df_sales)

        grafico_vendas_por_profissao_top10 = px.pie(
                groupby_vendas_por_profissao,
                names='Profissão cliente',
                values='Valor líquido', # : 'sum'
                title='Valor comprado por Profissão - Top10',
                labels={'Valor líquido': 'Valor Comprado', 'Profissão cliente': 'Profissão'},
            )

        st.plotly_chart(grafico_vendas_por_profissao_top10)

    with col2:
        groupby_vendas_por_vendedoras = groupby_sales_por_vendedoras(df_sales)

        grafico_vendas_por_consultor = px.bar(
            groupby_vendas_por_vendedoras,
            x='Consultor',
            y='Valor líquido',
            title='Venda Por Consultora',
            labels={'Valor líquido': 'Valor Líquido', 'Consultor': 'Consultora de Vendas'},
        )

        grafico_vendas_por_consultor = px.bar(
            groupby_vendas_por_vendedoras,
            x='Consultor',
            y='Valor líquido',
            title='Venda Por Consultora - Top10',
            labels={'Valor líquido': 'Valor Líquido', 'Consultor': 'Consultora de Vendas'},
        )
        st.plotly_chart(grafico_vendas_por_consultor)