import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
from datetime import datetime, timedelta
from data.stores import stores_to_remove

def load_data():
    """Load and preprocess sales data."""
    sales = 'db/sales.xlsx' # Change this later on #TODO

    df = pd.read_excel(sales)
    df = df.loc[~df['Unidade'].isin(stores_to_remove)]
    
    df['Data venda'] = pd.to_datetime(df['Data venda'])
    df['Dia'] = df['Data venda'].dt.day
    df['Mês'] = df['Data venda'].dt.month
    df['Dia da Semana'] = df['Data venda'].dt.day_name()
    
    return df

def create_time_filtered_df(df, days=None):
    """Create a time-filtered dataframe."""
    if days:
        cutoff_date = datetime.now() - timedelta(days=days)
        return df[df['Dia da entrada'] >= cutoff_date]
    return df

def load_page_sales():
    """Main function to display sales analytics."""
    st.title("📊 2 - Sales")
    
    df_sales = load_data()
    
    st.sidebar.header("Filtros")
    time_filter = st.sidebar.selectbox(
        "Período",
        ["Todos os dados", "Últimos 7 dias", "Últimos 30 dias", "Últimos 90 dias"]
    )    
    days_map = {
        "Últimos 7 dias": 7,
        "Últimos 30 dias": 30,
        "Últimos 90 dias": 90
    }
    if time_filter != "Todos os dados":
        df_sales = create_time_filtered_df(df_sales, days_map[time_filter])
    
    unidades = ["Todas"] + sorted(df_sales['Unidade'].unique().tolist())
    selected_store = st.sidebar.selectbox("Unidade", unidades)
    
    if selected_store != "Todas":
        df_sales = df_sales[df_sales['Unidade'] == selected_store]
    
    # Overview metrics
    st.header("Visão Geral")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total de sales", len(df_sales))
    with col2:
        st.metric("Total de Unidades", df_sales['Unidade'].nunique())
    with col3:
        conversion_rate = (df_sales['Status'] == 'Convertido').mean() * 100
        st.metric("Taxa de Conversão", f"{conversion_rate:.1f}%")
    with col4:
        avg_sales_per_day = len(df_sales) / df_sales['Dia'].nunique()
        st.metric("Média de sales/Dia", f"{avg_sales_per_day:.1f}")
    st.markdown("---")

    ########
    # Tratativas especiais:
    # Tratativa #1: Criando uma coluna para "DIA"
    df_sales['Data venda'] = pd.to_datetime(df_sales['Data venda']) # Aqui vamos dizer para o código que este campo é uma data (pd.to_datetime)
    df_sales['Dia'] = df_sales['Data venda'].dt.day # Isolando o dia do campo "Data venda"

    # Tratativa #2: Filtrando apenas vendas Finalizadas
    df_sales = df_sales.loc[df_sales['Status'] == 'Finalizado']

    # Tratativa #3: Excluindo Consultor "BKO VENDAS"
    df_sales = df_sales.loc[df_sales['Consultor'] != 'BKO VENDAS']

    # Groupby com as vendas por dia
    # Group by Vendas por Dia
    groupby_vendas_por_dia = (
        df_sales
        .groupby('Dia')
        .agg({'Valor líquido': 'sum'})
        .reset_index()
    )

    # Gráfico barra vendas_por_dia
    grafico_vendas_por_dia = px.bar(
        groupby_vendas_por_dia,
        x='Dia',
        y='Valor líquido',
        title='Venda Diária',
        labels={'Valor líquido': 'Valor Líquido', 'Dia': 'Dia do Mês'},
    )
    st.plotly_chart(grafico_vendas_por_dia)

    #####
    # Tarefa de colocar linha de tendência no gráfico
    ####

    # Group by Venda / Dia / Loja
    # colunas: 'Unidade', 'Valor líquido', 'Dia'
    groupby_vendas_dia_loja = (
        df_sales
        .groupby(['Dia', 'Unidade'])
        .agg({'Valor líquido': 'sum'})
        .reset_index()
        .fillna(0)
    )

    # Pivotando os dados para exibir Dia x Unidade
    pivot_vendas_dia_loja = groupby_vendas_dia_loja.pivot(
                            index='Dia',
                            columns='Unidade',
                            values='Valor líquido')

    pivot_vendas_dia_loja = pivot_vendas_dia_loja.fillna(0)

    st.write("Venda Diária Detalhada")
    st.dataframe(pivot_vendas_dia_loja)


    # Dividindo a tela em 2 colunas:
    col1, col2 = st.columns(2)

    with col1:
        # Groupby por profissões
        groupby_vendas_por_profissao = (
            df_sales
            .groupby('Profissão cliente')
            .agg({'Valor líquido': 'sum'})
            .reset_index()
            .sort_values('Valor líquido', ascending=False)
            .head(10) # top 10
        )

        # Gráfico de Pizza
        grafico_vendas_por_profissao_top10 = px.pie(
                groupby_vendas_por_profissao,
                names='Profissão cliente',
                values='Valor líquido', # : 'sum'
                title='Valor comprado por Profissão',
                labels={'Valor líquido': 'Valor Comprado', 'Profissão cliente': 'Profissão'},
            )

        st.plotly_chart(grafico_vendas_por_profissao_top10)

    with col2:
        # Groupby por Vendedoras
        groupby_vendas_por_vendedoras = (
            df_sales
            .groupby('Consultor')
            .agg({'Valor líquido': 'sum'})
            .reset_index()
            .sort_values('Valor líquido', ascending=False)
            .head(10) # top 10
        )

        # Gráfico barra vendas_por_dia
        grafico_vendas_por_consultor = px.bar(
            groupby_vendas_por_vendedoras,
            x='Consultor',
            y='Valor líquido',
            title='Venda Por Consultora',
            labels={'Valor líquido': 'Valor Líquido', 'Consultor': 'Consultora de Vendas'},
        )
        st.plotly_chart(grafico_vendas_por_consultor)