import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
from datetime import datetime, timedelta
import asyncio
from data.stores import stores_to_remove
from data.date_intervals import days_map, available_periods
from components.headers import header_sales
from helpers.date import transform_date_from_sales
from apiCrm.resolvers.fetch_grossSalesReport import fetch_and_process_grossSales_report 
from views.sales.sales_grouper import (
                                        groupby_sales_por_dia,
                                        groupby_sales_por_unidade,
                                        groupby_sales_por_profissao,
                                        groupby_sales_por_vendedoras)
def load_data(start_date=None, end_date=None, use_api=False):
    """Load and preprocess leads data."""
    sales = 'db/sales.xlsx'

    # TODO Just like what we did @ lead_view.py we're going to adapt the code here too!
    if use_api and start_date and end_date:
        try:
            # Run the async function using asyncio
            sales_data = asyncio.run(fetch_and_process_grossSales_report(start_date, end_date))

            if not sales_data:
                st.error("Não foi possível obter dados da API. Usando dados locais.")
                return load_data(use_api=False)
            
            df = pd.DataFrame(sales_data)
            
            # Map API field names to match the excel structure
            df = df.rename(columns={
                'id': 'ID venda',
                'createdAt': 'Data',
                'customerSignedAt': 'Data de assinatura',
                'isFree': 'Venda Gratuita',
                'isReseller': 'Venda de revendedor',
                'status': 'Status',
                'statusLabel': 'StatusLabel',
                'store_name': 'Unidade',
                'createdBy': 'Consultor',
                'employees': 'Profissão',
                'chargableTotal': 'Valor líquido',
                'bill_items': 'Itens',
                'procedure_groupLabels': 'Grupo do procedimento',
                'customer_name': 'Nome cliente',
                'customer_email': 'Email',
                'taxvat': 'CPF',
                'taxvatFormatted': 'CPF',
                'source': 'Fonte de cadastro do cliente',
                'telephones': 'Telefone',
                'birthdate': 'Data de nascimento',
                'occupation': 'Profissão cliente',                
            })
            
            # Convert startDate to datetime
            df['Data'] = pd.to_datetime(df['Data'])
            
            # Format the date for 'Dia' column (single step)
            df['Dia'] = df['Data'].dt.strftime('%d-%m-%Y')
            
            st.success(f"Dados obtidos com sucesso via API: {len(df)} vendas carregadas.")
            
        except Exception as e:
            st.error(f"Erro ao buscar dados da API: {str(e)}")
            return load_data(use_api=False)
    else:
        # Use the original Excel data source
        sales = 'db/sales.xlsx'
        df = pd.read_excel(sales)
        
    # Apply common transformations
    df = df.loc[~df['Unidade'].isin(stores_to_remove)]

    # Only apply date transformation if not using API
    if not use_api:
        df = transform_date_from_sales(df)
    
    return df

def create_time_filtered_df(df, days=None):
    """Create a time-filtered dataframe."""
    if days:
        cutoff_date = datetime.now() - timedelta(days=days)
        return df[df['Data'] >= cutoff_date]
    return df

def load_page_sales():
    """Main function to display leads data."""
    
    st.title("📊 11 - Vendas")

    st.sidebar.header("Filtros")
    use_date_range = st.sidebar.checkbox("Usar intervalo de datas personalizado", False)
    
    use_api = False
    start_date = None
    end_date = None
    
    if use_date_range:
        col1, col2 = st.sidebar.columns(2)
        with col1:
            start_date = st.date_input(
                "Data Inicial",
                value=datetime.now() - timedelta(days=30),
                max_value=datetime.now()
            ).strftime('%Y-%m-%d')
        with col2:
            end_date = st.date_input(
                "Data Final",
                value=datetime.now(),
                max_value=datetime.now()
            ).strftime('%Y-%m-%d')
        
        use_api = st.sidebar.checkbox("Usar API para buscar dados", True)

        if use_api:
            st.sidebar.info("Os dados serão buscados da API usando o intervalo de datas selecionado.")
    
    df_sales = load_data(start_date, end_date, use_api)

    if not use_date_range:
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

    st.dataframe(df_sales)
    
    # Tratativas especiais:
    df_sales = df_sales.loc[df_sales['Status'] == 'Finalizado']
    df_sales = df_sales.loc[df_sales['Consultor'] != 'BKO VENDAS']

    #####
    # Div 1: Vendas por Dia
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