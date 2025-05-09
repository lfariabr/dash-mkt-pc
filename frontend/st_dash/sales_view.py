import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
from datetime import datetime, timedelta
import asyncio
from components.headers import header_sales
from apiCrm.resolvers.dashboard.fetch_grossSalesReport import fetch_and_process_grossSales_report 
from frontend.sales.sales_grouper import (
                                        groupby_sales_por_dia,
                                        groupby_sales_por_unidade,
                                        groupby_sales_por_profissao,
                                        groupby_sales_por_vendedoras,
                                        groupby_sales_por_procedimento)
from components.date_input import date_input
from helpers.discord import send_discord_message

def load_data(start_date=None, end_date=None, use_api=False):
    """
    Load and preprocess sales data.
    
    Args:
        start_date (str, optional): Start date in YYYY-MM-DD format for API fetch
        end_date (str, optional): End date in YYYY-MM-DD format for API fetch
        use_api (bool): Whether to use the API or local Excel file
        
    Returns:
        DataFrame: Processed sales dataframe
    """
    
    if start_date and end_date:
        try:
            # Run the async function using asyncio
            sales_data = asyncio.run(fetch_and_process_grossSales_report(start_date, end_date))

            if not sales_data:
                st.error("Não foi possível obter dados da API. Usando dados locais.")
                return load_data(use_api=False)
            
            df = pd.DataFrame(sales_data)
            
            # Map API field names to match the excel structure
            df = df.rename(columns={
                'id': 'ID orçamento',
                'createdAt': 'Data orçamento',
                'status': 'Status',
                'isReseller': 'Revenda',
                'customerSignedAt': 'Data venda',
                'customerSignedMonth': 'Mês venda',
                'customerSignedTime': 'Hora venda',
                'store_name': 'Unidade',
                'createdBy': 'Consultor',
                'evaluator': 'Avaliador',
                'totalValue': 'Valor tabela',
                'discountValue': 'Valor desconto',
                'chargableTotal': 'Valor líquido',
                'procedure_groupLabels': 'Grupo procedimento',
                'procedureName': 'Procedimento',
                'quantity': 'Quantidade',
                'itemListPrice': 'Valor tabela item',
                'itemDiscountPercentage': 'Valor % desconto item',
                'itemDiscountValue': 'valor desconto item',
                'itemNetValue': 'Valor liquido item',
                'source': 'Fonte do cadastro do cliente',
                'customer_name': 'Nome cliente',
                'taxvat': 'CPF cliente',
                'customer_id': 'ID cliente',
                'customer_email': 'Email do cliente',
                'telephones': 'Telefone(s) do cliente',
                'birthdate': 'Data nascimento cliente',
                'occupation': 'Profissão cliente',
                'isFree': 'Cortesia?'
            })
            
            # Critical fix: Ensure 'Valor líquido' is explicitly converted from centavos to reais
            # API returns values in centavos (cents), e.g., 50000 = R$ 500,00
            if 'Valor líquido' in df.columns:
                # First ensure values are numeric
                df['Valor líquido'] = pd.to_numeric(df['Valor líquido'], errors='coerce').fillna(0)
                # Convert from centavos to reais by dividing by 100
                df['Valor líquido'] = df['Valor líquido'] / 100
                
                # Uncomment to debug conversion
                # st.write(f"After conversion: {df['Valor líquido'].head().tolist()}")
            
            # Convert dates to datetime with error handling
            df['Data venda'] = pd.to_datetime(df['Data venda'], errors='coerce')
            
            # Format the date for 'Dia' column (single step)
            df['Dia'] = df['Data venda'].dt.strftime('%d-%m-%Y')
            
            st.success(f"Dados obtidos com sucesso via API: {len(df)} vendas carregadas.")
            
        except Exception as e:
            st.error(f"Erro ao buscar dados da API: {str(e)}")
            return load_data(use_api=False)
    else:
        st.warning("Por favor, selecione um intervalo de datas.")
        return pd.DataFrame()
        
    # Apply common transformations
    # df = df.loc[~df['Unidade'].isin(stores_to_remove)]

    return df

def create_time_filtered_df(df, days=None):
    """Create a time-filtered dataframe."""
    if days:
        cutoff_date = datetime.now() - timedelta(days=days)
        return df[df['Data'] >= cutoff_date]
    return df

def load_page_sales():
    """Main function to display leads data."""
    
    st.title("📊 3 - Vendas")
    st.markdown("---")
    st.subheader("Selecione o intervalo de datas para o relatório:")

    start_date, end_date = date_input()
    
    if st.button("Carregar"):
        send_discord_message(f"Loading data in page sales_view")
        with st.spinner("Carregando dados..."):
            df_sales = load_data(start_date, end_date)
        
            ########
            # Header
            header_sales(df_sales)

            # Tratativas especiais:
            df_sales = df_sales.loc[df_sales['Status'] == 'completed']
            df_sales = df_sales.loc[df_sales['Consultor'] != 'BKO VENDAS']
            df_sales = df_sales.loc[df_sales['Unidade'] != 'PRAIA GRANDE']
            
            # Fix: Ensure proper numeric conversion of 'Valor líquido' for summation
            # This is the critical step that needs to happen after filtering
            df_sales['Valor líquido'] = df_sales['Valor líquido'].astype(float)
            
            
            # Sum of valor liquido
            total_sales = df_sales['Valor líquido'].sum()
            formatted_total = f"R$ {total_sales:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
            st.write(f"Total de vendas: {formatted_total}")
        
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
            # Div 3: Vendas por Unidade e Consultor
            col1, col2 = st.columns(2)

            with col1:
                groupby_vendas_por_unidade = groupby_sales_por_unidade(df_sales)
            
                # Sort by "Valor Líquido" descending
                groupby_vendas_por_unidade = groupby_vendas_por_unidade.sort_values('Unidade', ascending=True)
                
                grafico_vendas_por_unidade = px.bar(
                    groupby_vendas_por_unidade,
                    x='Unidade',
                    y='Valor líquido',
                    title='Venda Por Unidade',
                    labels={'Valor líquido': 'Valor Líquido', 'Unidade': 'Unidade'},
                )
                st.plotly_chart(grafico_vendas_por_unidade)

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
            
            #####
            # Div 4: Vendas por Profissão e Procedimento
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
                # extract "procedimento" from "bill_items"
                df_sales['Procedimento'] = df_sales['bill_items'].str.split('(').str[0]
                groupby_vendas_por_procedimento = groupby_sales_por_procedimento(df_sales)
                
                grafico_vendas_por_procedimento = px.pie(
                        groupby_vendas_por_procedimento,
                        names='Procedimento',
                        values='Valor líquido', # : 'sum'
                        title='Valor comprado por Procedimento - Top10',
                        labels={'Valor líquido': 'Valor Comprado', 'Procedimento': 'Procedimento'},
                    )
                st.plotly_chart(grafico_vendas_por_procedimento)
                
            