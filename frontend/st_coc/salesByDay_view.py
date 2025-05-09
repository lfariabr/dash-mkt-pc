import streamlit as st
import pandas as pd
import plotly.express as px
import asyncio
import plotly.graph_objects as go
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
        
    return df

def load_page_salesByDay():
    """Main function to display sales data."""
    
    st.title("📊 4 - Vendas por Dia")
    st.markdown("---")
    st.subheader("Selecione o intervalo de datas para o relatório:")

    start_date, end_date = date_input()
    
    if st.button("Carregar"):
        send_discord_message(f"Loading data in page salesByDay_view")
        with st.spinner("Carregando dados..."):
            df_sales = load_data(start_date, end_date)
        
            df_sales = df_sales.loc[df_sales['Status'] == 'completed']
            df_sales = df_sales.loc[df_sales['Consultor'] != 'BKO VENDAS']
            df_sales = df_sales.loc[df_sales['Unidade'] != 'PRAIA GRANDE']
            df_sales['Valor líquido'] = df_sales['Valor líquido'].astype(float)

            total_sales = df_sales['Valor líquido'].sum()
            formatted_total = f"R$ {total_sales:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
            
            st.header("Estatísticas Gerais 💎")
            col1, col2, col3 = st.columns(3)

            with col1:
                total_valor = df_sales['Valor líquido'].sum()
                st.metric("Valor Total", f"R$ {total_valor:,.2f}")
    
            with col2:
                media_valor = df_sales['Valor líquido'].mean()
                st.metric("Ticket Médio", f"R$ {media_valor:,.2f}")
    
            with col3:
                total_vendas = len(df_sales)
                st.metric("Total de Vendas", f"{total_vendas:,}")
        
            groupby_vendas_dia_loja = groupby_sales_por_unidade(df_sales)
            
            pivot_vendas_dia_loja = groupby_vendas_dia_loja.pivot(
                                    index='Dia',
                                    columns='Unidade',
                                    values='Valor líquido')
            pivot_vendas_dia_loja = pivot_vendas_dia_loja.fillna(0)
            
            st.write("---")
            st.subheader("Visão por Unidade")
            col1, col2 = st.columns(2)
            
            with col1:
                # DataFrame grouped by Unidade
                unidade_df = df_sales.groupby('Unidade')['Valor líquido'].sum().sort_values(ascending=False).reset_index()
                unidade_df['Valor líquido'] = unidade_df['Valor líquido'].apply(lambda x: f'R$ {x:,.2f}')
                
                st.dataframe(
                    unidade_df,
                    hide_index=True,
                    height=len(unidade_df)* 38)
            
            with col2:
                # Bar chart for "Valor líquido por Unidade"
                unidade_sales = df_sales.groupby('Unidade')['Valor líquido'].sum().sort_values(ascending=True)
                
                fig = go.Figure(data=[
                    go.Bar(
                        x=unidade_sales.index,
                        y=unidade_sales.values,
                        text=[f'R$ {x:,.2f}' for x in unidade_sales.values],
                        textposition='auto',
                    )
                ])
                
                fig.update_layout(
                    xaxis_title='Unidade',
                    yaxis_title='Valor líquido (R$)',
                    showlegend=False
                )
                st.markdown("##### Valor líquido por Unidade")
                st.plotly_chart(
                    fig,
                    use_container_width=True)
            
            st.subheader("Vendas por Consultora - Top 10")
            col1, col2 = st.columns(2)
            
            with col1:
                # Bar chart for "Análise por Consultora"
                consultor_sales = df_sales.groupby('Consultor')['Valor líquido'].sum().sort_values(ascending=True).head(10)
                
                fig = go.Figure(data=[
                    go.Bar(
                        x=consultor_sales.index,
                        y=consultor_sales.values,
                        text=[f'R$ {x:,.2f}' for x in consultor_sales.values],
                        textposition='auto',
                    )
                ])
                
                fig.update_layout(
                    xaxis_title='Consultor',
                    yaxis_title='Valor líquido (R$)',
                    showlegend=False
                )
                st.plotly_chart(
                    fig,
                    use_container_width=True)
            
            with col2:
                consultor_stats = df_sales.groupby('Consultor').agg({
                    'Valor líquido': ['sum', 'mean'],
                    'ID orçamento': 'nunique'  # Count unique budget IDs
                }).round(2)
                
                # Rename columns
                consultor_stats.columns = ['Total Vendas', 'Média por Venda', 'Quantidade']
        
                # Reorder columns
                consultor_stats = consultor_stats[['Total Vendas', 'Quantidade', 'Média por Venda']]
                consultor_stats = consultor_stats.sort_values('Total Vendas', ascending=False)
        
                # Format currency values
                consultor_stats['Total Vendas'] = consultor_stats['Total Vendas'].apply(lambda x: f'R$ {x:,.2f}')
                consultor_stats['Média por Venda'] = consultor_stats['Média por Venda'].apply(lambda x: f'R$ {x:,.2f}')
        
                # Reset index to show Consultor as a column
                consultor_stats = consultor_stats.reset_index()
                st.dataframe(
                    consultor_stats,
                    hide_index=True,
                    use_container_width=True)
            
        # Div 4: Vendas por Profissão e Procedimento
        st.subheader("Compras: Profissão e Procedimentos")
        col1, col2 = st.columns(2)
        with col1:
            groupby_vendas_por_profissao = groupby_sales_por_profissao(df_sales)

            grafico_vendas_por_profissao_top10 = px.pie(
                    groupby_vendas_por_profissao,
                    names='Profissão cliente',
                    values='Valor líquido', # : 'sum'
                    title='Valor comprado por Profissão - Top10',
                    labels={'Valor líquido': 'Valor Comprado', 'Profissão cliente': 'Profissão'},
                    hole=0.3
                )
            st.plotly_chart(grafico_vendas_por_profissao_top10)

        with col2:
            # extract "procedimento" from "bill_items"
            df_sales['Procedimento'] = df_sales['bill_items'].str.split('(').str[0]

            groupby_vendas_por_procedimento = groupby_sales_por_procedimento(df_sales)
            grafico_vendas_por_procedimento = px.pie(
                groupby_vendas_por_procedimento,
                names='Procedimento',
                values='Valor líquido',
                title='Valor comprado por Procedimento - Top10',
                labels={'Valor líquido': 'Valor Comprado', 'Procedimento': 'Procedimento'},
                hole=0.3  # Para um gráfico de rosca (opcional)
            )
            st.plotly_chart(grafico_vendas_por_procedimento)
            
            