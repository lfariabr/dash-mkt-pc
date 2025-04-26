import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
from datetime import datetime, timedelta
import asyncio
from data.sources import paid_sources, organic_sources
from data.stores import stores_to_remove
from data.date_intervals import days_map, available_periods
from components.headers import header_leads
from helpers.date import transform_date_from_leads
from views.leads.lead_category import process_lead_categories
from views.leads.leads_grouper import (
                                        groupby_leads_por_dia,
                                        groupby_leads_por_unidade,
                                        groupby_leads_por_fonte,
                                        groupby_leads_por_status,
                                        groupby_unidade_fonte_paga,
                                        groupby_unidade_fonte_organica
                                    )
from apiCrm.resolvers.fetch_leadReport import fetch_and_process_lead_report
from components.date_input import date_input

def load_data(start_date=None, end_date=None, use_api=False):
    """
    Load and preprocess leads data.
    
    Args:
        start_date (str, optional): Start date in YYYY-MM-DD format for API fetch
        end_date (str, optional): End date in YYYY-MM-DD format for API fetch
        use_api (bool): Whether to use the API or local Excel file
        
    Returns:
        DataFrame: Processed leads dataframe
    """
    if start_date and end_date:
        try:
            # Run the async function using asyncio
            leads_data = asyncio.run(fetch_and_process_lead_report(start_date, end_date))
            
            if not leads_data:
                st.error("Não foi possível obter dados da API. Usando dados locais.")
                return load_data(start_date, end_date, use_api=False)
            
            # Convert the API data to a DataFrame
            df = pd.DataFrame(leads_data)
            
            # Map API field names to match the excel structure
            df = df.rename(columns={
                'id': 'ID do lead',
                'name': 'Nome',
                'email': 'Email',
                'telephone': 'Telefone',
                'message': 'Mensagem',
                'createdAt': 'Dia da entrada',
                'store': 'Unidade',
                'source': 'Fonte',
                'status': 'Status',
                'utmSource': 'utmSource',
                'utmMedium': 'utmMedium',
                'utmTerm': 'utmTerm',
                'utmContent': 'Content',
                'utmCampaign': 'utmCampaign',
                'searchTerm': 'searchTerm'
            })
            
            # Convert createdAt to datetime
            df['Dia da entrada'] = pd.to_datetime(df['Dia da entrada'])
            
            # Format the date for 'Dia' column (single step)
            df['Dia'] = df['Dia da entrada'].dt.strftime('%d-%m-%Y')
            
            st.success(f"Dados obtidos com sucesso via API: {len(df)} leads carregados.")
            
        except Exception as e:
            st.error(f"Erro ao buscar dados da API: {str(e)}")
            return load_data(use_api=False)
    else:
        st.warning("Por favor, selecione um intervalo de datas.")
        return pd.DataFrame()
        
    # Apply common transformations
    df = df.loc[~df['Unidade'].isin(stores_to_remove)]
    
    return df

def create_time_filtered_df(df, days=None):
    """Create a time-filtered dataframe."""
    if days:
        cutoff_date = datetime.now() - timedelta(days=days)
        return df[df['Dia da entrada'] >= cutoff_date]
    return df

def load_page_leads():
    """Main function to display leads data."""    

    st.title("📊 1 - Leads")
    st.markdown("---")
    st.subheader("Selecione o intervalo de datas para o relatório:")
    
    start_date, end_date = date_input()
    
    if st.button("Carregar"):
        from utils.discord import send_discord_message
        send_discord_message(f"Loading data in page leads_view")
        with st.spinner("Carregando dados..."):
            df_leads = load_data(start_date, end_date)
    
            ########
            # Header
            header_leads(df_leads)
            
            #######
            # Div 1 Análise Detalhada: Leads por Dia do Mês e Leads por Unidade
            col1, col2 = st.columns(2)
            
            with col1:
                groupby_leads_by_day = groupby_leads_por_dia(df_leads)
                
                fig_day = px.line(
                    groupby_leads_by_day,
                    x='Dia',
                    y='ID do lead',
                    title='Leads por Dia do Mês',
                    labels={'ID do lead': 'Quantidade de Leads', 'Dia': 'Dia do mês'},
                    markers=True
                )
                st.plotly_chart(fig_day, use_container_width=True, key='fig_day')
            
            with col2:
                groupby_leads_by_store = groupby_leads_por_unidade(df_leads)
                
                fig_store = px.bar(
                    groupby_leads_by_store,
                    x='Unidade',
                    y='ID do lead',
                    title='Leads por Unidade',
                    labels={'ID do lead': 'Quantidade de Leads', 'Unidade': 'Unidade'}
                )
                st.plotly_chart(fig_store, use_container_width=True, key='fig_store')
            
            #######
            # Div 2: Distribuição de Leads por Fonte e Distribuição de Leads por Status
            col1, col2 = st.columns(2)
            
            with col1:
                groupby_leads_by_source = groupby_leads_por_fonte(df_leads)
                
                fig_source = px.pie(
                    groupby_leads_by_source,
                    names='Fonte',
                    values='ID do lead',
                    title='Distribuição de Leads por Fonte',
                    hole=0.4
                )
                st.plotly_chart(fig_source, use_container_width=True, key='fig_source')
            
            with col2:
                groupby_leads_by_status = groupby_leads_por_status(df_leads)
                
                fig_status = px.pie(
                    groupby_leads_by_status,
                    names='Status',
                    values='ID do lead',
                    title='Distribuição de Leads por Status',
                    hole=0.4
                )
                st.plotly_chart(fig_status, use_container_width=True, key='fig_status')
            
            #######
            # Div 3: Distribuição de Leads por Categoria e Dia

            df_leads = process_lead_categories(df_leads)

            groupby_category_leads_by_day = (
                df_leads
                .groupby(['Categoria', 'Dia'])
                .agg({'ID do lead': 'nunique'})
                .reset_index()
                .pivot(index='Dia', columns='Categoria', values='ID do lead')
                .fillna(0)
                .reset_index()
                .melt(id_vars=['Dia'], var_name='Categoria', value_name='ID do lead')
            )
            
            fig_category_leads_by_day = px.bar(
                groupby_category_leads_by_day,
                x='Dia',
                y='ID do lead',
                color='Categoria',
                title='Distribuição de Leads por Categoria e Dia'
            )
            st.plotly_chart(fig_category_leads_by_day, use_container_width=True, key='fig_category_leads_by_day')

            ########
            # Div 4: Distribuição de Leads por Fonte Paga e Orgânica 
            st.markdown("---")
            st.markdown("##### Análise por Tipo de Fonte")
            tab1, tab2 = st.tabs(["Fontes Pagas", "Fontes Orgânícas"])

            with tab1:
                col1, col2 = st.columns(2)

                with col1:
                    groupby_store_by_paid_source = groupby_unidade_fonte_paga(
                                                    df_leads[df_leads['Fonte'].isin(paid_sources)]
                                                )
                    
                    fig_paid_source = px.bar(
                        groupby_store_by_paid_source,
                        x='Unidade',
                        y='ID do lead',
                        color='Fonte',
                        title='Leads por Unidade e Fonte',
                        labels={'ID do lead': 'Quantidade de Leads', 'Unidade': 'Unidade'}
                    )
                    st.plotly_chart(fig_paid_source, use_container_width=True, key='fig_paid_source')    

                with col2:
                    # Pizza graphic with leads by paid source
                    fig_paid_source_pie = px.pie(
                        groupby_store_by_paid_source,
                        names='Fonte',
                        values='ID do lead',
                        title='Distribuição de Leads por Fonte Paga',
                        hole=0.4
                    )
                    st.plotly_chart(fig_paid_source_pie, use_container_width=True, key='fig_paid_source_pie')

                pivot_store_by_paid_source = (
                    groupby_store_by_paid_source.pivot_table(
                        index='Fonte',
                        columns='Unidade',
                        values='ID do lead',
                        aggfunc='sum'
                    )
                )
                st.markdown("##### Distribuição de Leads por Fonte Paga")
                st.dataframe(pivot_store_by_paid_source, use_container_width=True)

            with tab2:
                col1, col2 = st.columns(2)
                with col1:
                    groupby_store_by_organic_source = groupby_unidade_fonte_organica(
                        df_leads[df_leads['Fonte'].isin(organic_sources)]
                    )
                    
                    fig_organic_source = px.bar(
                        groupby_store_by_organic_source,
                        x='Unidade',
                        y='ID do lead',
                        color='Fonte',
                        title='Leads por Unidade e Fonte',
                        labels={'ID do lead': 'Quantidade de Leads', 'Unidade': 'Unidade'}
                    )
                    st.plotly_chart(fig_organic_source, use_container_width=True, key='fig_organic_source')    

                with col2:
                    # Pizza graphic with leads by organic source
                    fig_organic_source_pie = px.pie(
                        groupby_store_by_organic_source,
                        names='Fonte',
                        values='ID do lead',
                        title='Distribuição de Leads por Fonte Orgânica',
                        hole=0.4
                    )
                    st.plotly_chart(fig_organic_source_pie, use_container_width=True, key='fig_organic_source_pie')

                pivot_store_by_organic_source = (
                    groupby_store_by_organic_source.pivot_table(
                        index='Fonte',
                        columns='Unidade',
                        values='ID do lead',
                        aggfunc='sum'
                    )
                )
                st.markdown("##### Distribuição de Leads por Fonte Orgânica")
                st.dataframe(pivot_store_by_organic_source, use_container_width=True)

            #######
            # Div 5: Entrada Diária de Leads por Loja
            st.markdown("---")
            st.markdown("##### Entrada Diária de Leads por Loja")
            groupby_store_leads_by_day = (
                df_leads
                .groupby(['Unidade', 'Dia'])
                .agg({'ID do lead': 'nunique'})
                .reset_index()
            )

            pivot_store_leads_by_day = (
                groupby_store_leads_by_day
                .pivot_table(
                    values='ID do lead',
                    index='Dia',
                    columns='Unidade',
                    aggfunc='sum'
                )
            )

            st.dataframe(pivot_store_leads_by_day)

            st.header("Download dos Dados")
            if st.download_button(
                label="Download dados completos (CSV)",
                data=df_leads.to_csv(index=False).encode('utf-8'),
                file_name='leads_analysis.csv',
                mime='text/csv'
            ):
                st.success('Download iniciado!')

if __name__ == "__main__":
    load_page_leads()
