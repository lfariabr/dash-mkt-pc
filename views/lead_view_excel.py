import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
from datetime import datetime, timedelta
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

def load_data():
    """Load and preprocess leads data."""
    leads = 'db/leads.xlsx'
    
    #TODO make this part pull data from apiCrm/resolvers/fetch_leadReport
    # BACK AT THIS BABY

    df = pd.read_excel(leads)
    df = df.loc[~df['Unidade'].isin(stores_to_remove)]
    df = transform_date_from_leads(df)
    
    return df

def create_time_filtered_df(df, days=None):
    """Create a time-filtered dataframe."""
    if days:
        cutoff_date = datetime.now() - timedelta(days=days)
        return df[df['Dia da entrada'] >= cutoff_date]
    return df

def load_page_leads():
    """Main function to display leads data."""
    

    st.title("📊 10 - Leads")
    df_leads = load_data()
    
    st.sidebar.header("Filtros")
    time_filter = st.sidebar.selectbox(
        "Período", available_periods
    )
    if time_filter != "Todos os dados":
        df_leads = create_time_filtered_df(df_leads, days_map[time_filter])
    
    unidades = ["Todas"] + sorted(df_leads['Unidade'].unique().tolist())
    selected_store = st.sidebar.selectbox("Unidade", unidades)
    
    if selected_store != "Todas":
        df_leads = df_leads[df_leads['Unidade'] == selected_store]
    
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
        st.plotly_chart(fig_day, use_container_width=True)
    
    with col2:
        groupby_leads_by_store = groupby_leads_por_unidade(df_leads)
        
        fig_store = px.bar(
            groupby_leads_by_store,
            x='Unidade',
            y='ID do lead',
            title='Leads por Unidade',
            labels={'ID do lead': 'Quantidade de Leads', 'Unidade': 'Unidade'}
        )
        st.plotly_chart(fig_store, use_container_width=True)
    
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
        st.plotly_chart(fig_source, use_container_width=True)
    
    with col2:
        groupby_leads_by_status = groupby_leads_por_status(df_leads)
        
        fig_status = px.pie(
            groupby_leads_by_status,
            names='Status',
            values='ID do lead',
            title='Distribuição de Leads por Status',
            hole=0.4
        )
        st.plotly_chart(fig_status, use_container_width=True)
    
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
    st.plotly_chart(fig_category_leads_by_day, use_container_width=True)

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
            st.plotly_chart(fig_paid_source, use_container_width=True)    

        with col2:
            # Pizza graphic with leads by paid source
            fig_paid_source_pie = px.pie(
                groupby_store_by_paid_source,
                names='Fonte',
                values='ID do lead',
                title='Distribuição de Leads por Fonte Paga',
                hole=0.4
            )
            st.plotly_chart(fig_paid_source_pie, use_container_width=True)

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
            st.plotly_chart(fig_organic_source, use_container_width=True)    

        with col2:
            # Pizza graphic with leads by organic source
            fig_organic_source_pie = px.pie(
                groupby_store_by_organic_source,
                names='Fonte',
                values='ID do lead',
                title='Distribuição de Leads por Fonte Orgânica',
                hole=0.4
            )
            st.plotly_chart(fig_organic_source_pie, use_container_width=True)

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
