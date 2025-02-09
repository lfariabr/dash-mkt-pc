import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
from datetime import datetime, timedelta
from data.sources import paid_sources, organic_sources
from data.stores import stores_to_remove
from views.leads.lead_category import categorize, process_lead_categories

def load_data():
    """Load and preprocess leads data."""
    leads = 'db/leads.xlsx' # Change this later on #TODO

    df = pd.read_excel(leads)
    df = df.loc[~df['Unidade'].isin(stores_to_remove)]
    
    df['Dia da entrada'] = pd.to_datetime(df['Dia da entrada'])
    df['Dia'] = df['Dia da entrada'].dt.day
    df['Mês'] = df['Dia da entrada'].dt.month
    df['Dia da Semana'] = df['Dia da entrada'].dt.day_name()
    
    return df

def create_time_filtered_df(df, days=None):
    """Create a time-filtered dataframe."""
    if days:
        cutoff_date = datetime.now() - timedelta(days=days)
        return df[df['Dia da entrada'] >= cutoff_date]
    return df

def load_page_leads():
    """Main function to display leads analytics."""
    st.title("📊 10 - Leads")
    
    df_leads = load_data()
    
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
        df_leads = create_time_filtered_df(df_leads, days_map[time_filter])
    
    unidades = ["Todas"] + sorted(df_leads['Unidade'].unique().tolist())
    selected_store = st.sidebar.selectbox("Unidade", unidades)
    
    if selected_store != "Todas":
        df_leads = df_leads[df_leads['Unidade'] == selected_store]
    
    ########
    # Header
    st.header("Visão Geral")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total de Leads", len(df_leads))
    with col2:
        st.metric("Total de Unidades", df_leads['Unidade'].nunique())
    with col3:
        total_days_count = df_leads['Dia'].nunique()
        st.metric("Dias Até Ontem", total_days_count)
    with col4:
        avg_leads_per_day = len(df_leads) / df_leads['Dia'].nunique()
        st.metric("Média de Leads/Dia", f"{avg_leads_per_day:.1f}")
    st.markdown("---")
    
    #######
    # Div 1 Análise Detalhada: Leads por Dia do Mês e Leads por Unidade
    col1, col2 = st.columns(2)
    
    with col1:
        groupby_leads_by_day = (df_leads
                                .groupby('Dia')                 
                                .agg({'ID do lead': 'nunique'})  
                                .reset_index()                   
                            )
        
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
        groupby_leads_by_store = (df_leads
                                    .groupby('Unidade')                 
                                    .agg({'ID do lead': 'nunique'})  
                                    .reset_index()                   
                                )
        
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
        groupby_leads_by_source = (
                                df_leads
                                .groupby('Fonte')
                                .agg({'ID do lead': 'nunique'})
                                .reset_index()
                                )
        
        fig_source = px.pie(
            groupby_leads_by_source,
            names='Fonte',
            values='ID do lead',
            title='Distribuição de Leads por Fonte',
            hole=0.4
        )
        st.plotly_chart(fig_source, use_container_width=True)
    
    with col2:
        groupby_leads_by_status = (
                                    df_leads
                                    .groupby('Status')
                                    .agg({'ID do lead': 'nunique'})
                                    .reset_index()
                                )
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
            groupby_store_by_paid_source = (
                df_leads[df_leads['Fonte'].isin(paid_sources)]
                .groupby(['Unidade', 'Fonte'])
                .agg({
                    'ID do lead': 'nunique',
                    'Status': lambda x: (x == 'Convertido').mean() * 100
                })
                .round(2)
                .reset_index()
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
            groupby_store_by_organic_source = (
                df_leads[df_leads['Fonte'].isin(organic_sources)]
                .groupby(['Unidade', 'Fonte'])
                .agg({
                    'ID do lead': 'nunique',
                    'Status': lambda x: (x == 'Convertido').mean() * 100
                })
                .round(2)
                .reset_index()
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
