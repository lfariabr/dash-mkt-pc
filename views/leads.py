import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
from datetime import datetime, timedelta
from data.sources import FONTES_PAGAS, FONTES_ORGANICAS
from data.stores import UNIDADES_PARA_REMOVER
from .leads_grouper import (
    get_leads_por_dia,
    get_leads_por_unidade,
    get_leads_por_fonte,
    get_leads_por_status
)

# Constants

def load_data():
    """Load and preprocess leads data."""
    leads = 'db/leads.xlsx' # TEMPORARY

    df = pd.read_excel(leads)
    df = df.loc[~df['Unidade'].isin(UNIDADES_PARA_REMOVER)]
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

def show_leads_analytics():
    """Main function to display leads analytics."""
    st.title("📊 Dashboard de Leads")
    
    # Load data
    df_leads = load_data()
    
    # Sidebar filters
    st.sidebar.header("Filtros")
    
    # Time filter
    time_filter = st.sidebar.selectbox(
        "Período",
        ["Todos os dados", "Últimos 7 dias", "Últimos 30 dias", "Últimos 90 dias"]
    )
    
    # Apply time filter
    days_map = {
        "Últimos 7 dias": 7,
        "Últimos 30 dias": 30,
        "Últimos 90 dias": 90
    }
    if time_filter != "Todos os dados":
        df_leads = create_time_filtered_df(df_leads, days_map[time_filter])
    
    # Unit filter
    unidades = ["Todas"] + sorted(df_leads['Unidade'].unique().tolist())
    selected_unidade = st.sidebar.selectbox("Unidade", unidades)
    
    if selected_unidade != "Todas":
        df_leads = df_leads[df_leads['Unidade'] == selected_unidade]
    
    # Overview metrics
    st.header("Visão Geral")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total de Leads", len(df_leads))
    with col2:
        st.metric("Total de Unidades", df_leads['Unidade'].nunique())
    with col3:
        conversion_rate = (df_leads['Status'] == 'Convertido').mean() * 100
        st.metric("Taxa de Conversão", f"{conversion_rate:.1f}%")
    with col4:
        avg_leads_per_day = len(df_leads) / df_leads['Dia'].nunique()
        st.metric("Média de Leads/Dia", f"{avg_leads_per_day:.1f}")
    
    # Main charts
    st.header("Análise Detalhada")
    
    # Row 1: Leads por Dia e Unidade
    col1, col2 = st.columns(2)
    
    with col1:
        # Leads por Dia
        groupby_leads_por_dia = get_leads_por_dia(df_leads)
        
        fig_dia = px.line(
            groupby_leads_por_dia,
            x='Dia',
            y='ID do lead',
            title='Leads por Dia do Mês',
            labels={'ID do lead': 'Quantidade de Leads', 'Dia': 'Dia do mês'},
            markers=True
        )
        st.plotly_chart(fig_dia, use_container_width=True, key="leads_by_day")
    
    with col2:
        # Leads por Unidade
        groupby_leads_por_unidade = get_leads_por_unidade(df_leads)
        
        fig_unidade = px.bar(
            groupby_leads_por_unidade,
            x='Unidade',
            y='ID do lead',
            title='Leads por Unidade',
            labels={'ID do lead': 'Quantidade de Leads', 'Unidade': 'Unidade'}
        )
        st.plotly_chart(fig_unidade, use_container_width=True, key="leads_by_unit")
    
    # Row 2: Fonte e Status
    col1, col2 = st.columns(2)
    
    with col1:
        # Leads por Fonte
        groupby_leads_por_fonte = get_leads_por_fonte(df_leads)
        
        fig_fonte = px.pie(
            groupby_leads_por_fonte,
            names='Fonte',
            values='ID do lead',
            title='Distribuição de Leads por Fonte',
            hole=0.4  # Makes it a donut chart
        )
        st.plotly_chart(fig_fonte, use_container_width=True, key="leads_by_source")
    
    with col2:
        # Leads por Status
        groupby_leads_por_status = get_leads_por_status(df_leads)
        
        fig_status = px.pie(
            groupby_leads_por_status,
            names='Status',
            values='ID do lead',
            title='Distribuição de Leads por Status',
            hole=0.4  # Makes it a donut chart
        )
        st.plotly_chart(fig_status, use_container_width=True, key="leads_by_status")
    
    # Detailed Analysis Section
    st.header("Análise por Tipo de Fonte")
    
    tab1, tab2 = st.tabs(["Fontes Pagas", "Fontes Orgânicas"])
    
    with tab1:
        df_leads_fontes_pagas = df_leads[df_leads['Fonte'].isin(FONTES_PAGAS)]
        groupby_unidade_fonte_paga = (
            df_leads_fontes_pagas
            .groupby(['Unidade', 'Fonte'])
            .agg({
                'ID do lead': 'nunique',
                'Status': lambda x: (x == 'Convertido').mean() * 100
            })
            .round(2)
            .reset_index()
        )
        groupby_unidade_fonte_paga.columns = ['Unidade', 'Fonte', 'Quantidade de Leads', 'Taxa de Conversão (%)']
        st.dataframe(
            groupby_unidade_fonte_paga,
            hide_index=True,
            use_container_width=True
        )
    
    with tab2:
        df_leads_fontes_organicas = df_leads[df_leads['Fonte'].isin(FONTES_ORGANICAS)]
        groupby_unidade_fonte_organica = (
            df_leads_fontes_organicas
            .groupby(['Unidade', 'Fonte'])
            .agg({
                'ID do lead': 'nunique',
                'Status': lambda x: (x == 'Convertido').mean() * 100
            })
            .round(2)
            .reset_index()
        )
        groupby_unidade_fonte_organica.columns = ['Unidade', 'Fonte', 'Quantidade de Leads', 'Taxa de Conversão (%)']
        st.dataframe(
            groupby_unidade_fonte_organica,
            hide_index=True,
            use_container_width=True
        )
    
    # Download section
    st.header("Download dos Dados")
    if st.download_button(
        label="Download dados completos (CSV)",
        data=df_leads.to_csv(index=False).encode('utf-8'),
        file_name='leads_analysis.csv',
        mime='text/csv'
    ):
        st.success('Download iniciado!')

if __name__ == "__main__":
    show_leads_analytics()
