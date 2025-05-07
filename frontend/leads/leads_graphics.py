from plotly import express as px
import streamlit as st
import pandas as pd
from frontend.leads.leads_grouper import groupby_leads_por_dia

def graphic_leads_by_day(df_leads):
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

