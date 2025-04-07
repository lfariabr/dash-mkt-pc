import streamlit as st
import pandas as pd
from datetime import datetime, timedelta


def sidebar_api():
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
    
    return start_date, end_date, use_api
