import streamlit as st
from datetime import datetime, timedelta

def date_input():
    start_date = None
    end_date = None
    
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "Data Inicial",
            value=datetime.now() - timedelta(days=1),
            max_value=datetime.now() + timedelta(days=5)
        ).strftime('%Y-%m-%d')
    with col2:
        end_date = st.date_input(
            "Data Final",
            value=datetime.now() - timedelta(days=1),
            max_value=datetime.now() + timedelta(days=5)
        ).strftime('%Y-%m-%d')

    return start_date, end_date