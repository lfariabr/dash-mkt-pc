import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import asyncio
from apiCrm.resolvers.fetch_appointmentReportSamir import fetch_and_process_appointment_reportSamir 
from components.date_input import date_input

def load_data(start_date=None, end_date=None, use_api=False):
    """
    Load and preprocess appointments data.
    
    Args:
        start_date (str, optional): Start date in YYYY-MM-DD format for API fetch
        end_date (str, optional): End date in YYYY-MM-DD format for API fetch
        use_api (bool): Whether to use the API or local Excel file
        
    Returns:
        DataFrame: Processed appointments dataframe
    """
    
    if start_date and end_date:
        try:
            # Run the async function using asyncio
            appointments_data = asyncio.run(fetch_and_process_appointment_reportSamir(start_date, end_date))

            if not appointments_data:
                st.error("Não foi possível obter dados da API. Usando dados locais.")
                return load_data(use_api=False)
            
            df = pd.DataFrame(appointments_data)
            
            st.success(f"Dados obtidos com sucesso via API: {len(df)} agendamentos carregados.")
            
        except Exception as e:
            st.error(f"Erro ao buscar dados da API: {str(e)}")
            return load_data(use_api=False)
    else:
        st.warning("Por favor, selecione um intervalo de datas.")
        return pd.DataFrame()
    
    return df

def create_time_filtered_df(df, days=None):
    """Create a time-filtered dataframe."""
    if days:
        cutoff_date = datetime.now() - timedelta(days=days)
        return df[df['Data'] >= cutoff_date]
    return df

def load_page_appointmentsSamir():
    """Main function to display leads data."""
    
    st.title("📊 Testando...")
    st.markdown("---")
    st.subheader("Selecione o intervalo de datas para o relatório:")

    start_date, end_date = date_input()
        
    if st.button("Carregar"):
        with st.spinner("Carregando dados..."):
            df_appointments = load_data(start_date, end_date)

            ########
            # Header
            
            st.dataframe(df_appointments)

                