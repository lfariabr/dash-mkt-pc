import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import asyncio
from apiCrm.resolvers.fetch_appointmentsByUserReport import fetch_and_process_appointmentsByUserReport 

def load_data(start_date=None, end_date=None, use_api=True):
    """
    Load and preprocess appointments by user data.
    
    Args:
        start_date (str, optional): Start date in YYYY-MM-DD format for API fetch
        end_date (str, optional): End date in YYYY-MM-DD format for API fetch
        use_api (bool): Whether to use the API (default) or fallback mechanism
        
    Returns:
        DataFrame: Processed appointments by user dataframe
    """
    
    if start_date and end_date:
        try:
            # Run the async function using asyncio
            appointments_data = asyncio.run(fetch_and_process_appointmentsByUserReport(start_date, end_date))

            if not appointments_data:
                st.error("Não foi possível obter dados da API.")
                return pd.DataFrame()
            
            df = pd.DataFrame(appointments_data)
            
            st.success(f"Dados obtidos com sucesso via API: {len(df)} registros carregados.")
            return df
            
        except Exception as e:
            st.error(f"Erro ao buscar dados da API: {str(e)}")
            return pd.DataFrame()
    else:
        st.warning("Por favor, selecione um intervalo de datas.")
        return pd.DataFrame()

def load_page_appointmentsByUser():
    """
    Main function to display appointments by user data.
    """
    
    st.title("📊 2 - Agendamentos por Usuário")
    st.markdown("---")
    st.subheader("Selecione o intervalo de datas para o relatório:")

    start_date = None
    end_date = None
    
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "Data Inicial",
            value=datetime.now() - timedelta(days=7),
            max_value=datetime.now()
        ).strftime('%Y-%m-%d')
    with col2:
        end_date = st.date_input(
            "Data Final",
            value=datetime.now(),
            max_value=datetime.now()
        ).strftime('%Y-%m-%d')
        
    if st.button("Carregar"):
        with st.spinner("Carregando dados..."):
            df_appointmentsByUser = load_data(start_date, end_date)
            
            if not df_appointmentsByUser.empty:
                st.markdown("---")
                st.subheader("Agendamentos por Atendente")
                
                # Select and rename columns for display
                display_columns = ['name', 'appointments_count']
                df_display = df_appointmentsByUser[display_columns].copy()
                
                # Rename columns for better readability
                df_display = df_display.rename(columns={
                    'name': 'Atendente',
                    'appointments_count': 'Total de Agendamentos',
                })
                
                # Sort by appointments count in descending order
                df_display = df_display.sort_values(by='Total de Agendamentos', ascending=False)
                
                # Display the dataframe
                st.dataframe(df_display, hide_index=True)
                
                # Process and display the procedure counts
                st.markdown("---")
                st.subheader("Detalhamento por Tipo de Procedimento")
                
                # Create a new dataframe for procedure types
                procedure_df = pd.DataFrame()
                
                for idx, row in df_appointmentsByUser.iterrows():
                    # Extract the name and procedure counts
                    name = row['name']
                    procedure_counts = row['procedure_counts']
                    
                    # Create a row with the name and procedure counts
                    procedure_row = {'Atendente': name}
                    procedure_row.update(procedure_counts)
                    
                    # Add the row to the procedure dataframe
                    procedure_df = pd.concat([procedure_df, pd.DataFrame([procedure_row])], ignore_index=True)
                
                # Sort by the 'Atendente' column for consistency
                if not procedure_df.empty:
                    # Get the procedure columns (all columns except 'Atendente')
                    procedure_columns = [col for col in procedure_df.columns if col != 'Atendente']
                    
                    # Create a mapping of procedure codes to user-friendly names if needed
                    procedure_names = {
                        'aesthetic': 'Estética',
                        'hair_removal': 'Depilação',
                        'invasive': 'Injetáveis e Invasivos',
                        'surgery': 'Cirurgia',
                        'tattoo': 'Tatuagem'
                    }
                    
                    # Rename procedure columns if they match known codes
                    rename_dict = {}
                    for col in procedure_columns:
                        if col in procedure_names:
                            rename_dict[col] = procedure_names[col]
                    
                    if rename_dict:
                        procedure_df = procedure_df.rename(columns=rename_dict)
                    
                    # Sort by the 'Injetáveis e Invasivos' column
                    procedure_df = procedure_df.sort_values(by='Injetáveis e Invasivos', ascending=False)
                    
                    # Display the procedure dataframe
                    st.dataframe(procedure_df, hide_index=True)
                else:
                    st.info("Não foram encontrados detalhes de procedimentos para o período selecionado.")
