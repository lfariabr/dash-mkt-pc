import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import asyncio
from apiCrm.resolvers.fetch_leadsByUserReport import fetch_and_process_leadsByUserReport

def load_data(start_date=None, end_date=None):
    if start_date and end_date:
        try:
            leads_data = asyncio.run(fetch_and_process_leadsByUserReport(start_date, end_date))
            return pd.DataFrame(leads_data)
        except Exception as e:
            st.error(f"Erro ao carregar dados: {str(e)}")
            return pd.DataFrame()
    else:
        st.warning("Por favor, selecione um intervalo de datas.")
        return pd.DataFrame()

def load_page_leadsByUser():
    """Main function to display leads by user data."""
    st.title("📊 1 - Puxada de Leads")

    st.markdown("---")

    st.subheader("Selecione o intervalo de datas para o relatório:")
    
    start_date = None
    end_date = None
    
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "Data Inicial",
            value=datetime.now() - timedelta(days=1),
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
            df_leadsByUser = load_data(start_date, end_date)
            
            if df_leadsByUser.empty:
                st.warning("Não foram encontrados dados para o período selecionado.")
            else:
                # Treating data columns:
                leadsByUserColumns = ['name', 'messages_count', 'unique_messages_count', 'messages_count_by_status', 'success_rate']
                
                agendamento_por_lead_column = ['agd', 'jag'] # to filter messages_count_by_status column
                
                # Define attendants lists
                atendentes_puxadas_tarde = {
                    'Ingrid Caroline Santos Andrade' : 'Campinas', 
                    'Jenniffer Lopes Romão da Silva' : 'Tucuruvi',
                    'Fernanda Machado Leite' : 'Tijuca',
                    'Iasmin Monteiro Dias dos Santos' : 'Jardins',
                    'Gabrielly Cristina Silva dos Santos' : 'Tatuapé',
                    'Aryanne de jesus luiz' : 'Itaim',
                    'Giovanna Vitoria Cota Mascarenhas' : 'Tatuapé',
                    'Jihad Pereira dos Santos' : 'Ipiranga',
                    'Vitoria Almeida Silva' : 'Osasco',
                    'Sarah leal oliveira' : 'Vila Mascote',
                    'Thais Dias Souza Vasquez' : 'Moema',
                    'Talita Vitória Moreira' : 'Londrina',
                    'Luisa Yuka Hiraide Soares' : 'Copacabana',
                    'Ana Luiza Silva Martins' : 'Santo Amaro',
                    'Giovanna Maia Alves' : 'Santos',
                    'Alany Melo de Souza' : 'Santos'
                }

                atendentes_puxadas_manha = {
                    'Thamyres Lima Marques' : 'Tijuca',
                    'Aline Araujo de Oliveira' : 'Guarulhos',
                    'Amanda Raquel Cassula' : 'Londrina',
                    'Larissa Sabino dos Santos' : 'Moema',
                    'Sarah de Jesus dos Santos Pilatos' : 'Ipiranga',
                    'Yasmin Veronez Ramos' : 'São Bernardo',
                    'Geovanna Maynara Soares' : 'Sorocaba',
                    'Letícia Moreira Valentim' : 'Tatuapé',
                    'Camylli Victoria Lonis Silva' : 'Tucuruvi',
                    'Marta Maria Palma' : 'Jardins',
                    'Larissa Rodrigues da Silva' : 'Alphaville',
                    'Camilly Giselli Barros e Barros' : 'Osasco',
                    'Gabriela Gomes Magalhaes dos Anjos' : 'Santo Amaro',
                    'Renally Carla da Silva Moura' : 'Copacabana',
                    'Beatriz Nogueira de Oliveira' : 'Vila Mascote',
                    'Loren Schiavo Araujo' : 'Santos',
                    'Flavia Feitosa da Silva' : 'Mooca'
                }
                
                # Select basic columns and rename them
                df_leadsByUser = df_leadsByUser[leadsByUserColumns]
                
                # Process messages_count_by_status to extract agendamentos data
                df_leadsByUser['agendamentos_por_lead'] = 0
                
                # Check if messages_count_by_status is a dictionary and extract values
                def extract_agendamentos(status_dict):
                    if isinstance(status_dict, dict):
                        return sum(status_dict.get(status, 0) for status in agendamento_por_lead_column)
                    return 0
                
                df_leadsByUser['agendamentos_por_lead'] = df_leadsByUser['messages_count_by_status'].apply(extract_agendamentos)
                                
                # Add location from dictionaries
                df_leadsByUser['local'] = ''
                df_leadsByUser['turno'] = ''
                
                # Add location and shift info
                for atendente, local in atendentes_puxadas_manha.items():
                    mask = df_leadsByUser['name'] == atendente
                    if mask.any():
                        df_leadsByUser.loc[mask, 'local'] = local
                        df_leadsByUser.loc[mask, 'turno'] = 'Manhã'
                
                for atendente, local in atendentes_puxadas_tarde.items():
                    mask = df_leadsByUser['name'] == atendente
                    if mask.any():
                        df_leadsByUser.loc[mask, 'local'] = local
                        df_leadsByUser.loc[mask, 'turno'] = 'Tarde'
                
                # Add a default team column (can be customized later)
                df_leadsByUser['Tam'] = 'P'  # Default team
                
                # Rename columns
                df_leadsByUser = df_leadsByUser.rename(columns={
                    'name': 'Atendente',
                    'messages_count': 'Leads Puxados',
                    'unique_messages_count': 'Leads Puxados (únicos)',
                    'agendamentos_por_lead': 'Agendamentos por lead',
                    'local': 'Unidade',
                    'Tam': 'Tam',
                    'turno': 'Turno',
                    'success_rate': 'Conversão'
                })
                
                # Reset index and sort
                df_leadsByUser = df_leadsByUser.reset_index(drop=True)
                df_leadsByUser = df_leadsByUser.sort_values(by='Leads Puxados (únicos)', ascending=False)
                
                # Filter to show only attendants from the lists
                all_atendentes = list(atendentes_puxadas_manha.keys()) + list(atendentes_puxadas_tarde.keys())
                df_atendentes_manha = df_leadsByUser[df_leadsByUser['Atendente'].isin(atendentes_puxadas_manha.keys())]
                df_atendentes_tarde = df_leadsByUser[df_leadsByUser['Atendente'].isin(atendentes_puxadas_tarde.keys())]
                
                # Define display columns order
                display_columns = ['Atendente', 'Unidade', 'Turno', 'Tam',
                                    'Leads Puxados', 'Leads Puxados (únicos)',
                                    'Agendamentos por lead', 'Conversão']
                
                # Display dataframes
                st.subheader("Atendentes do Turno da Manhã")
                st.dataframe(df_atendentes_manha[display_columns], hide_index=True)
                
                st.subheader("Atendentes do Turno da Tarde")
                st.dataframe(df_atendentes_tarde[display_columns], hide_index=True)