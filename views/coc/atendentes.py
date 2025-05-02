import streamlit as st
import pandas as pd
from google.oauth2.service_account import Credentials
from helpers.gsheet import get_gspread_client, get_ss_url
from utils.discord import send_discord_message

atendentes_puxadas_manha = {
    'Geovanna Maynara Soares' : 'Sorocaba',
    'Marta Maria Palma' : 'Jardins',
    'Flavia Feitosa da Silva' : 'Mooca',
    'Camilly Giselli Barros e Barros' : 'Osasco',
    'Beatriz Nogueira de Oliveira' : 'Vila Mascote',
    'Iasmin Monteiro Dias dos Santos' : 'Ipiranga',
    'Larissa Rodrigues da Silva' : 'Alphaville',
    'Aline Araujo de Oliveira' : 'Guarulhos',
    'Sarah de Jesus dos Santos Pilatos' : 'Ipiranga',
    'Renally Carla da Silva Moura' : 'Copacabana',
    'Yasmin Veronez Ramos' : 'São Bernardo',
    'Camylli Victoria Lonis Silva' : 'Tucuruvi',
    'Thamyres Lima Marques' : 'Tijuca',
    'Letícia Moreira Valentim' : 'Tatuapé',
    'Gabriela Gomes Magalhaes dos Anjos' : 'Santo Amaro',
    'Larissa Sabino dos Santos' : 'Moema',
    'Nicolly Alves dos Santos' : 'Lapa',
    'Loren Schiavo Araujo' : 'Santos'
}

atendentes_puxadas_tarde = {
    'Ingrid Caroline Santos Andrade' : 'Campinas',
    'Amanda Raquel Cassula' : 'Londrina',
    'Iasmin Monteiro Dias dos Santos' : 'Ipiranga',
    'Jihad Pereira dos Santos' : 'Ipiranga',
    'Beatriz da Silva Nunes' : 'Jardins',
    'Gabrielly Cristina Silva dos Santos' : 'Tatuapé',
    'Vitoria Almeida Silva' : 'Osasco',
    'Sarah leal oliveira' : 'Vila Mascote',
    'Aryanne de jesus luiz' : 'Itaim',
    'Jenniffer Lopes Romão da Silva' : 'Tucuruvi',
    'Agata Andrelli Oliveira Vieira' : 'Jardins',
    'Giovanna Vitoria Cota Mascarenhas' : 'Tatuapé',
    'Luisa Yuka Hiraide Soares' : 'Copacabana',
    'Fernanda Machado Leite' : 'Tijuca',
    'Giovanna Maia Alves' : 'Santos',
    'Thais Dias Souza Vasquez' : 'Moema',
    'Ana Luiza Silva Martins' : 'Santo Amaro',
    'Luana da Silva Belizario' : 'Lapa',
    'Alany Melo de Souza' : 'Santos'
}


def get_atendente_from_spreadsheet():
    """
    Get atendentes data from Google Sheets and return DataFrames for morning and afternoon shifts.
    
    Returns:
        tuple: (df_atendentes_manha, df_atendentes_tarde)
    
    Raises:
        Exception: If there's an error accessing the spreadsheet
    """
    try:
        spreadsheet_url = get_ss_url()
        client = get_gspread_client()

        # Atendentes
        sheet_name = client.open_by_url(spreadsheet_url)
        atendentes = sheet_name.worksheet("atendentes")
        dados_atendentes = atendentes.get_all_values()
        
        if len(dados_atendentes) <= 1:  # Only header or empty
            st.warning("Planilha de atendentes vazia ou sem dados")
            return pd.DataFrame(), pd.DataFrame()
            
        df_atendentes = pd.DataFrame(dados_atendentes[1:], columns=dados_atendentes[0])
        
        # Ensure required columns exist
        required_columns = ['Atendente', 'Unidade', 'Turno', 'Tam']
        if not all(col in df_atendentes.columns for col in required_columns):
            st.error(f"Planilha de atendentes deve conter as colunas: {', '.join(required_columns)}")
            return pd.DataFrame(), pd.DataFrame()
        
        # Filter by shift
        df_atendentes_manha = df_atendentes[df_atendentes['Turno'].str.strip().str.lower() == 'manhã'].copy()
        df_atendentes_tarde = df_atendentes[df_atendentes['Turno'].str.strip().str.lower() == 'tarde'].copy()
        
        # Clean up data
        for df in [df_atendentes_manha, df_atendentes_tarde]:
            df['Atendente'] = df['Atendente'].str.strip()
            df['Unidade'] = df['Unidade'].str.strip()
        
        return df_atendentes_manha, df_atendentes_tarde
        
    except Exception as e:
        st.error(f"Erro ao carregar dados dos atendentes: {str(e)}")
        return pd.DataFrame(), pd.DataFrame()
