import streamlit as st
import pandas as pd
from google.oauth2.service_account import Credentials
from helpers.gsheet import get_gspread_client, get_ss_url

consultoras_manha = {
    'Beatriz Emanoela da Silva' : 'Tatuapé',
    'Alessandra Araújo de Oliveira' : 'Tatuapé',
    'Gabriela Rodrigues Evaristo Barbosa' : 'Osasco',
    'Estefani Lima Santos' : 'Santo Amaro',
    'Joyce Bearari da Silva' : 'Campinas',
    'Anna Flavia Medeiros Paiva' : 'Tijuca',
    'Leticia Araujo Dos Santos' : 'Lapa',
    'Larissa Teixeira da Rocha' : 'Tucuruvi',
    'Laryssa Benicio Santos' : 'Londrina',
    'Bruna Nascimento Rangel' : 'Copacabana',
    'Barbara Sumire Ireijo Bravo' : 'Santos',
    'Carla Tais de Souza Lamego' : 'Jardins',
    'Julia Bigliazzi Amorim Nogueira' : 'Ipiranga',
    'Diana da Silva Sousa' : 'Jardins',
    'Claudia Vitoria Xavier da Silva' : 'Moema'
}

consultoras_tarde = {
    'Beatriz Abrantes Duarte' : 'Ipiranga',
    'Eskarlete Kloh Matos' : 'Osasco',
    'Kathelyn Matoso Pinheiro dos Santos' : 'Santo Amaro',
    'Yasmin Emanuele Dal-Bó Teixeira' : 'Londrina',
    'Jessica Oliveira de Azevedo' : 'Santo Amaro',
    'Larissa Gabriely Fonseca de Lima' : 'Campinas',
    'Ana Carolina da Silva Coloma' : 'Copacabana',
    'Luana Rodrigues Parrillo' : 'Santos',
    'Caroline aparecida dos Santos Costa' : 'Lapa',
    'Vanessa Trajano Lopes' : 'Santos',
    'Lorrana Assis Santana de Souza' : 'Tatuapé',
    'Pamela Sabrina do Nascimento Lima' : 'Vila Mascote',
    'Jacqueline Santos da Silva' : 'Guarulhos',
    'Isadora Cristina Harder de Almeida' : 'Sorocaba',
    'Lawany Fernanda dos Santos' : 'São Bernardo',
    'Sabrina Caroline de Jesus Ferreira' : 'Moema',
    'Gisely Gutierres Araújo Conservani' : 'Sorocaba',
    'Ethel Castro Flexa Ribeiro Bastos' : 'Tijuca',
    'Thais Silva Lima' : 'Itaim Bibi',
    'Vitoria Lins Ferreira' : 'Mooca',
    'Lara Goncalo Aparicio' : 'Jardins',
    'Ingrid Porciuncula Ferreira da Silva' : 'Jardins'
}

def get_consultora_from_spreadsheet():
    """
    Get atendentes data from Google Sheets and return DataFrames for morning and afternoon shifts.
    
    Returns:
        tuple: (df_consultoras_manha, df_consultoras_tarde)
    
    Raises:
        Exception: If there's an error accessing the spreadsheet
    """
    try:
        spreadsheet_url = get_ss_url()
        client = get_gspread_client()

        # Atendentes
        sheet_name = client.open_by_url(spreadsheet_url)
        consultoras = sheet_name.worksheet("consultoras")
        dados_consultoras = consultoras.get_all_values()
        
        if len(dados_consultoras) <= 1:  # Only header or empty
            st.warning("Planilha de consultoras vazia ou sem dados")
            return pd.DataFrame(), pd.DataFrame()
            
        df_consultoras = pd.DataFrame(dados_consultoras[1:], columns=dados_consultoras[0])
        
        # Ensure required columns exist
        required_columns = ['Consultora', 'Unidade', 'Turno', 'Tam']
        if not all(col in df_consultoras.columns for col in required_columns):
            st.error(f"Planilha de consultoras deve conter as colunas: {', '.join(required_columns)}")
            return pd.DataFrame(), pd.DataFrame()
        
        # Filter by shift
        df_consultoras_manha = df_consultoras[df_consultoras['Turno'].str.strip().str.lower() == 'manhã'].copy()
        df_consultoras_tarde = df_consultoras[df_consultoras['Turno'].str.strip().str.lower() == 'tarde'].copy()
        
        # Clean up data
        for df in [df_consultoras_manha, df_consultoras_tarde]:
            df['Consultora'] = df['Consultora'].str.strip()
            df['Unidade'] = df['Unidade'].str.strip()
        
        return df_consultoras_manha, df_consultoras_tarde
        
    except Exception as e:
        st.error(f"Erro ao carregar dados das consultoras: {str(e)}")
        return pd.DataFrame(), pd.DataFrame()
