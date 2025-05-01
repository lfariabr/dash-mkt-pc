import streamlit as st
import pandas as pd
from google.oauth2.service_account import Credentials
from helpers.gsheet import get_gspread_client, get_ss_url
from utils.discord import send_discord_message

def load_page_adminAtendentes():
    """
    Load and display atendentes data from Google Sheets.
    
    Returns:
        df_atendentes: DataFrame containing atendentes data
    """

    st.title("💎 Admin")
    st.markdown("---")
    st.subheader("Lista de Atendentes")

    # Botão para carregar dados ou manter se já estiverem no session_state
    if st.button("Carregar") or "df_atendentes" in st.session_state:
        if "df_atendentes" not in st.session_state:
            send_discord_message(f"Loading data in page adminAtendentes")
            with st.spinner("Carregando dados..."):
                try:
                    spreadsheet_url = get_ss_url()
                    client = get_gspread_client()

                    # Atendentes
                    sheet_name = client.open_by_url(spreadsheet_url)
                    atendentes = sheet_name.worksheet("atendentes")
                    dados_atendentes = atendentes.get_all_values()

                    if not dados_atendentes:
                        st.warning("No data found in the spreadsheet!")
                        st.info("Por favor, verifique suas credenciais e conexão com o Google Sheets.")
                        st.stop()
                    
                    df_atendentes = pd.DataFrame(dados_atendentes[1:], columns=dados_atendentes[0])
                    st.session_state["df_atendentes"] = df_atendentes
                    st.session_state["headers_atendentes"] = dados_atendentes[0]
                    st.session_state["sheet_atendentes"] = atendentes
                
                except Exception as e:
                    st.error(f"Erro ao carregar dados: {str(e)}")
                    return
                    
                
    if "df_atendentes" in st.session_state:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("##### Atendentes")
            st.dataframe(
                st.session_state["df_atendentes"],
                hide_index=True,
                height=len(st.session_state["df_atendentes"]) * 35,
                # use_container_width=True
            )

        with col2:
            st.markdown("##### Adicionar Atendente")  
            with st.form("atendentes_form"):
                col1, col2 = st.columns(2)
                with col1:
                    atendente = st.text_input("Atendente")
                    unidade = st.text_input("Unidade")
                with col2:
                    turno = st.text_input("Turno")
                    tam = st.text_input("Tam")
                submit = st.form_submit_button("Adicionar")

                if submit:
                    try:
                        st.session_state["sheet_atendentes"].append_row([atendente, unidade, turno, tam])
                        st.success(f"Atendente {atendente} inserido com sucesso!")
                        st.warning("Recarregue a página para atualizar os dados.")

                        new_row = pd.DataFrame([[atendente, unidade, turno, tam]],
                                            columns=st.session_state["headers_atendentes"])
                        st.session_state["df_atendentes"] = pd.concat(
                            [st.session_state["df_atendentes"], new_row],
                            ignore_index=True
                        )
                    except Exception as e:
                        st.error(f"Erro ao adicionar atendente: {str(e)}")