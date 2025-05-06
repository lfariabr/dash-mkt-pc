import streamlit as st
import pandas as pd
from google.oauth2.service_account import Credentials
from helpers.gsheet import get_gspread_client, get_ss_url
from utils.discord import send_discord_message

def load_page_adminConsultoras():
    """
    Load and display consultoras data from Google Sheets.
    
    Returns:
        df_consultoras: DataFrame containing consultoras data
    """

    st.title("💎 Admin")
    st.markdown("---")
    st.subheader("Lista de Consultoras")

    # Botão para carregar dados ou manter se já estiverem no session_state
    if st.button("Carregar") or "df_consultoras" in st.session_state:
        if "df_consultoras" not in st.session_state:
            send_discord_message(f"Loading data in page adminConsultoras")
            with st.spinner("Carregando dados..."):
                try:
                    spreadsheet_url = get_ss_url()
                    client = get_gspread_client()

                    # Consultoras
                    sheet_name = client.open_by_url(spreadsheet_url)
                    consultoras = sheet_name.worksheet("consultoras")
                    dados_consultoras = consultoras.get_all_values()

                    if not dados_consultoras:
                        st.warning("No data found in the spreadsheet!")
                        st.info("Por favor, verifique suas credenciais e conexão com o Google Sheets.")
                        st.stop()
                    
                    df_consultoras = pd.DataFrame(dados_consultoras[1:], columns=dados_consultoras[0])
                    st.session_state["df_consultoras"] = df_consultoras
                    st.session_state["headers_consultoras"] = dados_consultoras[0]
                    st.session_state["sheet_consultoras"] = consultoras

                except Exception as e:
                    st.error(f"Erro ao carregar dados: {str(e)}")
                    return
            
    if "df_consultoras" in st.session_state:
        
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("##### Consultoras")
            st.dataframe(
                st.session_state["df_consultoras"],
                hide_index=True,
                height=len(st.session_state["df_consultoras"]) * 35,
                # use_container_width=True
            )

        with col2:
            st.markdown("##### Adicionar Consultora")
            with st.form("consultoras_form"):
                col1, col2 = st.columns(2)
                with col1:
                    consultora = st.text_input("Consultora")
                    unidade = st.text_input("Unidade")
                with col2:
                    turno = st.selectbox("Turno", ["Manhã", "Tarde"])
                    tam = st.selectbox("Tam", ["P", "M", "G"])
                submit = st.form_submit_button("Adicionar")

                if submit:
                    try:
                        st.session_state["sheet_consultoras"].append_row([consultora, unidade, turno, tam])
                        st.success(f"Consultora {consultora} inserida com sucesso!")
                        st.warning("Recarregue a página para atualizar os dados.")

                        new_row = pd.DataFrame([[consultora, unidade, turno, tam]],
                                            columns=st.session_state["headers_consultoras"])
                        st.session_state["df_consultoras"] = pd.concat(
                            [st.session_state["df_consultoras"], new_row],
                            ignore_index=True
                        )
                    except Exception as e:
                        st.error(f"Erro ao adicionar consultora: {str(e)}")