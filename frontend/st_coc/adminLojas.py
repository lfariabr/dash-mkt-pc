import streamlit as st
import pandas as pd
from google.oauth2.service_account import Credentials
from helpers.gsheet import get_gspread_client, get_ss_url
from helpers.discord import send_discord_message

def load_page_adminLojas():
    """
    Load and display lojas data from Google Sheets.
    
    Returns:
        df_lojas: DataFrame containing lojas data
    """
    st.title("💎 Admin")
    st.markdown("---")
    st.subheader("Lista de Lojas")

    # Botão para carregar dados ou manter se já estiverem no session_state
    if st.button("Carregar") or "df_lojas" in st.session_state:
        if "df_lojas" not in st.session_state:
            send_discord_message("Loading data in page adminLojas")
            with st.spinner("Carregando dados..."):
                try:
                    spreadsheet_url = get_ss_url()
                    client = get_gspread_client()
                    sheet = client.open_by_url(spreadsheet_url).worksheet("lojas")
                    dados_lojas = sheet.get_all_values()

                    if not dados_lojas:
                        st.warning("No data found in the spreadsheet!")
                        return

                    df_lojas = pd.DataFrame(dados_lojas[1:], columns=dados_lojas[0])
                    st.session_state["df_lojas"] = df_lojas
                    st.session_state["headers_lojas"] = dados_lojas[0]
                    st.session_state["sheet_lojas"] = sheet

                except Exception as e:
                    st.error(f"Erro ao carregar dados: {str(e)}")
                    st.info("Por favor, verifique suas credenciais e conexão com o Google Sheets.")
                    return

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("##### Lojas")
            # Exibe tabela
            st.dataframe(
                st.session_state["df_lojas"],
                hide_index=True,
                height=len(st.session_state["df_lojas"]) * 40,
                use_container_width=True
            )

        with col2:
            st.markdown("##### Adicionar Loja")
            # Formulário para inserir nova loja
            with st.form("lojas_form"):
                loja = st.text_input("Loja")
                tamanho = st.text_input("Tam")
                submit = st.form_submit_button("Adicionar")

                if submit:
                    try:
                        st.session_state["sheet_lojas"].append_row([loja, tamanho])
                        st.success(f"Loja {loja} inserida com sucesso!")
                        st.warning("Recarregue a página para atualizar os dados.")

                        # Atualiza também o DataFrame no session_state
                        new_row = pd.DataFrame([[loja, tamanho]], columns=st.session_state["headers_lojas"])
                        st.session_state["df_lojas"] = pd.concat([st.session_state["df_lojas"], new_row], ignore_index=True)
                    except Exception as e:
                        st.error(f"Erro ao adicionar nova loja: {str(e)}")