import streamlit as st
from views.page_lead import load_page_leads
from views.page_sales import load_page_sales
from views.page_marketing import load_page_marketing

st.set_page_config(
    page_title="Dash Pró-Corpo",
    page_icon="📊",
    layout="wide"
)

def main():
    # Sidebar
    st.sidebar.title("Menu")
    page = st.sidebar.selectbox(
        "Selecione a página",
        [
        "2 - Vendas",
        "10 - Leads",
        "11 - Marketing"
        ]
    )
    
    if page == "2 - Vendas":
        load_page_sales()

    if page == "11 - Marketing":
        load_page_marketing()
    
    elif page == "10 - Leads":
        load_page_leads()
    
    
if __name__ == "__main__":
    main()