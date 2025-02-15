import streamlit as st
from views.page_lead import load_page_leads
from views.page_sales import load_page_sales
from views.page_marketing import load_page_marketing
from views.page_appointments import load_page_appointments

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
        "0 - Marketing", 
        "2 - Vendas",
        "10 - Leads",
        "11 - Agendamentos"
        ]
    )
    
    if page == "0 - Marketing":
        load_page_marketing()
    
    elif page == "2 - Vendas":
        load_page_sales()

    elif page == "11 - Agendamentos":
        load_page_appointments()
    
    elif page == "10 - Leads":
        load_page_leads()
    
    
if __name__ == "__main__":
    main()