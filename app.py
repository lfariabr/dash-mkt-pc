import streamlit as st
from views.page_lead import load_page_leads  # Import directly from the module
from views.page_sales import load_page_sales


# Set page config first
st.set_page_config(
    page_title="Dash",
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
        "10 - Leads" 
        ]
    )
    
    if page == "2 - Vendas":
        load_page_sales()
    
    elif page == "10 - Leads":
        load_page_leads()
    
    
if __name__ == "__main__":
    main()