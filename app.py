import streamlit as st

# Marketing
from views.marketing_view import load_page_marketing
from views.mkt_leads_view import load_page_mkt_leads

# Dashboard
from views.lead_view import load_page_leads
from views.appointments_view import load_page_appointments
from views.sales_view import load_page_sales

# COC
from views.leadsByUserReport_view import load_page_leadsByUser
from views.followUpReport_view import load_page_followUpReport_and_followUpCommentsReport

st.set_page_config(
    page_title="Pró-Corpo BI",
    page_icon="🤖",
    layout="wide"
)

def main():
    # Define the menu structure
    menu_structure = {
        "Marketing": {
            "1 - Funil": load_page_marketing,
            "2 - Histórico": load_page_mkt_leads,
        },
        "Dashboard": {
            "1 - Leads": load_page_leads,
            "2 - Agendamentos": load_page_appointments,
            "3 - Vendas": load_page_sales,
        },
        "COC": {
            "1 - Puxada de Leads": load_page_leadsByUser,
            "2 - Tarefas de Pós-Vendas": load_page_followUpReport_and_followUpCommentsReport
        }
    }
    
    # Sidebar with two-level selection
    st.sidebar.title("Menu")
    
    # First level - Select category
    category = st.sidebar.radio("Selecione a categoria", list(menu_structure.keys()))
    
    # Second level - Select page within category
    st.sidebar.markdown("---")
    st.sidebar.subheader(f"Páginas - {category}")
    
    pages = list(menu_structure[category].keys())
    selected_page = st.sidebar.radio("Selecione a página", pages, key="page_selector")
    
    # Load the selected page
    menu_structure[category][selected_page]()
    
if __name__ == "__main__":
    main()