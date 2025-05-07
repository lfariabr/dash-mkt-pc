import streamlit as st

# Marketing
from frontend.st_mkt.marketing_view import load_page_marketing
from frontend.st_mkt.mkt_leads_view import load_page_mkt_leads

# Dashboard
from frontend.st_dash.lead_view import load_page_leads
from frontend.st_dash.appointments_view import load_page_appointments
from frontend.st_dash.sales_view import load_page_sales

# COC
from frontend.st_coc.leadsByUserReport_view import load_page_leadsByUser
from frontend.st_coc.followUpReport_view import load_page_followUpReport_and_followUpCommentsReport
from frontend.st_coc.adminConsultoras import load_page_adminConsultoras
from frontend.st_coc.adminAtendentes import load_page_adminAtendentes
from frontend.st_coc.adminLojas import load_page_adminLojas

from frontend.st_coc.appointmentByUser_view import load_page_appointmentsByUser
from frontend.st_coc.appointments_view_CreatedAt import load_page_appointments_CreatedAt

st.set_page_config(
    page_title="Pró-Corpo BI",
    page_icon="🤖",
    layout="wide"
)

def main():
    """
    Main function for the application.
    
    This function defines the menu structure of the application and uses Streamlit's 
    sidebar to let the user select a category and a page. It then calls the function 
    associated with the selected page.
    """
    # Define the menu structure
    menu_structure = {
        "COC": {
            "1 - Puxada de Leads": load_page_leadsByUser,
            "2 - Tarefas Pós-Vendas": load_page_followUpReport_and_followUpCommentsReport,
            # "3 - Agd Diário": load_page_appointments_CreatedAt,
            # "4 - Agd por Usuário": load_page_appointmentsByUser,
        },
        "Dash": {
            "1 - Leads": load_page_leads,
            "2 - Agendamentos": load_page_appointments,
            "3 - Vendas": load_page_sales,
        },
        "Marketing": {
            "1 - Funil": load_page_marketing,
            # "2 - Histórico": load_page_mkt_leads,
        },
        "Admin": {
            "1 - Consultoras": load_page_adminConsultoras,
            "2 - Atendentes": load_page_adminAtendentes,
            "3 - Lojas": load_page_adminLojas,
        }
    }
    
    st.sidebar.title("Menu")

    category = st.sidebar.radio("Selecione a categoria", list(menu_structure.keys()))
    
    st.sidebar.markdown("---")
    st.sidebar.subheader(f"Páginas - {category}")
    
    pages = list(menu_structure[category].keys())
    selected_page = st.sidebar.radio("Selecione a página", pages, key="page_selector")
    
    menu_structure[category][selected_page]()
    
if __name__ == "__main__":
    main()