import streamlit as st

# Set page config first
st.set_page_config(
    page_title="Dash",
    page_icon="📊",
    layout="wide"
)

from views import leads

def main():
    # Sidebar
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox(
        "Choose a page",
        ["Leads"]
    )
    
    if page == "Leads":
        leads.show_leads_analytics()

if __name__ == "__main__":
    main()