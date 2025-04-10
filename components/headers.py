import streamlit as st

def header_leads(df_leads):
    st.header("Visão Geral")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total de leads", len(df_leads))
    with col2:
        st.metric("Total de Unidades", df_leads['Unidade'].nunique())
    with col3:
        total_days_count = df_leads['Dia'].nunique()
        st.metric("Dias Úteis", total_days_count)
    with col4:
        avg_leads_per_day = len(df_leads) / df_leads['Dia'].nunique()
        st.metric("Média de leads/Dia", f"{avg_leads_per_day:.1f}")
    st.markdown("---")
    
def header_sales(df_sales):
    st.header("Visão Geral")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total de sales", len(df_sales))
    with col2:
        st.metric("Total de Unidades", df_sales['Unidade'].nunique())
    with col3:
        total_days_count = df_sales['Dia'].nunique()
        st.metric("Dias Úteis", total_days_count)
    with col4:
        avg_sales_per_day = len(df_sales) / df_sales['Dia'].nunique()
        st.metric("Média de sales/Dia", f"{avg_sales_per_day:.1f}")
    st.markdown("---")

def header_appointments(df_appointments):
    st.header("Visão Geral")
    col1, col2, col3, col4 = st.columns(4)
    
    # Total appointments
    with col1:
        st.metric("Total de Agendamentos", len(df_appointments))
    
    # Total units - handle both potential column names
    with col2:
        if 'Unidade do agendamento' in df_appointments.columns:
            st.metric("Total de Unidades", df_appointments['Unidade do agendamento'].nunique())
        elif 'store' in df_appointments.columns:
            st.metric("Total de Unidades", df_appointments['store'].nunique())
        else:
            st.metric("Total de Unidades", 0)
    
    # Total days
    with col3:
        if 'Data' in df_appointments.columns:
            total_days_count = df_appointments['Data'].nunique()
            st.metric("Dias Úteis", total_days_count)
        else:
            st.metric("Dias Úteis", 0)
    
    # Average appointments per day
    with col4:
        if 'Data' in df_appointments.columns and df_appointments['Data'].nunique() > 0:
            avg_appointments_per_day = len(df_appointments) / df_appointments['Data'].nunique()
            st.metric("Média de Agendamentos/Dia", f"{avg_appointments_per_day:.1f}")
        else:
            st.metric("Média de Agendamentos/Dia", "0.0")
    
    st.markdown("---")
