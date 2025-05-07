import streamlit as st
import pandas as pd
import logging
from sqlalchemy import func, desc, asc
from datetime import datetime, timedelta
import sys
import io

# Add backend to path if needed
sys.path.append('/Users/luisfaria/Desktop/sEngineer/dash')

# Import database components
from backend.database import SessionLocal, engine
from backend.models.mkt_lead import MktLead

# Import helpers from marketing view
from frontend.marketing.marketing_grouper import (
    groupby_marketing_by_category,
    groupby_marketing_by_source,
    groupby_marketing_by_category_and_comprou,
    groupby_marketing_by_source_and_comprou,
    pivot_table_marketing_by_category_and_comprou,
    pivot_table_marketing_by_source_and_comprou
)
from helpers.cleaner import columns_to_hide_from_final_df_leads_appointments_sales

def load_data_from_db(
    limit=1000, 
    offset=0, 
    source_filter=None, 
    store_filter=None, 
    date_from=None, 
    date_to=None,
    category_filter=None,
    purchased_filter=None
):
    """
    Load data from MktLead table with various filtering options
    """
    session = SessionLocal()
    try:
        # Start building the query
        query = session.query(MktLead)
        
        # Add debug logging
        logging.info(f"Querying MktLead table with filters: source={source_filter}, store={store_filter}, category={category_filter}, purchased={purchased_filter}")
        
        # Get total count before any filters to verify data exists
        total_before_filter = query.count()
        logging.info(f"Total records in MktLead table before filtering: {total_before_filter}")
        
        # Apply filters if provided
        if source_filter:
            query = query.filter(MktLead.lead_source == source_filter)
        
        if store_filter:
            query = query.filter(MktLead.lead_store == store_filter)
        
        if date_from:
            # Use the correct field name for date filtering
            query = query.filter(MktLead.lead_entry_day >= date_from.day if hasattr(MktLead, 'lead_entry_day') else True)
            logging.info(f"Date filter from: {date_from}")
            
        if date_to:
            # Use the correct field name for date filtering
            query = query.filter(MktLead.lead_entry_day <= date_to.day if hasattr(MktLead, 'lead_entry_day') else True)
            logging.info(f"Date filter to: {date_to}")
            
        if category_filter:
            query = query.filter(MktLead.lead_category == category_filter)
            
        if purchased_filter is not None:
            query = query.filter(MktLead.sales_purchased == purchased_filter)
        
        # Get total count for pagination
        total_count = query.count()
        logging.info(f"Total records after filtering: {total_count}")
        
        # Order by most recent leads first
        query = query.order_by(desc(MktLead.lead_id))
        
        # Apply pagination
        query = query.limit(limit).offset(offset)
        
        # Execute query and fetch results
        results = query.all()
        logging.info(f"Retrieved {len(results)} records after pagination")
        
        # Convert to DataFrame
        data = []
        for lead in results:
            data.append({
                "ID": lead.lead_id,
                "Email": lead.lead_email,
                "Telefone": lead.lead_phone,
                "Mensagem": lead.lead_message,
                "Unidade": lead.lead_store,
                "Fonte": lead.lead_source,
                "Data de Entrada": None,  # Adjust based on what's available
                "Categoria": lead.lead_category,
                "Data Agendamento": lead.appointment_date,
                "Status Agendamento": lead.appointment_status,
                "Comprou": "Sim" if lead.sales_purchased else "Não",
                "Data de Compra": lead.sales_date,
                "Valor": lead.sales_total_bought
            })
        
        df = pd.DataFrame(data)
        
        return df, total_count
        
    except Exception as e:
        logging.error(f"Error loading data from database: {str(e)}")
        return pd.DataFrame(), 0
    finally:
        session.close()

def get_filter_options():
    """Get unique values for filters from database"""
    session = SessionLocal()
    try:
        sources = [source[0] for source in session.query(MktLead.lead_source).distinct().all() if source[0]]
        stores = [store[0] for store in session.query(MktLead.lead_store).distinct().all() if store[0]]
        categories = [cat[0] for cat in session.query(MktLead.lead_category).distinct().all() if cat[0]]
        
        return sources, stores, categories
    except Exception as e:
        logging.error(f"Error fetching filter options: {str(e)}")
        return [], [], []
    finally:
        session.close()

def load_page_mkt_leads():
    """Main function to display MktLead data"""
    st.title("📊 Marketing Leads Database")
    
    # Get filter options
    sources, stores, categories = get_filter_options()
    
    # Sidebar filters
    st.sidebar.header("Filtros")
    
    # Date filters
    st.sidebar.subheader("Filtro por Data")
    date_filter = st.sidebar.selectbox(
        "Filtrar por período",
        ["Todos", "Últimos 7 dias", "Últimos 30 dias", "Últimos 90 dias", "Personalizado"]
    )
    
    date_from = None
    date_to = None
    
    if date_filter == "Últimos 7 dias":
        date_from = datetime.now() - timedelta(days=7)
    elif date_filter == "Últimos 30 dias":
        date_from = datetime.now() - timedelta(days=30)
    elif date_filter == "Últimos 90 dias":
        date_from = datetime.now() - timedelta(days=90)
    elif date_filter == "Personalizado":
        col1, col2 = st.sidebar.columns(2)
        with col1:
            date_from = st.date_input("De", datetime.now() - timedelta(days=30))
        with col2:
            date_to = st.date_input("Até", datetime.now())
    
    # Source filter
    source_filter = st.sidebar.selectbox(
        "Fonte",
        ["Todos"] + sources
    )
    if source_filter == "Todos":
        source_filter = None
    
    # Store filter
    store_filter = st.sidebar.selectbox(
        "Unidade",
        ["Todos"] + stores
    )
    if store_filter == "Todos":
        store_filter = None
    
    # Category filter
    category_filter = st.sidebar.selectbox(
        "Categoria",
        ["Todos"] + categories
    )
    if category_filter == "Todos":
        category_filter = None
    
    # Purchase filter
    purchased_filter = st.sidebar.selectbox(
        "Status de Compra",
        ["Todos", "Comprou", "Não Comprou"]
    )
    if purchased_filter == "Todos":
        purchased_filter = None
    elif purchased_filter == "Comprou":
        purchased_filter = True
    else:
        purchased_filter = False
    
    # Pagination settings
    st.sidebar.subheader("Paginação")
    items_per_page = st.sidebar.selectbox(
        "Itens por página",
        [10, 20, 50, 100, 30000]
    )
    
    # Load data
    with st.spinner("Carregando dados..."):
        df, total_count = load_data_from_db(
            limit=items_per_page,
            offset=0,
            source_filter=source_filter,
            store_filter=store_filter,
            date_from=date_from,
            date_to=date_to,
            category_filter=category_filter,
            purchased_filter=purchased_filter
        )
    
    # Display data
    if not df.empty:
        st.write(f"Mostrando {len(df)} de {total_count} registros")
        
        # Add pagination controls
        page_count = (total_count + items_per_page - 1) // items_per_page
        
        if page_count > 1:
            cols = st.columns([1, 3, 1])
            with cols[1]:
                page = st.number_input("Página", min_value=1, max_value=page_count, value=1)
                
                offset = (page - 1) * items_per_page
                
                # If page changed, reload data with new offset
                if offset > 0:
                    df, _ = load_data_from_db(
                        limit=items_per_page,
                        offset=offset,
                        source_filter=source_filter,
                        store_filter=store_filter,
                        date_from=date_from,
                        date_to=date_to,
                        category_filter=category_filter,
                        purchased_filter=purchased_filter
                    )
        
        # Show data table
        st.dataframe(df)
        
        # Add export button
        if st.button("Exportar para Excel"):
            # Create a download link
            excel_data = io.BytesIO()
            with pd.ExcelWriter(excel_data, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Leads')
            
            current_date = datetime.now().strftime("%Y%m%d")
            st.download_button(
                label="Download Excel",
                data=excel_data.getvalue(),
                file_name=f"marketing_leads_{current_date}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        
        # COOL STATISTICS section with expander
        with st.container(border=True):
            # Calculate summary statistics
            total_leads = len(df)
            leads_comprou = df[df["Comprou"] == "Sim"]
            total_leads_compraram = len(leads_comprou)
            
            # Convert to numeric if it's not
            if "Valor" in df.columns:
                df["Valor"] = pd.to_numeric(df["Valor"], errors='coerce')
                total_comprado = df["Valor"].sum()
            else:
                total_comprado = 0
            
            # Status counts
            status_counts = df["Status Agendamento"].value_counts()
            atendidos_count = status_counts.get("Atendido", 0)
            agendados_count = status_counts.get("Agendado", 0) + status_counts.get("Confirmado", 0) + \
                            status_counts.get("Falta", 0) + status_counts.get("Cancelado", 0)
            nao_agenda_count = status_counts.get("Não está na agenda", 0)
            
            # Display summary statistics
            st.write("### Resumo da Análise:")
            
            # Calculate percentages safely
            nao_agenda_pct = (nao_agenda_count/total_leads*100) if total_leads > 0 else 0
            agendados_pct = (agendados_count/total_leads*100) if total_leads > 0 else 0
            atendidos_pct = (atendidos_count/total_leads*100) if total_leads > 0 else 0
            compraram_pct = (total_leads_compraram/total_leads*100) if total_leads > 0 else 0
            
            summary_data = {
                "Indicador": ["👀 -> Total de Leads", 
                            "⚠️ -> Não encontrados na Agenda", 
                            "📅 -> Agendado, Confirmado, Falta e Cancelado", 
                            "✅ -> Atendidos", 
                            "🎉 -> Total de leads que compraram", 
                            "💰 -> Total comprado pelos leads"],
                "Valor": [
                    f"{total_leads}",
                    f"{nao_agenda_count} ({nao_agenda_pct:.1f}%)",
                    f"{agendados_count} ({agendados_pct:.1f}%)",
                    f"{atendidos_count} ({atendidos_pct:.1f}%)",
                    f"{total_leads_compraram} ({compraram_pct:.1f}%)",
                    f"R$ {total_comprado:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
                ]
            }
            
            df_summary = pd.DataFrame(summary_data)
            st.dataframe(
                df_summary, 
                use_container_width=True,  
                hide_index=True
            )
        
        # Detailed analysis in expander
        with st.expander("🔍 Detalhes Dinâmica Marketing:"):
            tab1, tab2 = st.tabs(["Visão por Categoria", "Visão por Fonte"])
            
            with tab1:
                st.write("Leads por Categoria e Unidade:")
                try:
                    # Rename columns to match expected format for groupby functions
                    df_for_groupby = df.copy()
                    df_for_groupby.rename(columns={
                        "ID": "ID lead",
                        "Unidade": "Unidade do lead",
                        "Categoria": "Categoria",
                        "Comprou": "comprou"
                    }, inplace=True)
                    
                    # Convert "Sim"/"Não" to boolean
                    df_for_groupby["comprou"] = df_for_groupby["comprou"].map({"Sim": True, "Não": False})
                    
                    # Create pivot table
                    df_leads_by_category = df_for_groupby.groupby(['Categoria', 'Unidade do lead']).size().reset_index(name='ID lead')
                    pivot_table_lead_categoria_loja = df_leads_by_category.pivot_table(
                        index='Categoria', 
                        columns='Unidade do lead', 
                        values='ID lead',
                        aggfunc='sum'
                    )
                    pivot_table_lead_categoria_loja = pivot_table_lead_categoria_loja.fillna(0)
                    st.dataframe(pivot_table_lead_categoria_loja)
                    
                    # Leads by Category and Appointment Status
                    st.write("Leads por Categoria e Status Agenda:")
                    df_leads_by_category_and_status = df_for_groupby.groupby(['Categoria', 'Status Agendamento']).size().reset_index(name='ID lead')
                    pivot_table_lead_categoria_status = df_leads_by_category_and_status.pivot_table(
                        index='Categoria', 
                        columns='Status Agendamento', 
                        values='ID lead',
                        aggfunc='sum'
                    )
                    pivot_table_lead_categoria_status = pivot_table_lead_categoria_status.fillna(0)
                    st.dataframe(pivot_table_lead_categoria_status)
                    
                    # Leads by Category and Purchase Status
                    st.write("Leads por Categoria (Compradores x Valor Total)")
                    df_by_category_purchase = df_for_groupby.groupby(['Categoria', 'comprou']).agg({
                        'ID lead': 'count',
                        'Valor': 'sum'
                    }).reset_index()
                    
                    # Create pivot
                    pivot_category_purchase = df_by_category_purchase.pivot_table(
                        index='Categoria',
                        columns='comprou',
                        values=['ID lead', 'Valor']
                    ).fillna(0)
                    
                    # Flatten the columns
                    pivot_category_purchase.columns = [f"{'Compradores' if c[1] else 'Não Compradores'} - {c[0]}" for c in pivot_category_purchase.columns]
                    st.dataframe(pivot_category_purchase)
                    
                except Exception as e:
                    st.error(f"Erro ao processar dados por categoria: {str(e)}")
            
            with tab2:
                st.write("Leads por Fonte e Unidade:")
                try:
                    # Create pivot table for Source
                    df_leads_by_source = df_for_groupby.groupby(['Fonte', 'Unidade do lead']).size().reset_index(name='ID lead')
                    pivot_table_lead_source_loja = df_leads_by_source.pivot_table(
                        index='Fonte', 
                        columns='Unidade do lead', 
                        values='ID lead',
                        aggfunc='sum'
                    )
                    pivot_table_lead_source_loja = pivot_table_lead_source_loja.fillna(0)
                    st.dataframe(pivot_table_lead_source_loja)
                    
                    # Leads by Source and Appointment Status
                    st.write("Leads por Fonte e Status Agenda:")
                    df_leads_by_source_and_status = df_for_groupby.groupby(['Fonte', 'Status Agendamento']).size().reset_index(name='ID lead')
                    pivot_table_lead_source_status = df_leads_by_source_and_status.pivot_table(
                        index='Fonte', 
                        columns='Status Agendamento', 
                        values='ID lead',
                        aggfunc='sum'
                    )
                    pivot_table_lead_source_status = pivot_table_lead_source_status.fillna(0)
                    st.dataframe(pivot_table_lead_source_status)
                    
                    # Leads by Source and Purchase Status
                    st.write("Leads por Fonte (Compradores x Valor Total)")
                    df_by_source_purchase = df_for_groupby.groupby(['Fonte', 'comprou']).agg({
                        'ID lead': 'count',
                        'Valor': 'sum'
                    }).reset_index()
                    
                    # Create pivot
                    pivot_source_purchase = df_by_source_purchase.pivot_table(
                        index='Fonte',
                        columns='comprou',
                        values=['ID lead', 'Valor']
                    ).fillna(0)
                    
                    # Flatten the columns
                    pivot_source_purchase.columns = [f"{'Compradores' if c[1] else 'Não Compradores'} - {c[0]}" for c in pivot_source_purchase.columns]
                    st.dataframe(pivot_source_purchase)
                    
                except Exception as e:
                    st.error(f"Erro ao processar dados por fonte: {str(e)}")
    else:
        st.info("Nenhum registro encontrado com os filtros selecionados.")
    
    # Show some stats
    if not df.empty:
        st.subheader("Estatísticas")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            purchase_counts = df["Comprou"].value_counts()
            purchase_rate = purchase_counts.get("Sim", 0) / len(df) * 100 if len(df) > 0 else 0
            st.metric("Taxa de Conversão", f"{purchase_rate:.1f}%")
        
        with col2:
            if "Fonte" in df.columns:
                top_source = df["Fonte"].value_counts().idxmax() if not df["Fonte"].empty else "N/A"
                st.metric("Fonte Principal", top_source)
        
        with col3:
            if "Categoria" in df.columns:
                top_category = df["Categoria"].value_counts().idxmax() if not df["Categoria"].empty else "N/A"
                st.metric("Categoria Principal", top_category)

# For direct execution
if __name__ == "__main__":
    load_page_mkt_leads()
