# helpers/data_wrestler.py
"""
Responsible for getting the data from views/marketing_view.py 
and push it to the database when click on the button is clicked.
"""

import pandas as pd
import logging
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError
from backend.database import SessionLocal, engine
from backend.models.mkt_lead import MktLead

# TODO save in batch

def push_data_to_db(df_leads_with_purchases):
    """
    Process and push leads data to the database.
    
    Args:
        df_leads_with_purchases (pd.DataFrame): DataFrame containing leads, appointments, and sales data
        
    Returns:
        tuple: (success_flag, message, stats)
            - success_flag (bool): True if operation was successful, False otherwise
            - message (str): Status message
            - stats (dict): Statistics about the operation (records processed, inserted, updated)
    """
    session = SessionLocal()
    stats = {"processed": 0, "inserted": 0, "updated": 0, "errors": 0}
    
    try:
        logging.info(f"Starting database push operation for {len(df_leads_with_purchases)} records")
        
        # Begin a transaction
        for _, row in df_leads_with_purchases.iterrows():
            stats["processed"] += 1
            
            try:
                # Check if lead already exists in database
                lead_id = str(row.get("ID lead", ""))
                if not lead_id:
                    logging.warning(f"Skipping record with missing lead ID")
                    stats["errors"] += 1
                    continue
                    
                existing_lead = session.query(MktLead).filter_by(lead_id=lead_id).first()
                
                if existing_lead:
                    logging.info(f"Updating existing lead with ID: {lead_id}")
                    # Update existing lead
                    existing_lead.lead_email = str(row.get("Email do lead", ""))
                    existing_lead.lead_phone = str(row.get("Telefone do lead", ""))
                    existing_lead.lead_message = str(row.get("Mensagem", ""))
                    existing_lead.lead_store = str(row.get("Unidade do lead", ""))
                    existing_lead.lead_source = str(row.get("Fonte", ""))
                    existing_lead.lead_entry_day = row.get("Dia da entrada").day if row.get("Dia da entrada") else None
                    existing_lead.lead_mkt_source = str(row.get("Source", ""))
                    existing_lead.lead_mkt_medium = str(row.get("Medium", ""))
                    existing_lead.lead_mkt_term = str(row.get("Term", ""))
                    existing_lead.lead_mkt_content = str(row.get("Content", ""))
                    existing_lead.lead_mkt_campaign = str(row.get("Campaign", ""))
                    existing_lead.lead_month = str(row.get("Mês do lead", ""))
                    existing_lead.lead_category = str(row.get("Categoria", ""))
                    
                    # Appointment data
                    existing_lead.appointment_date = row.get("Data Na Agenda")
                    existing_lead.appointment_procedure = str(row.get("Procedimento", ""))
                    existing_lead.appointment_status = str(row.get("Status Agenda", ""))
                    existing_lead.appointment_store = str(row.get("Unidade da Agenda", ""))
                    
                    # Sales data
                    existing_lead.sale_cleaned_phone = str(row.get("Telefones Limpos", ""))
                    existing_lead.sales_phone = str(row.get("Telefone(s) do cliente", ""))
                    existing_lead.sales_quote_id = str(row.get("ID orçamento", ""))
                    existing_lead.sales_date = row.get("Data Venda")
                    existing_lead.sales_store = str(row.get("Unidade da Venda", ""))
                    existing_lead.sales_first_quote = str(row.get("Valor primeiro orçamento", ""))
                    existing_lead.sales_total_bought = str(row.get("Total comprado pelo cliente", ""))
                    existing_lead.sales_number_of_quotes = str(row.get("Número de orçamentos do cliente", ""))
                    existing_lead.sales_day = row.get("Dia") if isinstance(row.get("Dia"), int) else None
                    existing_lead.sales_month = str(row.get("Mês da Venda", ""))
                    existing_lead.sales_day_of_week = str(row.get("Dia da Semana", ""))
                    existing_lead.sales_purchased = bool(row.get("comprou", False))
                    existing_lead.sales_interval = row.get("intervalo da compra") if row.get("intervalo da compra") else None
                    
                    stats["updated"] += 1
                else:
                    logging.info(f"Creating new lead with ID: {lead_id}")
                    # Create new lead record
                    new_lead = MktLead(
                        lead_id=lead_id,
                        lead_email=str(row.get("Email do lead", "")),
                        lead_phone=str(row.get("Telefone do lead", "")),
                        lead_message=str(row.get("Mensagem", "")),
                        lead_store=str(row.get("Unidade do lead", "")),
                        lead_source=str(row.get("Fonte", "")),
                        lead_entry_day=row.get("Dia da entrada").day if row.get("Dia da entrada") else None,
                        lead_mkt_source=str(row.get("Source", "")),
                        lead_mkt_medium=str(row.get("Medium", "")),
                        lead_mkt_term=str(row.get("Term", "")),
                        lead_mkt_content=str(row.get("Content", "")),
                        lead_mkt_campaign=str(row.get("Campaign", "")),
                        lead_month=str(row.get("Mês do lead", "")),
                        lead_category=str(row.get("Categoria", "")),
                        
                        # Appointment data
                        appointment_date=row.get("Data Na Agenda"),
                        appointment_procedure=str(row.get("Procedimento", "")),
                        appointment_status=str(row.get("Status Agenda", "")),
                        appointment_store=str(row.get("Unidade da Agenda", "")),
                        
                        # Sales data
                        sale_cleaned_phone=str(row.get("Telefones Limpos", "")),
                        sales_phone=str(row.get("Telefone(s) do cliente", "")),
                        sales_quote_id=str(row.get("ID orçamento", "")),
                        sales_date=row.get("Data Venda"),
                        sales_store=str(row.get("Unidade da Venda", "")),
                        sales_first_quote=str(row.get("Valor primeiro orçamento", "")),
                        sales_total_bought=str(row.get("Total comprado pelo cliente", "")),
                        sales_number_of_quotes=str(row.get("Número de orçamentos do cliente", "")),
                        sales_day=row.get("Dia") if isinstance(row.get("Dia"), int) else None,
                        sales_month=str(row.get("Mês da Venda", "")),
                        sales_day_of_week=str(row.get("Dia da Semana", "")),
                        sales_purchased=bool(row.get("comprou", False)),
                        sales_interval=row.get("intervalo da compra") if row.get("intervalo da compra") else None
                    )
                    session.add(new_lead)
                    stats["inserted"] += 1
                    logging.info(f"Count updated: {stats['processed']}")
                    
            except Exception as e:
                logging.error(f"Error processing record {stats['processed']}: {str(e)}")
                stats["errors"] += 1
                continue
                
        # Commit the transaction
        session.commit()
        success = True
        message = f"Successfully processed {stats['processed']} records: {stats['inserted']} inserted, {stats['updated']} updated, {stats['errors']} errors"
        logging.info(message)
        
    except SQLAlchemyError as e:
        session.rollback()
        success = False
        message = f"Database error: {str(e)}"
        logging.error(message)
    except Exception as e:
        session.rollback()
        success = False
        message = f"Unexpected error: {str(e)}"
        logging.error(message)
    finally:
        session.close()
        
    return success, message, stats


def save_data_to_db(df_leads_with_purchases):
    """
    Wrapper function to be called from the UI that handles the database operation
    and returns appropriate status messages for the user interface.
    
    Args:
        df_leads_with_purchases (pd.DataFrame): DataFrame containing leads, appointments, and sales data
        
    Returns:
        tuple: (success_flag, user_message)
    """
    if df_leads_with_purchases is None or df_leads_with_purchases.empty:
        return False, "No data to save. Please process the data first."
    
    success, message, stats = push_data_to_db(df_leads_with_purchases)
    
    if success:
        user_message = f"""
        ## ✅ Data Saved Successfully!
        
        **Summary:**
        - Total records processed: {stats['processed']}
        - New records added: {stats['inserted']}
        - Existing records updated: {stats['updated']}
        - Errors encountered: {stats['errors']}
        """
    else:
        user_message = f"""
        ## ❌ Error Saving Data
        
        There was a problem saving your data to the database:
        
        ```
        {message}
        ```
        
        Please try again or contact support.
        """
    
    return success, user_message