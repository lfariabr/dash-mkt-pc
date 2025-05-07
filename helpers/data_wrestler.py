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
from sqlalchemy.orm import Session
from frontend.coc.columns import agendamento_por_lead_column


def safe_date(date_value):
    """
    Safely convert a date value to None if its invalid, NaT or NaN.
    """
    if pd.isna(date_value) or date_value is None or date_value == 'NaT':
        return None
    return date_value

def clean_datetime(value):
    """
    Safely convert any pandas timestamp value to Python datetime or None.
    Handles NaT, None, and string 'NaT' values properly.
    """
    if value is None or pd.isna(value) or str(value) == 'NaT':
        return None
    
    # Ensure non-null timestamps are returned as Python datetime objects
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    
    return value

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
                    existing_lead.appointment_date = clean_datetime(row.get("Data Na Agenda"))
                    existing_lead.appointment_procedure = str(row.get("Procedimento", ""))
                    existing_lead.appointment_status = str(row.get("Status Agenda", ""))
                    existing_lead.appointment_store = str(row.get("Unidade da Agenda", ""))
                    
                    # Sales data
                    existing_lead.sale_cleaned_phone = str(row.get("Telefones Limpos", ""))
                    existing_lead.sales_phone = str(row.get("Telefone(s) do cliente", ""))
                    existing_lead.sales_quote_id = str(row.get("ID orçamento", ""))
                    existing_lead.sales_date = clean_datetime(row.get("Data Venda"))
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
                        appointment_date=clean_datetime(row.get("Data Na Agenda")),
                        appointment_procedure=str(row.get("Procedimento", "")),
                        appointment_status=str(row.get("Status Agenda", "")),
                        appointment_store=str(row.get("Unidade da Agenda", "")),
                        
                        # Sales data
                        sale_cleaned_phone=str(row.get("Telefones Limpos", "")),
                        sales_phone=str(row.get("Telefone(s) do cliente", "")),
                        sales_quote_id=str(row.get("ID orçamento", "")),
                        sales_date=clean_datetime(row.get("Data Venda")),
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

def save_data_to_db_batch(df_leads_with_purchases):
    """
    Batch processing version that handles large datasets more efficiently.
    Processes records in chunks rather than one by one, significantly improving performance.
    
    Args:
        df_leads_with_purchases (pd.DataFrame): DataFrame containing leads, appointments, and sales data
        
    Returns:
        tuple: (success_flag, user_message)
    """
    if df_leads_with_purchases is None or df_leads_with_purchases.empty:
        return False, "No data to save. Please process the data first."
    
    batch_size = 2000
    total_records = len(df_leads_with_purchases)
    total_batches = (total_records + batch_size - 1) // batch_size

    stats = {"processed": 0, "inserted": 0, "updated": 0, "errors": 0}
    overall_success = True
    error_messages = []

    logging.info(f"Start batch processing: {total_records} records, {total_batches} batches with {batch_size} records per batch")

    session = SessionLocal()

    try:
        # Process data in batches
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, total_records)
            current_batch = df_leads_with_purchases.iloc[start_idx:end_idx]

            logging.info(f"Processing batch {batch_num + 1} of {total_batches}")

            to_insert = []
            to_update = []
            
            lead_ids = [str(row.get("ID lead", "")) for _, row in current_batch.iterrows() if row.get("ID lead", "")]
            existing_leads = {lead.lead_id: lead for lead in session.query(MktLead).filter(MktLead.lead_id.in_(lead_ids)).all()}

            for _, row in current_batch.iterrows():
                stats["processed"] += 1

                try:
                    lead_id = str(row.get("ID lead", ""))
                    if not lead_id:
                        logging.warning(f"Skipping record with missing lead ID")
                        stats["errors"] += 1
                        continue

                     # Check if lead exists
                    if lead_id in existing_leads:
                        # Update existing lead
                        existing_lead = existing_leads[lead_id]
                        logging.info(f"Updating existing lead with ID: {lead_id}")
                        
                        # Update fields
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
                        existing_lead.appointment_date = clean_datetime(row.get("Data Na Agenda"))
                        existing_lead.appointment_procedure = str(row.get("Procedimento", ""))
                        existing_lead.appointment_status = str(row.get("Status Agenda", ""))
                        existing_lead.appointment_store = str(row.get("Unidade da Agenda", ""))
                        
                        # Sales data
                        existing_lead.sale_cleaned_phone = str(row.get("Telefones Limpos", ""))
                        existing_lead.sales_phone = str(row.get("Telefone(s) do cliente", ""))
                        existing_lead.sales_quote_id = str(row.get("ID orçamento", ""))
                        existing_lead.sales_date = clean_datetime(row.get("Data Venda"))
                        existing_lead.sales_store = str(row.get("Unidade da Venda", ""))
                        existing_lead.sales_first_quote = str(row.get("Valor primeiro orçamento", ""))
                        existing_lead.sales_total_bought = str(row.get("Total comprado pelo cliente", ""))
                        existing_lead.sales_number_of_quotes = str(row.get("Número de orçamentos do cliente", ""))
                        existing_lead.sales_day = row.get("Dia") if isinstance(row.get("Dia"), int) else None
                        existing_lead.sales_month = str(row.get("Mês da Venda", ""))
                        existing_lead.sales_day_of_week = str(row.get("Dia da Semana", ""))
                        existing_lead.sales_purchased = bool(row.get("comprou", False))
                        existing_lead.sales_interval = row.get("intervalo da compra") if row.get("intervalo da compra") else None
                        
                        to_update.append(existing_lead)
                        stats["updated"] += 1
                    else:
                        # Create new lead
                        logging.info(f"Creating new lead with ID: {lead_id}")
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
                            appointment_date=clean_datetime(row.get("Data Na Agenda")),
                            appointment_procedure=str(row.get("Procedimento", "")),
                            appointment_status=str(row.get("Status Agenda", "")),
                            appointment_store=str(row.get("Unidade da Agenda", "")),
                            
                            # Sales data
                            sale_cleaned_phone=str(row.get("Telefones Limpos", "")),
                            sales_phone=str(row.get("Telefone(s) do cliente", "")),
                            sales_quote_id=str(row.get("ID orçamento", "")),
                            sales_date=clean_datetime(row.get("Data Venda")),
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
                        to_insert.append(new_lead)
                        stats["inserted"] += 1
                        logging.info(f"Count updated: {stats['processed']}")
                
                except Exception as e:
                    logging.error(f"Error processing record {stats['processed']} in batch {batch_num+1}: {str(e)}")
                    stats["errors"] += 1
                    continue
            try:
                # Bulk insert new records
                if to_insert:
                    logging.info(f"Preparing {len(to_insert)} records for bulk insert in batch {batch_num+1}")
                    # Clean all datetime fields before bulk insert
                    for obj in to_insert:
                        obj.appointment_date = clean_datetime(obj.appointment_date)
                        obj.sales_date = clean_datetime(obj.sales_date)
                    
                    # Now perform the bulk insert with clean datetime values
                    session.bulk_save_objects(to_insert)
                    logging.info(f"Bulk inserted {len(to_insert)} new records in batch {batch_num+1}")
                
                # For updates, we've already modified the objects in the session
                if to_update:
                    logging.info(f"Updated {len(to_update)} existing records in batch {batch_num+1}")
                
                # Commit batch
                session.commit()
                logging.info(f"Committed batch {batch_num+1}/{total_batches}")
                
            except SQLAlchemyError as e:
                session.rollback()
                overall_success = False
                error_message = f"Database error in batch {batch_num+1}: {str(e)}"
                error_messages.append(error_message)
                logging.error(error_message)
                continue
            except Exception as e:
                session.rollback()
                overall_success = False
                error_message = f"Unexpected error in batch {batch_num+1}: {str(e)}"
                error_messages.append(error_message)
                logging.error(error_message)
                continue
    
    except Exception as e:
        session.rollback()
        overall_success = False
        error_message = f"Batch processing failed: {str(e)}"
        error_messages.append(error_message)
        logging.error(error_message)
    finally:
        session.close()
    
    # Generate final message
    if overall_success:
        message = f"Successfully processed {stats['processed']} records in {total_batches} batches: {stats['inserted']} inserted, {stats['updated']} updated, {stats['errors']} errors"
        logging.info(message)
        
        user_message = f"""
        ## ✅ Data Saved Successfully with Batch Processing!
        
        **Summary:**
        - Total records processed: {stats['processed']}
        - New records added: {stats['inserted']}
        - Existing records updated: {stats['updated']}
        - Errors encountered: {stats['errors']}
        - Number of batches: {total_batches}
        - Batch size: {batch_size}
        """
    else:
        message = "Batch processing encountered errors: " + "; ".join(error_messages)
        logging.error(message)
        
        user_message = f"""
        ## ⚠️ Batch Processing Completed with Errors
        
        **Summary:**
        - Total records processed: {stats['processed']}
        - New records added: {stats['inserted']}
        - Existing records updated: {stats['updated']}
        - Errors encountered: {stats['errors']}
        - Number of batches: {total_batches}
        
        **Error Details:**
        ```
        {" ".join(error_messages)}
        ```
        
        Some data may have been saved. Please check your database.
        """
    
    return overall_success, user_message

def extract_agendamentos(status_dict):
    """
    Extracts the sum of values from a dictionary where keys are in the list `agendamento_por_lead_column`.
    
    Args:
        status_dict (dict): Dictionary containing status values.
    
    Returns:
        int: Sum of values corresponding to keys in `agendamento_por_lead_column`.
    """
    if isinstance(status_dict, dict):
        return sum(status_dict.get(status, 0) for status in agendamento_por_lead_column)
    return 0

def highlight_total_row(s):
    if s['Consultora de Vendas'] == 'Total':
        return [
            'background-color: #5B2C6F; color: white; font-weight: bold'
        ] * len(s)
    else:
        return [''] * len(s)

def highlight_total_row_leadsByUser(s):
    if s['Atendente'] == 'Total':
        return [
            'background-color: #5B2C6F; color: white; font-weight: bold'
        ] * len(s)
    else:
        return [''] * len(s)

def highlight_total_row_leadsByStore(s):
    if s['Unidade'] == 'Total':
        return [
            'background-color: #5B2C6F; color: white; font-weight: bold'
        ] * len(s)
    else:
        return [''] * len(s)

def enrich_consultora_df(df, consultoras_dict, turno_label):
    for consultora, local in consultoras_dict.items():
        mask = df['Consultora de Vendas'] == consultora
        if mask.any():
            df.loc[mask, 'Unidade'] = local
            df.loc[mask, 'Turno'] = turno_label
    df['Tam'] = 'P'
    return df

def append_totals_row(df, label_col='Consultora de Vendas'):
    totals_row = df.sum(numeric_only=True)

    int_cols = ['Novos Pós-Vendas', 'Comentários de Pós-Vendas', 'Pedidos']
    float_cols = ['Valor líquido']

    for col in int_cols:
        if col in df.columns:
            totals_row[col] = int(round(totals_row[col]))

    for col in float_cols:
        if col in df.columns:
            totals_row[col] = float(round(totals_row[col], 2))

    totals_row[label_col] = 'Total'
    for col in ['Unidade', 'Turno', 'Tam']:
        if col in df.columns:
            totals_row[col] = ''

    return pd.concat([df, totals_row.to_frame().T], ignore_index=True)

def append_total_rows_leadsByStore(df, label_col='Unidade'):
    totals_row = df.sum(numeric_only=True)

    int_cols = ['Leads Puxados', 'Agendamentos por lead', 'Agendamentos na Agenda']

    for col in int_cols:
        if col in df.columns and col in totals_row.index:
            totals_row[col] = int(round(totals_row[col]))

    # Set manual columns that should be empty
    for col in ['Atendente', 'Turno', 'Tam']:
        if col in df.columns:
            totals_row[col] = ''

    totals_row[label_col] = 'Total'

    return pd.concat([df, totals_row.to_frame().T], ignore_index=True)

def append_total_rows_leadsByUser(df, label_col='Atendente'):
    totals_row = df.sum(numeric_only=True)

    int_cols = ['Leads Puxados', 'Leads Puxados (únicos)', 'Agendamentos por lead', 'Agendamentos na Agenda']

    for col in int_cols:
        if col in df.columns and col in totals_row.index:
            totals_row[col] = int(round(totals_row[col]))

    # Set manual columns that should be empty
    for col in ['Conversão', 'Unidade', 'Turno', 'Tam']:
        if col in df.columns:
            totals_row[col] = ''

    totals_row[label_col] = 'Total'

    return pd.concat([df, totals_row.to_frame().T], ignore_index=True)

def enrich_leadsByUser_df(df, atendentes_puxadas_manha, atendentes_puxadas_tarde):
    # Ensure columns exist
    df['Unidade'] = ''
    df['Turno'] = ''
    
    # Merge both dictionaries with shift info
    all_atendentes = {**{k: ('Manhã', v) for k, v in atendentes_puxadas_manha.items()},
                      **{k: ('Tarde', v) for k, v in atendentes_puxadas_tarde.items()}}
    
    for atendente, (turno, local) in all_atendentes.items():
        mask = df['name'] == atendente
        if mask.any():
            df.loc[mask, 'Unidade'] = local
            df.loc[mask, 'Turno'] = turno
    
    df['Tam'] = 'P'
    return df