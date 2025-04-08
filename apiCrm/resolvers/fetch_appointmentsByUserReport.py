"""
cd /Users/luisfaria/Desktop/sEngineer/dash
python -m apiCrm.tests.fetch_appointmentsByUserReport_test
"""
import asyncio
import logging
from typing import List, Dict
from .fetch_graphql import fetch_graphql
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

logger = logging.getLogger(__name__)

async def fetch_appointmentsByUserReport(session, start_date: str, end_date: str) -> List[Dict]:
    """
    Fetches appointments by user report data from the CRM API within a specified date range.
    Report name: "Agendamento por Atendente"
    Args:
        session: The aiohttp ClientSession object
        start_date: Start date in ISO format (YYYY-MM-DD)
        end_date: End date in ISO format (YYYY-MM-DD)
        
    Returns:
        List of appointments by user report dictionaries
    """
    current_page = 1
    all_appointments = []
    api_url = os.getenv('API_CRM_URL', 'https://open-api.queromeubotox.com.br/graphql')

    # Updated query with non-nullable types (Date!) for date parameters
    query = '''
    query appointmentsByUserReport($start: Date!, $end: Date!) {
        appointmentsByUserReport(
            filters: { createdAtRange: { start: $start, end: $end } }
            pagination: { currentPage: 1, perPage: 100 }
        ) {
            data {
                name
                shiftNumber
                countByProcedureGroup {
                    code
                    label
                }
                appointmentsCount
            }
            meta {
                currentPage
                perPage
                lastPage
                total
            }
        }
    }
    '''
    
    variables = {
        'start': start_date,
        'end': end_date
    }
        
    logger.info(f"Attempting to fetch appointments by user report from {start_date} to {end_date}")
    
    data = await fetch_graphql(session, api_url, query, variables)
    
    if data is None or 'errors' in data:
        error_msg = data.get('errors', [{'message': 'Unknown error'}])[0]['message'] if data else 'No data returned'
        logger.error(f"Failed initial appointments by user report fetch: {error_msg}")
        return all_appointments  # Return empty list on initial failure
        
    # If we got here, the query structure works
    appointments_data = data['data']['appointmentsByUserReport']['data']
    meta = data['data']['appointmentsByUserReport']['meta']
    
    transformed_appointments = []
    for appointment in appointments_data:
        transformed_appointment = {
            'name': appointment.get('name', ''),
            'shift_number': appointment.get('shiftNumber'),
            'appointments_count': appointment.get('appointmentsCount', 0),
            'procedure_counts': {}
        }
        
        # Process countByProcedureGroup
        if 'countByProcedureGroup' in appointment and appointment['countByProcedureGroup']:
            for procedure in appointment['countByProcedureGroup']:
                code = procedure.get('code', '')
                label = procedure.get('label', '0')
                # Convert label to integer (it comes as string from API)
                count = int(label) if label.isdigit() else 0
                transformed_appointment['procedure_counts'][code] = count
        
        # Add timestamps for database records
        transformed_appointment['created_at'] = datetime.now().isoformat()
        transformed_appointment['report_start_date'] = start_date
        transformed_appointment['report_end_date'] = end_date
                
        # Add to results
        transformed_appointments.append(transformed_appointment)
        
    return transformed_appointments

async def fetch_and_process_appointmentsByUserReport(start_date: str, end_date: str) -> List[Dict]:
    """
    Creates a session and fetches appointments by user report data.
    
    Args:
        start_date: Start date in ISO format (YYYY-MM-DD)
        end_date: End date in ISO format (YYYY-MM-DD)
        
    Returns:
        List of processed appointments by user report dictionaries ready for database insertion
    """
    import aiohttp
    
    async with aiohttp.ClientSession() as session:
        appointments = await fetch_appointmentsByUserReport(session, start_date, end_date)
    
    return appointments