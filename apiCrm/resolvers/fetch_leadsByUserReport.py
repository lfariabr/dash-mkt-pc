"""
cd /Users/luisfaria/Desktop/sEngineer/dash
python -m apiCrm.tests.fetch_leadsByUserReport_test
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

async def fetch_leadsByUserReport(session, start_date: str, end_date: str) -> List[Dict]:
    """
    Fetches leads by user report data from the CRM API within a specified date range.
    Report name: "Leads por Atendente"
    Args:
        session: The aiohttp ClientSession object
        start_date: Start date in ISO format (YYYY-MM-DD)
        end_date: End date in ISO format (YYYY-MM-DD)
        
    Returns:
        List of leads by user report dictionaries
    """
    current_page = 1
    all_leads = []
    api_url = os.getenv('API_CRM_URL', 'https://open-api.queromeubotox.com.br/graphql')

    # Updated query with non-nullable types (Date!) for date parameters
    query = '''
    query leadsByUserReport($start: Date!, $end: Date!) {
        leadsByUserReport(
            filters: { createdAtRange: { start: $start, end: $end } }
            pagination: { currentPage: 1, perPage: 100 }
        ) {
            data {
                name
                shiftNumber
                messagesCount
                uniqueMessagesCount
                messagesCountByStatus {
                    code
                    label
                }
                successRate
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
        
    logger.info(f"Attempting to fetch leads by user report from {start_date} to {end_date}")
    
    data = await fetch_graphql(session, api_url, query, variables)
    
    if data is None or 'errors' in data:
        error_msg = data.get('errors', [{'message': 'Unknown error'}])[0]['message'] if data else 'No data returned'
        logger.error(f"Failed initial leads by user report fetch: {error_msg}")
        return all_leads  # Return empty list on initial failure
        
    # If we got here, the query structure works
    leads_data = data['data']['leadsByUserReport']['data']
    meta = data['data']['leadsByUserReport']['meta']
    
    transformed_leads = []
    for appointment in leads_data:
        transformed_appointment = {
            'name': appointment.get('name', ''),
            'shift_number': appointment.get('shiftNumber'),
            'messages_count': appointment.get('messagesCount', 0),
            'unique_messages_count': appointment.get('uniqueMessagesCount', 0),
            'success_rate': appointment.get('successRate', 0),
            'messages_count_by_status': {}
        }
        
        # Process messagesCountByStatus
        if 'messagesCountByStatus' in appointment and appointment['messagesCountByStatus']:
            for status in appointment['messagesCountByStatus']:
                code = status.get('code', '')
                label = status.get('label', '0')
                # Convert label to integer (it comes as string from API)
                count = int(label) if label.isdigit() else 0
                transformed_appointment['messages_count_by_status'][code] = count
        
        # Add timestamps for database records
        transformed_appointment['created_at'] = datetime.now().isoformat()
        transformed_appointment['report_start_date'] = start_date
        transformed_appointment['report_end_date'] = end_date
                
        # Add to results
        transformed_leads.append(transformed_appointment)
        
    return transformed_leads

async def fetch_and_process_leadsByUserReport(start_date: str, end_date: str) -> List[Dict]:
    """
    Creates a session and fetches leads by user report data.
    
    Args:
        start_date: Start date in ISO format (YYYY-MM-DD)
        end_date: End date in ISO format (YYYY-MM-DD)
        
    Returns:
        List of processed leads by user report dictionaries ready for database insertion
    """
    import aiohttp
    
    async with aiohttp.ClientSession() as session:
        leads = await fetch_leadsByUserReport(session, start_date, end_date)
    
    return leads