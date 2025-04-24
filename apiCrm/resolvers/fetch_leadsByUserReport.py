"""
cd /Users/luisfaria/Desktop/sEngineer/dash
python -m apiCrm.tests.fetch_leadsByUserReport_test
"""
import os
import asyncio
import logging
from typing import List, Dict
from datetime import datetime
from dotenv import load_dotenv
from .fetch_graphql import fetch_graphql

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
    all_leads = []
    api_url = os.getenv('API_CRM_URL', 'https://open-api.eprocorpo.com.br/graphql')
    query = '''
    query leadsByUserReport($start: Date!, $end: Date!, $currentPage: Int!, $perPage: Int!) {
        leadsByUserReport(
            filters: { createdAtRange: { start: $start, end: $end } }
            pagination: { currentPage: $currentPage, perPage: $perPage }
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
    logger.info(f"Attempting to fetch leads by user report from {start_date} to {end_date}")
    variables = {
        'start': start_date,
        'end': end_date,
        'currentPage': 1,
        'perPage': 400
    }
    batch_size = 10  # Limiting concurrent requests to avoid rate limiting

    # First request to get meta info
    data = await fetch_graphql(session, api_url, query, variables)
    if data is None or 'errors' in data:
        logger.error(f"Failed initial leads by user report fetch: {data.get('errors', 'No data') if data else 'No data'}")
        return []

    leads_by_user = data.get('data', {}).get('leadsByUserReport', {})
    meta = leads_by_user.get('meta', {})
    total = meta.get('total', 0)
    per_page = meta.get('perPage', 400)
    last_page = meta.get('lastPage', 1)
    logger.info(f"API meta: total={total}, perPage={per_page}, lastPage={last_page}")

    # Calculate exact number of pages
    import math
    total_pages = math.ceil(total / per_page) if per_page else 1
    logger.info(f"Calculated total_pages={total_pages}")

    # Starting with first page
    leads_data = leads_by_user.get('data', [])
    if leads_data:
        all_leads.extend(process_leads_data(leads_data, start_date, end_date))
        logger.info(f"Fetched page 1, got {len(leads_data)} records.")

    # Fetching all remaining pages in parallel batches
    async def fetch_page(page):
        v = variables.copy()
        v['currentPage'] = page
        try:
            data = await fetch_graphql(session, api_url, query, v)
            leads_by_user = data.get('data', {}).get('leadsByUserReport', {}) if data else {}
            leads_data = leads_by_user.get('data', [])
            if leads_data:
                logger.info(f"Fetched page {page}, got {len(leads_data)} records.")
                return process_leads_data(leads_data, start_date, end_date)
            else:
                logger.info(f"Page {page} returned no data.")
                return []
        except Exception as e:
            logger.warning(f"Error fetching page {page}: {e}")
            return []

    pages = list(range(2, total_pages + 1))  # already fetched page 1
    for i in range(0, len(pages), batch_size):
        batch = pages[i:i+batch_size]
        tasks = [fetch_page(p) for p in batch]
        batch_results = await asyncio.gather(*tasks)
        for res in batch_results:
            all_leads.extend(res)
    logger.info(f"Total leads by user report records fetched: {len(all_leads)}")
    return all_leads

def process_leads_data(leads_data, start_date, end_date):
    """
    Helper function to process and transform leads data consistently
    """
    import logging
    logger = logging.getLogger(__name__)
    logger.debug(f"Processing {len(leads_data)} records in leads_data")
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
                code = status.get('code')
                label = status.get('label')
                # Convert label to integer (it comes as string from API)
                count = int(label) if label and str(label).isdigit() else 0
                transformed_appointment['messages_count_by_status'][code] = count
        
        # Add timestamps for database records
        from datetime import datetime
        transformed_appointment['created_at'] = datetime.now().isoformat()
        transformed_appointment['report_start_date'] = start_date
        transformed_appointment['report_end_date'] = end_date
                
        # Add to results
        transformed_leads.append(transformed_appointment)
    
    logger.debug(f"Transformed {len(transformed_leads)} records in process_leads_data")
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