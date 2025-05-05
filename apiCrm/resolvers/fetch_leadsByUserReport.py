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
        'perPage': 20
    }
    
    data = await fetch_graphql(session, api_url, query, variables)
    
    if data is None or 'errors' in data:
        error_msg = data.get('errors', [{'message': 'Unknown error'}])[0]['message'] if data else 'No data returned'
        logger.error(f"Failed initial leads by user report fetch: {error_msg}")
        return all_leads  # Return empty list on initial failure
    
    try:
        if 'data' in data and 'leadsByUserReport' in data['data']:
            # Process first page
            leads_data = data['data']['leadsByUserReport']['data']
            meta = data['data']['leadsByUserReport']['meta']
            
            # Transform first page of data
            page_transformed = process_leads_data(leads_data, start_date, end_date)
            all_leads.extend(page_transformed)
            
            last_page = meta.get('lastPage', 1)
            total_records = meta.get('total', 0)
            
            logger.info(f"Successfully fetched page 1/{last_page}, got {len(leads_data)} leads out of approximately {total_records}")
            
            # If we have more pages, fetch them
            if last_page > 1:
                for page in range(2, last_page + 1):
                    variables['currentPage'] = page
                    page_data = await fetch_graphql(session, api_url, query, variables)
                    
                    if page_data is None or 'errors' in page_data:
                        error_msg = page_data.get('errors', [{'message': 'Unknown error'}])[0]['message'] if page_data else 'No data returned'
                        logger.error(f"Failed to fetch leads by user report on page {page}/{last_page}. Error: {error_msg}")
                        continue
                    
                    if 'data' in page_data and 'leadsByUserReport' in page_data['data']:
                        page_leads = page_data['data']['leadsByUserReport']['data']
                        
                        # Transform data for this page
                        page_transformed = process_leads_data(page_leads, start_date, end_date)
                        all_leads.extend(page_transformed)
                        
                        logger.info(f"Successfully fetched page {page}/{last_page}, got {len(page_leads)} leads")
                    else:
                        logger.error(f"Unexpected API response structure on page {page}: {page_data}")
                    
                    # Add a delay between requests to avoid rate limiting
                    await asyncio.sleep(0.5)
        else:
            logger.error(f"Unexpected API response structure: {data}")
    
    except Exception as e:
        logger.error(f"Error processing leads by user report data: {str(e)}")
    
    logger.info(f"Total leads by user fetched: {len(all_leads)}")
    return all_leads

def process_leads_data(leads, start_date, end_date):
    """
    Helper function to process and transform leads data consistently
    """
    transformed_leads = []
    for lead in leads:
        transformed_lead = {
            'name': lead.get('name', ''),
            'shift_number': lead.get('shiftNumber'),
            'messages_count': lead.get('messagesCount', 0),
            'unique_messages_count': lead.get('uniqueMessagesCount', 0),
            'success_rate': lead.get('successRate', 0),
            'messages_count_by_status': {},
        }
        
        # Process messagesCountByStatus
        if 'messagesCountByStatus' in lead and lead['messagesCountByStatus']:
            for status in lead['messagesCountByStatus']:
                code = status.get('code')
                label = status.get('label')
                # Convert label to integer (it comes as string from API)
                count = int(label) if label and str(label).isdigit() else 0
                transformed_lead['messages_count_by_status'][code] = count
        
        # Add timestamps for database records
        transformed_lead['created_at'] = datetime.now().isoformat()
        transformed_lead['report_start_date'] = start_date
        transformed_lead['report_end_date'] = end_date
        
        transformed_leads.append(transformed_lead)
    
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
        leadsByUserReport = await fetch_leadsByUserReport(session, start_date, end_date)
    
    return leadsByUserReport
