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
    all_leads = []
    api_url = os.getenv('API_CRM_URL', 'https://open-api.eprocorpo.com.br/graphql')

    # This API exhibits unusual pagination behavior:
    # Page N returns exactly N records
    # The lastPage value reported is unreliable
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
    
    # First, get metadata to understand pagination and fetch first page
    variables = {
        'start': start_date,
        'end': end_date,
        'currentPage': 1,
        'perPage': 1000  # This is ignored by the API but we include it anyway
    }
    
    data = await fetch_graphql(session, api_url, query, variables)
    
    if data is None or 'errors' in data:
        error_msg = data.get('errors', [{'message': 'Unknown error'}])[0]['message'] if data else 'No data returned'
        logger.error(f"Failed initial leads by user report fetch: {error_msg}")
        return all_leads  # Return empty list on initial failure
    
    try:
        # Process first page (only 1 record)
        if 'data' in data and 'leadsByUserReport' in data['data']:
            leads_data = data['data']['leadsByUserReport']['data']
            meta = data['data']['leadsByUserReport']['meta']
            
            # Add first page data
            page_transformed = process_leads_data(leads_data, start_date, end_date)
            all_leads.extend(page_transformed)
            
            total_records = meta.get('total', 0)
            reported_last_page = meta.get('lastPage', 1)
            
            logger.info(f"Successfully fetched page 1, got {len(leads_data)} records out of approximately {total_records}")
            logger.info(f"API reports lastPage as {reported_last_page}, but this may be inaccurate")
            
            # Rather than trusting the lastPage value, let's find the actual highest valid page
            # using binary search. We know page 1 works, but we're not sure about higher pages.
            
            # Binary search to find the highest valid page
            low = 2  # We already fetched page 1
            high = min(100, reported_last_page)  # Start with a reasonable upper bound
            highest_valid_page = 1  # We know page 1 is valid
            
            logger.info(f"Performing binary search to find highest valid page between 2 and {high}")
            
            while low <= high:
                mid = (low + high) // 2
                variables['currentPage'] = mid
                
                page_data = await fetch_graphql(session, api_url, query, variables)
                
                if page_data is None or 'errors' in page_data:
                    # Error occurred, assume this page is too high
                    high = mid - 1
                    continue
                
                if 'data' in page_data and 'leadsByUserReport' in page_data['data']:
                    page_leads = page_data['data']['leadsByUserReport']['data']
                    
                    if page_leads and len(page_leads) > 0:
                        # This page is valid, try higher
                        highest_valid_page = mid
                        
                        # Process the data while we're here
                        page_transformed = process_leads_data(page_leads, start_date, end_date)
                        all_leads.extend(page_transformed)
                        
                        logger.info(f"Successfully fetched page {mid}, got {len(page_leads)} records during binary search")
                        
                        low = mid + 1
                    else:
                        # No data, try lower
                        high = mid - 1
                else:
                    # Unexpected structure, try lower
                    high = mid - 1
                
                await asyncio.sleep(0.2)  # Add delay to avoid rate limiting
            
            logger.info(f"Binary search complete. Highest valid page found: {highest_valid_page}")
            
            # Now fetch any pages we missed in descending order (to get more records per request)
            # Skip pages we already fetched (1 and highest_valid_page)
            pages_to_fetch = [p for p in range(2, highest_valid_page) if p != highest_valid_page]
            pages_to_fetch.sort(reverse=True)  # Fetch in descending order
            
            logger.info(f"Fetching remaining {len(pages_to_fetch)} pages in descending order")
            
            for page in pages_to_fetch:
                variables['currentPage'] = page
                
                page_data = await fetch_graphql(session, api_url, query, variables)
                
                if page_data is None or 'errors' in page_data:
                    logger.error(f"Failed to fetch page {page}")
                    continue
                
                if 'data' in page_data and 'leadsByUserReport' in page_data['data']:
                    page_leads = page_data['data']['leadsByUserReport']['data']
                    
                    if not page_leads:
                        logger.warning(f"No data returned for page {page}")
                        continue
                    
                    # Process the data
                    page_transformed = process_leads_data(page_leads, start_date, end_date)
                    all_leads.extend(page_transformed)
                    
                    logger.info(f"Successfully fetched page {page}, got {len(page_leads)} records")
                else:
                    logger.error(f"Unexpected API response structure on page {page}")
                
                await asyncio.sleep(0.2)  # Add delay to avoid rate limiting
        else:
            logger.error(f"Unexpected API response structure: {data}")
    
    except Exception as e:
        logger.error(f"Error processing leads by user report data: {str(e)}")
    
    logger.info(f"Total leads by user report records fetched: {len(all_leads)}")
    return all_leads

def process_leads_data(leads_data, start_date, end_date):
    """Helper function to process and transform leads data consistently"""
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