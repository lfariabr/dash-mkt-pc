import asyncio
import logging
from typing import List, Dict
from ..fetch_graphql import fetch_graphql
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

logger = logging.getLogger(__name__)

async def fetch_followUpEntriesReport(session, start_date: str, end_date: str) -> List[Dict]:
    """
    Fetches follow-ups entries report data from the CRM API within a specified date range.

    Args:
        session: The aiohttp ClientSession object
        start_date: Start date in ISO format (YYYY-MM-DD)
        end_date: End date in ISO format (YYYY-MM-DD)
        
    Returns:
        List of follow-ups entries report dictionaries
    """
    all_followUpEntries = []
    api_url = os.getenv('API_CRM_URL', 'https://open-api.eprocorpo.com.br/graphql')
    
    # Updated query with non-nullable types (Date!) for date parameters
    query = '''
    query FollowUpEntriesReport($start: Date!, $end: Date!, $currentPage: Int!, $perPage: Int!) {
        followUpEntriesReport(
            filters: { createdAtRange: { start: $start, end: $end } }
            pagination: { currentPage: $currentPage, perPage: $perPage }
        ) {
            data {
                customerIds
                followUpsCount
                name
            }
            meta {
                total
                currentPage
                perPage
                lastPage
            }
        }
    }
    '''
    
    # First, get metadata to understand pagination and fetch first page
    variables = {
        "start": start_date,
        "end": end_date,
        "currentPage": 1,
        "perPage": 400  # Keep the original perPage value
    }
    
    logger.info(f"Attempting to fetch follow-ups entries report from {start_date} to {end_date}")
    
    data = await fetch_graphql(session, api_url, query, variables)
    
    if data is None or 'errors' in data:
        error_msg = data.get('errors', [{'message': 'Unknown error'}])[0]['message'] if data else 'No data returned'
        logger.error(f"Failed initial follow-ups entries report fetch: {error_msg}")
        return all_followUpEntries  # Return empty list on initial failure
    
    try:
        if 'data' in data and 'followUpEntriesReport' in data['data']:
            # Process first page
            followUpEntries = data['data']['followUpEntriesReport']['data']
            meta = data['data']['followUpEntriesReport']['meta']
            
            # Transform first page of data
            page_transformed = process_followUpEntries_data(followUpEntries, start_date, end_date)
            all_followUpEntries.extend(page_transformed)
            
            last_page = meta.get('lastPage', 1)
            total_records = meta.get('total', 0)
            
            logger.info(f"Successfully fetched page 1/{last_page}, got {len(followUpEntries)} followUpEntries out of approximately {total_records}")
            
            # If we have more pages, fetch them
            if last_page > 1:
                for page in range(2, last_page + 1):
                    variables['currentPage'] = page
                    page_data = await fetch_graphql(session, api_url, query, variables)
                    
                    if page_data is None or 'errors' in page_data:
                        error_msg = page_data.get('errors', [{'message': 'Unknown error'}])[0]['message'] if page_data else 'No data returned'
                        logger.error(f"Failed to fetch followUpEntries on page {page}/{last_page}. Error: {error_msg}")
                        continue
                    
                    if 'data' in page_data and 'followUpEntriesReport' in page_data['data']:
                        page_followUpEntries = page_data['data']['followUpEntriesReport']['data']
                        
                        # Transform data for this page
                        page_transformed = process_followUpEntries_data(page_followUpEntries, start_date, end_date)
                        all_followUpEntries.extend(page_transformed)
                        
                        logger.info(f"Successfully fetched page {page}/{last_page}, got {len(page_followUpEntries)} followUpEntries")
                    else:
                        logger.error(f"Unexpected API response structure on page {page}: {page_data}")
                    
                    # Add a delay between requests to avoid rate limiting
                    await asyncio.sleep(0.5)
        else:
            logger.error(f"Unexpected API response structure: {data}")
    
    except Exception as e:
        logger.error(f"Error processing followUpEntries data: {str(e)}")
    
    logger.info(f"Total followUpEntries fetched: {len(all_followUpEntries)}")
    return all_followUpEntries

def process_followUpEntries_data(followUpEntries, start_date, end_date):
    """Helper function to process and transform followUpEntries data consistently"""
    transformed_followUpEntries = []
    for followUpEntrie in followUpEntries:
        transformed_followUpEntrie = {
            'name': followUpEntrie.get('name', ''),
            'customer_ids': followUpEntrie.get('customerIds', []),
            'follow_ups_count': followUpEntrie.get('followUpsCount', 0),
            # Add report metadata for database storage
            'report_start_date': start_date,
            'report_end_date': end_date,
            'created_at': datetime.now().isoformat()
        }
        transformed_followUpEntries.append(transformed_followUpEntrie)
    
    return transformed_followUpEntries

async def fetch_and_process_followUpEntriesReport(start_date: str, end_date: str) -> List[Dict]:
    """
    Creates a session and fetches follow-up entries report data.
    
    Args:
        start_date: Start date in ISO format (YYYY-MM-DD)
        end_date: End date in ISO format (YYYY-MM-DD)
        
    Returns:
        List of processed follow-up entries report dictionaries ready for database insertion
    """
    import aiohttp
    
    async with aiohttp.ClientSession() as session:
        followUpEntries = await fetch_followUpEntriesReport(session, start_date, end_date)
    
    return followUpEntries
