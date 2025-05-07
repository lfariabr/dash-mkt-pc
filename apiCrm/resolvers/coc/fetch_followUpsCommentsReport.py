import asyncio
import logging
from typing import List, Dict
from ..fetch_graphql import fetch_graphql
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

logger = logging.getLogger(__name__)

async def fetch_followUpsCommentsReport(session, start_date: str, end_date: str) -> List[Dict]:
    """
    Fetches follow-ups comments report data from the CRM API within a specified date range.
    
    Args:
        session: The aiohttp ClientSession object
        start_date: Start date in ISO format (YYYY-MM-DD)
        end_date: End date in ISO format (YYYY-MM-DD)
        
    Returns:
        List of follow-ups comments report dictionaries
    """
    all_followUpsComments = []
    api_url = os.getenv('API_CRM_URL', 'https://open-api.eprocorpo.com.br/graphql')
    
    # Updated query with non-nullable types (Date!) for date parameters
    query = '''
    query FollowUpsCommentsReport($start: Date!, $end: Date!, $currentPage: Int!, $perPage: Int!) {
        followUpsCommentsReport(
            filters: { createdAtRange: { start: $start, end: $end } }
            pagination: { currentPage: $currentPage, perPage: $perPage }
        ) {
            data {
                commentsCount
                commentsCustomerIds
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
    
    logger.info(f"Attempting to fetch follow-ups comments report from {start_date} to {end_date}")
    
    data = await fetch_graphql(session, api_url, query, variables)
    
    if data is None or 'errors' in data:
        error_msg = data.get('errors', [{'message': 'Unknown error'}])[0]['message'] if data else 'No data returned'
        logger.error(f"Failed initial follow-ups comments report fetch: {error_msg}")
        return all_followUpsComments  # Return empty list on initial failure
    
    try:
        if 'data' in data and 'followUpsCommentsReport' in data['data']:
            # Process first page
            appointments_data = data['data']['followUpsCommentsReport']['data']
            meta = data['data']['followUpsCommentsReport']['meta']
            
            # Transform first page of data
            page_transformed = process_followUpsComments_data(appointments_data, start_date, end_date)
            all_followUpsComments.extend(page_transformed)
            
            last_page = meta.get('lastPage', 1)
            total_records = meta.get('total', 0)
            
            logger.info(f"Successfully fetched page 1/{last_page}, got {len(appointments_data)} followUpsComments out of approximately {total_records}")
            
            # If we have more pages, fetch them
            if last_page > 1:
                for page in range(2, last_page + 1):
                    variables['currentPage'] = page
                    page_data = await fetch_graphql(session, api_url, query, variables)
                    
                    if page_data is None or 'errors' in page_data:
                        error_msg = page_data.get('errors', [{'message': 'Unknown error'}])[0]['message'] if page_data else 'No data returned'
                        logger.error(f"Failed to fetch followUpsComments on page {page}/{last_page}. Error: {error_msg}")
                        continue
                    
                    if 'data' in page_data and 'followUpsCommentsReport' in page_data['data']:
                        page_followUpsComments = page_data['data']['followUpsCommentsReport']['data']
                        
                        # Transform data for this page
                        page_transformed = process_followUpsComments_data(page_followUpsComments, start_date, end_date)
                        all_followUpsComments.extend(page_transformed)
                        
                        logger.info(f"Successfully fetched page {page}/{last_page}, got {len(page_followUpsComments)} followUpsComments")
                    else:
                        logger.error(f"Unexpected API response structure on page {page}: {page_data}")
                    
                    # Add a delay between requests to avoid rate limiting
                    await asyncio.sleep(0.5)
        else:
            logger.error(f"Unexpected API response structure: {data}")
    
    except Exception as e:
        logger.error(f"Error processing followUpsComments data: {str(e)}")
    
    logger.info(f"Total followUpsComments fetched: {len(all_followUpsComments)}")
    return all_followUpsComments

def process_followUpsComments_data(followUpsComments, start_date, end_date):
    """Helper function to process and transform followUpsComments data consistently"""
    transformed_followUpsComments = []
    for followUpComment in followUpsComments:
        transformed_followUpComment = {
            'name': followUpComment.get('name', ''),
            'comments_count': followUpComment.get('commentsCount', 0),
            'comments_customer_ids': followUpComment.get('commentsCustomerIds', []),
            # Add report metadata for database storage
            'report_start_date': start_date,
            'report_end_date': end_date,
            'created_at': datetime.now().isoformat()
        }
        transformed_followUpsComments.append(transformed_followUpComment)
    
    return transformed_followUpsComments

async def fetch_and_process_followUpsCommentsReport(start_date: str, end_date: str) -> List[Dict]:
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
        followUpsComments = await fetch_followUpsCommentsReport(session, start_date, end_date)
    
    return followUpsComments
