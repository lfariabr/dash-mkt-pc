"""
cd /Users/luisfaria/Desktop/sEngineer/dash
python -m apiCrm.tests.fetch_followUpsCommentsReport_test
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
    current_page = 1
    all_appointments = []
    api_url = os.getenv('API_CRM_URL', 'https://open-api.queromeubotox.com.br/graphql')

    # Updated query with non-nullable types (Date!) for date parameters
    query = '''
    query FollowUpsCommentsReport($start: Date!, $end: Date!) {
        followUpsCommentsReport(
            filters: { createdAtRange: { start: $start, end: $end } }
            pagination: { currentPage: 1, perPage: 100 }
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
            }
        }
    }
    '''

    variables = {
        "start": start_date,
        "end": end_date
    }
    
    logger.info(f"Attempting to fetch follow-ups comments report from {start_date} to {end_date}")
    
    data = await fetch_graphql(session, api_url, query, variables)
    
    if data is None or 'errors' in data:
        error_msg = data.get('errors', [{'message': 'Unknown error'}])[0]['message'] if data else 'No data returned'
        logger.error(f"Failed initial follow-ups comments report fetch: {error_msg}")
        return all_appointments  # Return empty list on initial failure
    
    # If we got here, the query structure works
    appointments_data = data['data']['followUpsCommentsReport']['data']
    meta = data['data']['followUpsCommentsReport']['meta']
    
    transformed_appointments = []
    for appointment in appointments_data:
        transformed_appointment = {
            'name': appointment.get('name', ''),
            'comments_count': appointment.get('commentsCount', 0),
            'comments_customer_ids': appointment.get('commentsCustomerIds', []),
            # Add report metadata for database storage
            'report_start_date': start_date,
            'report_end_date': end_date,
            'created_at': datetime.now().isoformat()
        }
        transformed_appointments.append(transformed_appointment)
    
    return transformed_appointments

async def fetch_and_process_followUpsCommentsReport(start_date: str, end_date: str) -> List[Dict]:
    import aiohttp
    
    async with aiohttp.ClientSession() as session:
        appointments = await fetch_followUpsCommentsReport(session, start_date, end_date)
    
    return appointments