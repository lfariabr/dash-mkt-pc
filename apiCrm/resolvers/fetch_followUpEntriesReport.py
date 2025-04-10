"""
cd /Users/luisfaria/Desktop/sEngineer/dash
python -m apiCrm.tests.fetch_followUpEntriesReport_test
"""

import asyncio
import logging
from typing import List, Dict
from .fetch_graphql import fetch_graphql
from dotenv import load_dotenv
import os
from datetime import datetime
import streamlit as st

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
    all_appointments = []
    api_url = os.getenv('API_CRM_URL', 'https://open-api.eprocorpo.com.br/graphql')

    # This API exhibits unusual pagination behavior:
    # Page N returns exactly N records
    # The lastPage value reported is unreliable
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
        return all_appointments  # Return empty list on initial failure
    
    try:
        # Process first page
        if 'data' in data and 'followUpEntriesReport' in data['data']:
            appointments_data = data['data']['followUpEntriesReport']['data']
            meta = data['data']['followUpEntriesReport']['meta']
            
            # Transform and process first page
            page_transformed = process_appointments_data(appointments_data, start_date, end_date)
            all_appointments.extend(page_transformed)
            
            total_records = meta.get('total', 0)
            reported_last_page = meta.get('lastPage', 1)
            
            logger.info(f"Successfully fetched page 1, got {len(appointments_data)} records out of approximately {total_records}")
            logger.info(f"API reports lastPage as {reported_last_page}, but this may be inaccurate")
            
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
                
                if 'data' in page_data and 'followUpEntriesReport' in page_data['data']:
                    page_appointments = page_data['data']['followUpEntriesReport']['data']
                    
                    if page_appointments and len(page_appointments) > 0:
                        # This page is valid, try higher
                        highest_valid_page = mid
                        
                        # Process the data while we're here
                        page_transformed = process_appointments_data(page_appointments, start_date, end_date)
                        all_appointments.extend(page_transformed)
                        
                        logger.info(f"Successfully fetched page {mid}, got {len(page_appointments)} records during binary search")
                        
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
                
                if 'data' in page_data and 'followUpEntriesReport' in page_data['data']:
                    page_appointments = page_data['data']['followUpEntriesReport']['data']
                    
                    if not page_appointments:
                        logger.warning(f"No data returned for page {page}")
                        continue
                    
                    # Process the data
                    page_transformed = process_appointments_data(page_appointments, start_date, end_date)
                    all_appointments.extend(page_transformed)
                    
                    logger.info(f"Successfully fetched page {page}, got {len(page_appointments)} records")
                else:
                    logger.error(f"Unexpected API response structure on page {page}")
                
                await asyncio.sleep(0.2)  # Add delay to avoid rate limiting
        else:
            logger.error(f"Unexpected API response structure: {data}")
    
    except Exception as e:
        logger.error(f"Error processing follow-ups entries report data: {str(e)}")
    
    logger.info(f"Total follow-ups entries records fetched: {len(all_appointments)}")
    return all_appointments

def process_appointments_data(appointments_data, start_date, end_date):
    """Helper function to process and transform appointments data consistently"""
    transformed_appointments = []
    for appointment in appointments_data:
        transformed_appointment = {
            'name': appointment.get('name', ''),
            'customer_ids': appointment.get('customerIds', []),
            'follow_ups_count': appointment.get('followUpsCount', 0),
            # Add report metadata for database storage
            'report_start_date': start_date,
            'report_end_date': end_date,
            'created_at': datetime.now().isoformat()
        }
        transformed_appointments.append(transformed_appointment)
    
    return transformed_appointments

async def fetch_and_process_followUpEntriesReport(start_date: str, end_date: str) -> List[Dict]:
    import aiohttp
    
    async with aiohttp.ClientSession() as session:
        appointments = await fetch_followUpEntriesReport(session, start_date, end_date)
    
    return appointments