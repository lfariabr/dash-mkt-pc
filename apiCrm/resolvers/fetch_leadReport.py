import asyncio
import logging
from typing import List, Dict
from .fetch_graphql import fetch_graphql
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

logger = logging.getLogger(__name__)

async def fetch_leadReport(session, start_date: str, end_date: str) -> List[Dict]:
    """
    Fetches lead report data from the CRM API within a specified date range.
    
    Args:
        session: The aiohttp ClientSession object
        start_date: Start date in ISO format (YYYY-MM-DD)
        end_date: End date in ISO format (YYYY-MM-DD)
        
    Returns:
        List of lead report dictionaries
    """
    current_page = 1
    all_leads = []
    api_url = os.getenv('API_CRM_URL', 'https://open-api.eprocorpo.com.br/graphql')
    
    # Updated query with non-nullable types (Date!) for date parameters
    query = '''
    query LeadsReport($currentPage: Int!, $perPage: Int!, $startDate: Date!, $endDate: Date!) {
        leadsReport(
            filters: { createdAtRange: { start: $startDate, end: $endDate } }
            pagination: { currentPage: $currentPage, perPage: $perPage }
        ) {
            data {
                id
                name
                email
                telephone
                message
                store {
                    name
                }
                source {
                    title
                }
                createdAt
                status {
                    label
                }
                utmSource
                utmMedium
                utmTerm
                utmContent
                utmCampaign
                searchTerm
            }
            meta {
                currentPage
                lastPage
                total
            }
        }
    }
    '''

    logger.info(f"Attempting to fetch leads from {start_date} to {end_date}")
    
    # Try with a single request first to verify schema
    variables = {
        'currentPage': current_page,
        'perPage': 400,  # Smaller batch size for initial testing
        'startDate': start_date,
        'endDate': end_date
    }
    
    data = await fetch_graphql(session, api_url, query, variables)
    
    if data is None or 'errors' in data:
        error_msg = data.get('errors', [{'message': 'Unknown error'}])[0]['message'] if data else 'No data returned'
        logger.error(f"Failed initial leads fetch: {error_msg}")
        return all_leads  # Return empty list on initial failure
    
    # If we got here, the query structure works
    try:
        # Process first page
        if 'data' in data and 'leadsReport' in data['data']:
            leads_data = data['data']['leadsReport']['data']
            meta = data['data']['leadsReport']['meta']
            
            # Transform first page of data
            transformed_leads = []
            for lead in leads_data:
                transformed_lead = {
                    'id': lead.get('id'),
                    'name': lead.get('name'),
                    'email': lead.get('email'),
                    'telephone': lead.get('telephone'),
                    'message': lead.get('message'),
                    'createdAt': lead.get('createdAt'),
                    'store': lead.get('store', {}).get('name') if lead.get('store') else None,
                    'source': lead.get('source', {}).get('title') if lead.get('source') else None,
                    'status': lead.get('status', {}).get('label') if lead.get('status') else None,
                    'utmSource': lead.get('utmSource'),
                    'utmMedium': lead.get('utmMedium'),
                    'utmTerm': lead.get('utmTerm'),
                    'utmContent': lead.get('utmContent'),
                    'utmCampaign': lead.get('utmCampaign'),
                    'searchTerm': lead.get('searchTerm')
                }
                transformed_leads.append(transformed_lead)
            
            all_leads.extend(transformed_leads)
            
            last_page = meta.get('lastPage', 1)
            
            logger.info(f"Successfully fetched page 1/{last_page}, got {len(leads_data)} leads")
            
            # If we have more pages, fetch them
            if last_page > 1:
                for page in range(2, last_page + 1):
                    variables['currentPage'] = page
                    page_data = await fetch_graphql(session, api_url, query, variables)
                    
                    if not page_data or 'errors' in page_data:
                        logger.error(f"Error fetching page {page}/{last_page}")
                        continue
                    
                    if 'data' in page_data and 'leadsReport' in page_data['data']:
                        page_leads = page_data['data']['leadsReport']['data']
                        
                        # Transform data for this page
                        page_transformed = []
                        for lead in page_leads:
                            transformed_lead = {
                                'id': lead.get('id'),
                                'name': lead.get('name'),
                                'email': lead.get('email'),
                                'telephone': lead.get('telephone'),
                                'message': lead.get('message'),
                                'createdAt': lead.get('createdAt'),
                                'store': lead.get('store', {}).get('name') if lead.get('store') else None,
                                'source': lead.get('source', {}).get('title') if lead.get('source') else None,
                                'status': lead.get('status', {}).get('label') if lead.get('status') else None,
                                'utmSource': lead.get('utmSource'),
                                'utmMedium': lead.get('utmMedium'),
                                'utmTerm': lead.get('utmTerm'),
                                'utmContent': lead.get('utmContent'),
                                'utmCampaign': lead.get('utmCampaign'),
                                'searchTerm': lead.get('searchTerm')
                            }
                            page_transformed.append(transformed_lead)
                        
                        all_leads.extend(page_transformed)
                        logger.info(f"Successfully fetched page {page}/{last_page}, got {len(page_leads)} leads")
                    
                    # Add a delay between requests to avoid rate limiting
                    await asyncio.sleep(2)
        else:
            logger.error(f"Unexpected API response structure: {data}")
    
    except Exception as e:
        logger.error(f"Error processing lead data: {str(e)}")
    
    logger.info(f"Total leads fetched: {len(all_leads)}")
    return all_leads

async def fetch_and_process_lead_report(start_date: str, end_date: str) -> List[Dict]:
    """
    Creates a session and fetches lead report data.
    
    Args:
        start_date: Start date in ISO format (YYYY-MM-DD)
        end_date: End date in ISO format (YYYY-MM-DD)
        
    Returns:
        List of processed lead report dictionaries ready for database insertion
    """
    import aiohttp
    
    async with aiohttp.ClientSession() as session:
        leads = await fetch_leadReport(session, start_date, end_date)
    
    return leads