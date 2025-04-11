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
    all_appointments = []
    api_url = os.getenv('API_CRM_URL', 'https://open-api.eprocorpo.com.br/graphql')

    # Query with proper pagination parameters
    query = '''
    query appointmentsByUserReport($start: Date!, $end: Date!, $currentPage: Int!, $perPage: Int!) {
        appointmentsByUserReport(
            filters: { createdAtRange: { start: $start, end: $end } }
            pagination: { currentPage: $currentPage, perPage: $perPage }
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
    
    # Set initial pagination values and fetch the first page
    per_page = 100
    
    # First, get metadata to understand pagination and fetch first page
    variables = {
        "start": start_date,
        "end": end_date,
        "currentPage": 1,
        "perPage": per_page 
    }
    
    logger.info(f"Attempting to fetch appointments by user report from {start_date} to {end_date}")
    
    # Fetch the first page
    data = await fetch_graphql(session, api_url, query, variables)
    
    if data is None or 'errors' in data:
        error_msg = data.get('errors', [{'message': 'Unknown error'}])[0]['message'] if data else 'No data returned'
        logger.error(f"Failed initial appointments by user report fetch: {error_msg}")
        return all_appointments  # Return empty list on initial failure
    
    try:
        # Process first page
        if 'data' in data and 'appointmentsByUserReport' in data['data']:
            appointments_data = data['data']['appointmentsByUserReport']['data']
            meta = data['data']['appointmentsByUserReport']['meta']
            
            # Process the first page data
            page_transformed = []
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
                        # Convert label to integer
                        count = int(label) if label.isdigit() else 0
                        transformed_appointment['procedure_counts'][code] = count
                
                # Add timestamps for database records
                transformed_appointment['created_at'] = datetime.now().isoformat()
                transformed_appointment['report_start_date'] = start_date
                transformed_appointment['report_end_date'] = end_date
                
                # Add to results
                page_transformed.append(transformed_appointment)
            
            # Add first page results to overall results
            all_appointments.extend(page_transformed)
            logger.info(f"Added {len(page_transformed)} appointments from page 1")
            
            total_records = meta.get('total', 0)
            reported_last_page = meta.get('lastPage', 1)
            
            logger.info(f"Total records: {total_records}, reported last page: {reported_last_page}")
            
            # Use binary search to find the highest valid page
            if reported_last_page > 1:
                low = 2  # We already fetched page 1
                high = min(500, reported_last_page)  # Set a reasonable upper bound to start with
                highest_valid_page = 1  # We know page 1 is valid
                
                logger.info(f"Performing binary search for highest valid page between 2 and {high}")
                
                while low <= high:
                    mid = (low + high) // 2
                    variables['currentPage'] = mid
                    
                    page_data = await fetch_graphql(session, api_url, query, variables)
                    
                    if page_data is None or 'errors' in page_data:
                        # Error occurred, assume this page is too high
                        high = mid - 1
                        continue
                    
                    if 'data' in page_data and 'appointmentsByUserReport' in page_data['data']:
                        page_appointments = page_data['data']['appointmentsByUserReport']['data']
                        
                        if page_appointments and len(page_appointments) > 0:
                            # Valid page found
                            highest_valid_page = mid
                            
                            # Process the data while we're here
                            page_transformed = []
                            for appointment in page_appointments:
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
                                        # Convert label to integer
                                        count = int(label) if label.isdigit() else 0
                                        transformed_appointment['procedure_counts'][code] = count
                                
                                # Add timestamps for database records
                                transformed_appointment['created_at'] = datetime.now().isoformat()
                                transformed_appointment['report_start_date'] = start_date
                                transformed_appointment['report_end_date'] = end_date
                                
                                # Add to page results
                                page_transformed.append(transformed_appointment)
                            
                            # Add page results to overall results
                            all_appointments.extend(page_transformed)
                            logger.info(f"Added {len(page_transformed)} appointments from page {mid}")
                            
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
                # Skip pages we already fetched (1 and highest_valid_page during binary search)
                pages_to_fetch = [p for p in range(2, highest_valid_page) if p != highest_valid_page]
                pages_to_fetch.sort(reverse=True)  # Fetch in descending order
                
                logger.info(f"Fetching remaining {len(pages_to_fetch)} pages in descending order")
                
                for page in pages_to_fetch:
                    variables['currentPage'] = page
                    
                    page_data = await fetch_graphql(session, api_url, query, variables)
                    
                    if page_data is None or 'errors' in page_data:
                        logger.error(f"Failed to fetch page {page}")
                        continue
                    
                    if 'data' in page_data and 'appointmentsByUserReport' in page_data['data']:
                        page_appointments = page_data['data']['appointmentsByUserReport']['data']
                        
                        if not page_appointments:
                            logger.warning(f"No data returned for page {page}")
                            continue
                        
                        # Process the data
                        page_transformed = []
                        for appointment in page_appointments:
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
                                    # Convert label to integer
                                    count = int(label) if label.isdigit() else 0
                                    transformed_appointment['procedure_counts'][code] = count
                            
                            # Add timestamps for database records
                            transformed_appointment['created_at'] = datetime.now().isoformat()
                            transformed_appointment['report_start_date'] = start_date
                            transformed_appointment['report_end_date'] = end_date
                            
                            # Add to page results
                            page_transformed.append(transformed_appointment)
                        
                        # Add page results to overall results
                        all_appointments.extend(page_transformed)
                        logger.info(f"Added {len(page_transformed)} appointments from page {page}")
                    else:
                        logger.error(f"Unexpected API response structure on page {page}")
                    
                    await asyncio.sleep(0.2)  # Add delay to avoid rate limiting
        else:
            logger.error(f"Unexpected API response structure: {data}")
    
    except Exception as e:
        logger.error(f"Error processing appointments by user report data: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
    
    logger.info(f"Total appointments by user fetched: {len(all_appointments)}")
    return all_appointments

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