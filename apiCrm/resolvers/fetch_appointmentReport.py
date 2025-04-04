"""
cd /Users/luisfaria/Desktop/sEngineer/dash
python -m apiCrm.tests.fetch_appointmentReport_test
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

async def fetch_appointmentReport(session, start_date: str, end_date: str) -> List[Dict]:
    """
    Fetches appointment report data from the CRM API within a specified date range.
    
    Args:
        session: The aiohttp ClientSession object
        start_date: Start date in ISO format (YYYY-MM-DD)
        end_date: End date in ISO format (YYYY-MM-DD)
        
    Returns:
        List of appointment report dictionaries
    """
    current_page = 1
    all_appointments = []
    api_url = os.getenv('API_CRM_URL', 'https://open-api.queromeubotox.com.br/graphql')

    # Updated query with non-nullable types (Date!) for date parameters
    query = '''
    query AppointmentsReport($start: Date!, $end: Date!) {
        appointmentsReport(
            filters: { startDateRange: { start: $start, end: $end } }
            pagination: { currentPage: 1, perPage: 100 }
        ) {
            data {
            afterPhotoUrl
            batchPhotoUrl
            beforePhotoUrl
            endDate
            id
            startDate
            updatedAt

            status {
                code
                label
            }

            oldestParent {
                createdAt

                createdBy {
                    name

                    group {
                        name
                    }
                }
            }

            customer {
                addressLine
                email
                id
                name
                taxvatFormatted

                telephones {
                    number
                }

                source {
                    title
                }
            }

            store {
                name
            }

            procedure {
                groupLabel
                name
            }

            employee {
                name
            }

            comments {
                comment
            }

            updatedBy {
                name
            }

            latestProgressComment {
                comment
                createdAt

                user {
                    name
                }
            }
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
        'start': start_date,  # Changed from 'startDate' to 'start'
        'end': end_date       # Changed from 'endDate' to 'end'
    }
    
    logger.info(f"Attempting to fetch appointments from {start_date} to {end_date}")
    
    data = await fetch_graphql(session, api_url, query, variables)
    
    if data is None or 'errors' in data:
        error_msg = data.get('errors', [{'message': 'Unknown error'}])[0]['message'] if data else 'No data returned'
        logger.error(f"Failed initial appointments fetch: {error_msg}")
        return all_appointments  # Return empty list on initial failure
    
    # If we got here, the query structure works
    try:
        # Process first page
        if 'data' in data and 'appointmentsReport' in data['data']:
            appointments_data = data['data']['appointmentsReport']['data']
            meta = data['data']['appointmentsReport']['meta']

            transformed_appointments = []
            for appointment in appointments_data:
                # Nested objects
                customer = appointment.get('customer', {}) or {}
                store = appointment.get('store', {}) or {}
                procedure = appointment.get('procedure', {}) or {}
                employee = appointment.get('employee', {}) or {}
                status = appointment.get('status', {}) or {}
                updatedBy = appointment.get('updatedBy', {}) or {}
                latestProgressComment = appointment.get('latestProgressComment', {}) or {}
                oldestParent = appointment.get('oldestParent', {}) or {}
                oldestParent_createdBy = oldestParent.get('createdBy', {}) or {}
                oldestParent_createdBy_group = oldestParent_createdBy.get('group', {}) or {}

                # Telephones
                telephones_data = customer.get('telephones', [])
                telephones = ', '.join([tel.get('number', '') for tel in telephones_data]) if telephones_data else None

                # Comments
                comments_data = appointment.get('comments', [])
                comments = ', '.join([c.get('comment', '') for c in comments_data]) if comments_data else None

                transformed_appointment = {
                    'id': appointment.get('id'),
                    'startDate': appointment.get('startDate'),
                    'endDate': appointment.get('endDate'),
                    'updatedAt': appointment.get('updatedAt'),
                    'beforePhotoUrl': appointment.get('beforePhotoUrl'),
                    'batchPhotoUrl': appointment.get('batchPhotoUrl'),
                    'afterPhotoUrl': appointment.get('afterPhotoUrl'),
                    'status_code': status.get('code'),
                    'status_label': status.get('label'),

                    # Customer
                    'client_id': customer.get('id'),
                    'name': customer.get('name'),
                    'email': customer.get('email'),
                    'telephones': telephones,
                    'addressLine': customer.get('addressLine'),
                    'taxvatFormatted': customer.get('taxvatFormatted'),
                    'source': customer.get('source', {}).get('title'),

                    # Store and procedure
                    'store': store.get('name'),
                    'procedure': procedure.get('name'),
                    'procedure_groupLabel': procedure.get('groupLabel'),

                    # Employee
                    'employee': employee.get('name'),
                    'updatedBy_name': updatedBy.get('name'),
                    'comments': comments,

                    # Progress comment
                    'latest_comment': latestProgressComment.get('comment'),
                    'latest_comment_createdAt': latestProgressComment.get('createdAt'),
                    'latest_comment_user': latestProgressComment.get('user', {}).get('name'),

                    # Oldest parent info
                    'oldestParent_createdAt': oldestParent.get('createdAt'),
                    'oldestParent_createdBy_name': oldestParent_createdBy.get('name'),
                    'oldestParent_createdBy_group': oldestParent_createdBy_group.get('name'),
                }

                transformed_appointments.append(transformed_appointment)  
            
            # Add first page to results
            all_appointments.extend(transformed_appointments)
            
            last_page = meta.get('lastPage', 1)
            total_items = meta.get('total', len(appointments_data))
            
            logger.info(f"Successfully fetched page 1/{last_page}, got {len(appointments_data)} appointments")
            
            # If we have more pages, fetch them
            if last_page > 1:
                for page in range(2, last_page + 1):
                    variables['currentPage'] = page
                    page_data = await fetch_graphql(session, api_url, query, variables)
                    
                    if not page_data or 'errors' in page_data:
                        logger.error(f"Error fetching page {page}/{last_page}")
                        continue
                    
                    if 'data' in page_data and 'appointmentsReport' in page_data['data']:
                        page_appointments = page_data['data']['appointmentsReport']['data']
                        
                        # Transform data for this page
                        page_transformed = []
                        for appointment in page_appointments:
                            # Check if required fields exist to avoid None errors
                            customer = appointment.get('customer', {}) or {}
                            store = appointment.get('store', {}) or {}
                            procedure = appointment.get('procedure', {}) or {}
                            employee = appointment.get('employee', {}) or {}
                            status = appointment.get('status', {}) or {}
                            updatedBy = appointment.get('updatedBy', {}) or {}
                            updatedBy_group = updatedBy.get('group', {}) or {}
                            updatedBy_group_createdBy = updatedBy_group.get('createdBy', {}) or {}
                            
                            # Format telephones
                            telephones_data = customer.get('telephones', [])
                            telephones = ', '.join([tel.get('number', '') for tel in telephones_data]) if telephones_data else None
                            
                            transformed_appointment = {
                                'id': appointment.get('id'),
                                'client_id': customer.get('id'),
                                'name': customer.get('name'),
                                'telephones': telephones,
                                'email': customer.get('email'),
                                'store': store.get('name'),
                                'procedure': procedure.get('name'),
                                'employee': employee.get('name'),
                                'startDate': appointment.get('startDate'),
                                'status': status.get('label'),
                                'updatedBy_name': updatedBy.get('name'),
                                'updatedBy_createdAt': updatedBy.get('createdAt'),
                                'updatedBy_group_name': updatedBy_group.get('name'),
                                'updatedBy_group_createdBy_name': updatedBy_group_createdBy.get('name')
                            }
                            page_transformed.append(transformed_appointment)
                        
                        all_appointments.extend(page_transformed)
                        logger.info(f"Successfully fetched page {page}/{last_page}, got {len(page_appointments)} appointments")
                    
                    # Add a delay between requests to avoid rate limiting
                    await asyncio.sleep(2)
        else:
            logger.error(f"Unexpected API response structure: {data}")
    
    except Exception as e:
        logger.error(f"Error processing appointment data: {str(e)}")
    
    logger.info(f"Total appointments fetched: {len(all_appointments)}")
    return all_appointments

async def fetch_and_process_appointment_report(start_date: str, end_date: str) -> List[Dict]:
    """
    Creates a session and fetches appointment report data.
    
    Args:
        start_date: Start date in ISO format (YYYY-MM-DD)
        end_date: End date in ISO format (YYYY-MM-DD)
        
    Returns:
        List of processed lead report dictionaries ready for database insertion
    """
    import aiohttp
    
    async with aiohttp.ClientSession() as session:
        appointments = await fetch_appointmentReport(session, start_date, end_date)
    
    return appointments