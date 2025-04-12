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

    # Updated query with correct pagination parameters
    query = '''
    query AppointmentsReport($start: Date!, $end: Date!, $currentPage: Int!, $perPage: Int!) {
        appointmentsReport(
            filters: { startDateRange: { start: $start, end: $end } }
            pagination: { currentPage: $currentPage, perPage: $perPage }
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
        'start': start_date,
        'end': end_date,
        'currentPage': current_page,
        'perPage': 400
    }
    
    logger.info(f"Attempting to fetch appointments from {start_date} to {end_date}")
    
    try:
        # Make the initial GraphQL request
        data = await fetch_graphql(session, api_url, query, variables)
        
        # Carefully check for None and errors
        if data is None:
            logger.error("GraphQL request returned None response")
            return all_appointments
            
        if 'errors' in data:
            error_msg = "Unknown error"
            if data['errors'] and isinstance(data['errors'], list) and len(data['errors']) > 0:
                if 'message' in data['errors'][0]:
                    error_msg = data['errors'][0]['message']
            logger.error(f"Failed initial appointments fetch: {error_msg}")
            return all_appointments
            
        # If we got here, the query structure works
        try:
            # Check if we have appointment data
            if 'data' not in data:
                logger.error("No 'data' field in GraphQL response")
                return all_appointments
                
            if 'appointmentsReport' not in data['data']:
                logger.error("No 'appointmentsReport' field in GraphQL response data")
                return all_appointments
                
            appointments_report = data['data']['appointmentsReport']
            if appointments_report is None:
                logger.error("appointmentsReport is None")
                return all_appointments
                
            appointments_data = appointments_report.get('data', [])
            if not appointments_data:
                logger.info("No appointments found in the specified date range")
                return all_appointments
                
            meta = appointments_report.get('meta', {})
            if meta is None:
                meta = {}  # Provide a default empty dict if meta is None

            transformed_appointments = []
            for appointment in appointments_data:
                if appointment is None:
                    continue
                    
                try:
                    # Safely extract nested objects with fallbacks to empty dictionaries
                    customer = appointment.get('customer', {}) or {}
                    store = appointment.get('store', {}) or {}
                    procedure = appointment.get('procedure', {}) or {}
                    employee = appointment.get('employee', {}) or {}
                    status = appointment.get('status', {}) or {}
                    updatedBy = appointment.get('updatedBy', {}) or {}
                    oldestParent = appointment.get('oldestParent', {}) or {}
                    latestProgressComment = appointment.get('latestProgressComment', {}) or {}
                    
                    # Safely extract nested objects one level deeper
                    customer_source = customer.get('source', {}) or {}
                    latestProgressComment_user = latestProgressComment.get('user', {}) or {}
                    
                    # Extract telephones with safety checks
                    telephones_data = customer.get('telephones', []) or []
                    telephones = ', '.join([tel.get('number', '') for tel in telephones_data if tel and isinstance(tel, dict)]) if telephones_data else ''
                    
                    # Extract comments with safety checks
                    comments_data = appointment.get('comments', []) or []
                    comments = '; '.join([c.get('comment', '') for c in comments_data if c and isinstance(c, dict)]) if comments_data else ''
                    
                    # Map fields to the expected column names for the UI, with default values
                    transformed_appointment = {
                        'id': appointment.get('id', ''),
                        'client_id': customer.get('id', ''),
                        'name': customer.get('name', ''),
                        'telephones': telephones,
                        'email': customer.get('email', ''),
                        'store': store.get('name', ''),
                        'procedure': procedure.get('name', ''),
                        'procedure_groupLabel': procedure.get('groupLabel', ''),
                        'employee': employee.get('name', ''),
                        'startDate': appointment.get('startDate', ''),
                        'endDate': appointment.get('endDate', ''),
                        'status': status.get('label', ''),
                        'comments': comments,
                        'beforePhotoUrl': appointment.get('beforePhotoUrl', ''),
                        'batchPhotoUrl': appointment.get('batchPhotoUrl', ''),
                        'afterPhotoUrl': appointment.get('afterPhotoUrl', ''),
                        'createdAt': oldestParent.get('createdAt', ''),
                        'createdBy': oldestParent.get('createdBy', {}).get('name', ''),
                        'createdBy_group': oldestParent.get('createdBy', {}).get('group', {}).get('name', ''),
                        'source': customer_source.get('title', ''),
                        'updatedAt': appointment.get('updatedAt', ''),
                        'updatedBy': updatedBy.get('name', ''),
                        'latestProgressComment': latestProgressComment.get('comment', ''),
                        'latestProgressDate': latestProgressComment.get('createdAt', ''),
                        'latestProgressUser': latestProgressComment_user.get('name', '')
                    }
                    transformed_appointments.append(transformed_appointment)
                except Exception as e:
                    logger.error(f"Error processing individual appointment: {str(e)}")
                    # Continue with the next appointment rather than failing entirely
                    continue
                
            all_appointments.extend(transformed_appointments)
            
            # Get pagination info with fallbacks
            current_page = meta.get('currentPage', 1)
            per_page = meta.get('perPage', 0)
            last_page = meta.get('lastPage', 1)
            total = meta.get('total', 0)
            
            logger.info(f"Fetched page {current_page} of {last_page}. Total appointments: {total}")
            
            # If there are more pages, fetch them
            if last_page > 1:
                # Create tasks for all remaining pages
                remaining_pages = list(range(2, last_page + 1))
                
                for page in remaining_pages:
                    try:
                        variables['currentPage'] = page
                        page_data = await fetch_graphql(session, api_url, query, variables)
                        
                        if page_data is None:
                            logger.error(f"GraphQL request returned None response for page {page}")
                            continue
                        
                        if 'errors' in page_data:
                            error_msg = "Unknown error"
                            if page_data['errors'] and isinstance(page_data['errors'], list) and len(page_data['errors']) > 0:
                                if 'message' in page_data['errors'][0]:
                                    error_msg = page_data['errors'][0]['message']
                            logger.error(f"Failed appointments fetch for page {page}: {error_msg}")
                            continue
                        
                        if 'data' in page_data and 'appointmentsReport' in page_data['data']:
                            page_appointments_report = page_data['data']['appointmentsReport']
                            if page_appointments_report and 'data' in page_appointments_report:
                                page_appointments = page_appointments_report['data']
                                
                                # Transform data for this page
                                page_transformed = []
                                for appointment in page_appointments:
                                    if appointment is None:
                                        continue
                                    
                                    try:
                                        # Safely extract nested objects with fallbacks to empty dictionaries
                                        customer = appointment.get('customer', {}) or {}
                                        store = appointment.get('store', {}) or {}
                                        procedure = appointment.get('procedure', {}) or {}
                                        employee = appointment.get('employee', {}) or {}
                                        status = appointment.get('status', {}) or {}
                                        updatedBy = appointment.get('updatedBy', {}) or {}
                                        oldestParent = appointment.get('oldestParent', {}) or {}
                                        createdBy = oldestParent.get('createdBy', {}) or {}
                                        latestProgressComment = appointment.get('latestProgressComment', {}) or {}
                                        
                                        # Safely extract nested objects one level deeper
                                        customer_source = customer.get('source', {}) or {}
                                        latestProgressComment_user = latestProgressComment.get('user', {}) or {}
                                        
                                        # Extract telephones with safety checks
                                        telephones_data = customer.get('telephones', []) or []
                                        telephones = ', '.join([tel.get('number', '') for tel in telephones_data if tel and isinstance(tel, dict)]) if telephones_data else ''
                                        
                                        # Extract comments with safety checks
                                        comments_data = appointment.get('comments', []) or []
                                        comments = '; '.join([c.get('comment', '') for c in comments_data if c and isinstance(c, dict)]) if comments_data else ''
                                        
                                        transformed_appointment = {
                                            'id': appointment.get('id', ''),
                                            'client_id': customer.get('id', ''),
                                            'name': customer.get('name', ''),
                                            'telephones': telephones,
                                            'email': customer.get('email', ''),
                                            'store': store.get('name', ''),
                                            'procedure': procedure.get('name', ''),
                                            'employee': employee.get('name', ''),
                                            'startDate': appointment.get('startDate', ''),
                                            'endDate': appointment.get('endDate', ''),
                                            'status': status.get('label', ''),
                                            'comments': comments,
                                            'beforePhotoUrl': appointment.get('beforePhotoUrl', ''),
                                            'batchPhotoUrl': appointment.get('batchPhotoUrl', ''),
                                            'afterPhotoUrl': appointment.get('afterPhotoUrl', ''),
                                            'createdBy': oldestParent.get('createdBy', {}).get('name', ''),
                                            'createdBy_group': oldestParent.get('createdBy', {}).get('group', {}).get('name', ''),
                                            'createdAt': oldestParent.get('createdAt', ''),
                                            'source': customer_source.get('title', ''),
                                            'updatedAt': appointment.get('updatedAt', ''),
                                            'updatedBy': updatedBy.get('name', ''),
                                            'latestProgressComment': latestProgressComment.get('comment', ''),
                                            'latestProgressDate': latestProgressComment.get('createdAt', ''),
                                            'latestProgressUser': latestProgressComment_user.get('name', '')
                                        }
                                        page_transformed.append(transformed_appointment)
                                    except Exception as e:
                                        logger.error(f"Error processing appointment on page {page}: {str(e)}")
                                        continue
                                        
                                all_appointments.extend(page_transformed)
                                logger.info(f"Added {len(page_transformed)} appointments from page {page}")
                            else:
                                logger.warning(f"No appointment data found on page {page}")
                        else:
                            logger.warning(f"Invalid response structure for page {page}")
                    except Exception as e:
                        logger.error(f"Error fetching page {page}: {str(e)}")
            
            logger.info(f"Total appointments fetched: {len(all_appointments)}")
            return all_appointments
            
        except Exception as e:
            logger.error(f"Error processing appointment data: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return all_appointments
            
    except Exception as e:
        logger.error(f"Error making GraphQL request: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
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


async def fetch_appointmentReportCreatedAt(session, start_date: str, end_date: str) -> List[Dict]:
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

    # Updated query with correct pagination parameters
    query = '''
    query AppointmentsReport($start: Date!, $end: Date!, $currentPage: Int!, $perPage: Int!) {
        appointmentsReport(
            filters: { createdAtRange: { start: $start, end: $end } }
            pagination: { currentPage: $currentPage, perPage: $perPage }
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
        'start': start_date,
        'end': end_date,
        'currentPage': current_page,
        'perPage': 400
    }
    
    logger.info(f"Attempting to fetch appointments from {start_date} to {end_date}")
    
    try:
        # Make the initial GraphQL request
        data = await fetch_graphql(session, api_url, query, variables)
        
        # Carefully check for None and errors
        if data is None:
            logger.error("GraphQL request returned None response")
            return all_appointments
            
        if 'errors' in data:
            error_msg = "Unknown error"
            if data['errors'] and isinstance(data['errors'], list) and len(data['errors']) > 0:
                if 'message' in data['errors'][0]:
                    error_msg = data['errors'][0]['message']
            logger.error(f"Failed initial appointments fetch: {error_msg}")
            return all_appointments
            
        # If we got here, the query structure works
        try:
            # Check if we have appointment data
            if 'data' not in data:
                logger.error("No 'data' field in GraphQL response")
                return all_appointments
                
            if 'appointmentsReport' not in data['data']:
                logger.error("No 'appointmentsReport' field in GraphQL response data")
                return all_appointments
                
            appointments_report = data['data']['appointmentsReport']
            if appointments_report is None:
                logger.error("appointmentsReport is None")
                return all_appointments
                
            appointments_data = appointments_report.get('data', [])
            if not appointments_data:
                logger.info("No appointments found in the specified date range")
                return all_appointments
                
            meta = appointments_report.get('meta', {})
            if meta is None:
                meta = {}  # Provide a default empty dict if meta is None

            transformed_appointments = []
            for appointment in appointments_data:
                if appointment is None:
                    continue
                    
                try:
                    # Safely extract nested objects with fallbacks to empty dictionaries
                    customer = appointment.get('customer', {}) or {}
                    store = appointment.get('store', {}) or {}
                    procedure = appointment.get('procedure', {}) or {}
                    employee = appointment.get('employee', {}) or {}
                    status = appointment.get('status', {}) or {}
                    createdBy = appointment.get('createdBy', {}) or {}
                    updatedBy = appointment.get('updatedBy', {}) or {}
                    oldestParent = appointment.get('oldestParent', {}) or {}
                    latestProgressComment = appointment.get('latestProgressComment', {}) or {}
                    
                    # Safely extract nested objects one level deeper
                    customer_source = customer.get('source', {}) or {}
                    latestProgressComment_user = latestProgressComment.get('user', {}) or {}
                    
                    # Extract telephones with safety checks
                    telephones_data = customer.get('telephones', []) or []
                    telephones = ', '.join([tel.get('number', '') for tel in telephones_data if tel and isinstance(tel, dict)]) if telephones_data else ''
                    
                    # Extract comments with safety checks
                    comments_data = appointment.get('comments', []) or []
                    comments = '; '.join([c.get('comment', '') for c in comments_data if c and isinstance(c, dict)]) if comments_data else ''
                    
                    # Map fields to the expected column names for the UI, with default values
                    transformed_appointment = {
                        'id': appointment.get('id', ''),
                        'client_id': customer.get('id', ''),
                        'name': customer.get('name', ''),
                        'telephones': telephones,
                        'email': customer.get('email', ''),
                        'store': store.get('name', ''),
                        'procedure': procedure.get('name', ''),
                        'procedure_groupLabel': procedure.get('groupLabel', ''),
                        'employee': employee.get('name', ''),
                        'startDate': appointment.get('startDate', ''),
                        'endDate': appointment.get('endDate', ''),
                        'status': status.get('label', ''),
                        'comments': comments,
                        'beforePhotoUrl': appointment.get('beforePhotoUrl', ''),
                        'batchPhotoUrl': appointment.get('batchPhotoUrl', ''),
                        'afterPhotoUrl': appointment.get('afterPhotoUrl', ''),
                        'createdBy': oldestParent.get('createdBy', {}).get('name', ''),
                        'createdBy_group': oldestParent.get('createdBy', {}).get('group', {}).get('name', ''),
                        'createdAt': oldestParent.get('createdAt', ''),
                        'source': customer_source.get('title', ''),
                        'updatedAt': appointment.get('updatedAt', ''),
                        'updatedBy': updatedBy.get('name', ''),
                        'latestProgressComment': latestProgressComment.get('comment', ''),
                        'latestProgressDate': latestProgressComment.get('createdAt', ''),
                        'latestProgressUser': latestProgressComment_user.get('name', '')
                    }
                    transformed_appointments.append(transformed_appointment)
                except Exception as e:
                    logger.error(f"Error processing individual appointment: {str(e)}")
                    # Continue with the next appointment rather than failing entirely
                    continue
                
            all_appointments.extend(transformed_appointments)
            
            # Get pagination info with fallbacks
            current_page = meta.get('currentPage', 1)
            per_page = meta.get('perPage', 0)
            last_page = meta.get('lastPage', 1)
            total = meta.get('total', 0)
            
            logger.info(f"Fetched page {current_page} of {last_page}. Total appointments: {total}")
            
            # If there are more pages, fetch them
            if last_page > 1:
                # Create tasks for all remaining pages
                remaining_pages = list(range(2, last_page + 1))
                
                for page in remaining_pages:
                    try:
                        variables['currentPage'] = page
                        page_data = await fetch_graphql(session, api_url, query, variables)
                        
                        if page_data is None:
                            logger.error(f"GraphQL request returned None response for page {page}")
                            continue
                        
                        if 'errors' in page_data:
                            error_msg = "Unknown error"
                            if page_data['errors'] and isinstance(page_data['errors'], list) and len(page_data['errors']) > 0:
                                if 'message' in page_data['errors'][0]:
                                    error_msg = page_data['errors'][0]['message']
                            logger.error(f"Failed appointments fetch for page {page}: {error_msg}")
                            continue
                        
                        if 'data' in page_data and 'appointmentsReport' in page_data['data']:
                            page_appointments_report = page_data['data']['appointmentsReport']
                            if page_appointments_report and 'data' in page_appointments_report:
                                page_appointments = page_appointments_report['data']
                                
                                # Transform data for this page
                                page_transformed = []
                                for appointment in page_appointments:
                                    if appointment is None:
                                        continue
                                    
                                    try:
                                        # Safely extract nested objects with fallbacks to empty dictionaries
                                        customer = appointment.get('customer', {}) or {}
                                        store = appointment.get('store', {}) or {}
                                        procedure = appointment.get('procedure', {}) or {}
                                        employee = appointment.get('employee', {}) or {}
                                        status = appointment.get('status', {}) or {}
                                        updatedBy = appointment.get('updatedBy', {}) or {}
                                        oldestParent = appointment.get('oldestParent', {}) or {}
                                        createdBy = oldestParent.get('createdBy', {}) or {}
                                        latestProgressComment = appointment.get('latestProgressComment', {}) or {}
                                        
                                        # Safely extract nested objects one level deeper
                                        customer_source = customer.get('source', {}) or {}
                                        latestProgressComment_user = latestProgressComment.get('user', {}) or {}
                                        
                                        # Extract telephones with safety checks
                                        telephones_data = customer.get('telephones', []) or []
                                        telephones = ', '.join([tel.get('number', '') for tel in telephones_data if tel and isinstance(tel, dict)]) if telephones_data else ''
                                        
                                        # Extract comments with safety checks
                                        comments_data = appointment.get('comments', []) or []
                                        comments = '; '.join([c.get('comment', '') for c in comments_data if c and isinstance(c, dict)]) if comments_data else ''
                                        
                                        transformed_appointment = {
                                            'id': appointment.get('id', ''),
                                            'client_id': customer.get('id', ''),
                                            'name': customer.get('name', ''),
                                            'telephones': telephones,
                                            'email': customer.get('email', ''),
                                            'store': store.get('name', ''),
                                            'procedure': procedure.get('name', ''),
                                            'procedure_groupLabel': procedure.get('groupLabel', ''),
                                            'employee': employee.get('name', ''),
                                            'startDate': appointment.get('startDate', ''),
                                            'endDate': appointment.get('endDate', ''),
                                            'status': status.get('label', ''),
                                            'comments': comments,
                                            'beforePhotoUrl': appointment.get('beforePhotoUrl', ''),
                                            'batchPhotoUrl': appointment.get('batchPhotoUrl', ''),
                                            'afterPhotoUrl': appointment.get('afterPhotoUrl', ''),
                                            'createdBy': oldestParent.get('createdBy', {}).get('name', ''),
                                            'createdBy_group': oldestParent.get('createdBy', {}).get('group', {}).get('name', ''),
                                            'createdAt': oldestParent.get('createdAt', ''),
                                            'source': customer_source.get('title', ''),
                                            'updatedAt': appointment.get('updatedAt', ''),
                                            'updatedBy': updatedBy.get('name', ''),
                                            'latestProgressComment': latestProgressComment.get('comment', ''),
                                            'latestProgressDate': latestProgressComment.get('createdAt', ''),
                                            'latestProgressUser': latestProgressComment_user.get('name', '')
                                        }
                                        page_transformed.append(transformed_appointment)
                                    except Exception as e:
                                        logger.error(f"Error processing appointment on page {page}: {str(e)}")
                                        continue
                                        
                                all_appointments.extend(page_transformed)
                                logger.info(f"Added {len(page_transformed)} appointments from page {page}")
                            else:
                                logger.warning(f"No appointment data found on page {page}")
                        else:
                            logger.warning(f"Invalid response structure for page {page}")
                    except Exception as e:
                        logger.error(f"Error fetching page {page}: {str(e)}")
            
            logger.info(f"Total appointments fetched: {len(all_appointments)}")
            return all_appointments
            
        except Exception as e:
            logger.error(f"Error processing appointment data: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return all_appointments
            
    except Exception as e:
        logger.error(f"Error making GraphQL request: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return all_appointments

async def fetch_and_process_appointment_report_created_at(start_date: str, end_date: str) -> List[Dict]:
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
        appointments = await fetch_appointmentReportCreatedAt(session, start_date, end_date)
    
    return appointments