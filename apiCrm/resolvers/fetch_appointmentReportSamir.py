import asyncio
import logging
from typing import List, Dict
from .fetch_graphql import fetch_graphql
from dotenv import load_dotenv
import os
from datetime import datetime
import json  # Add import for JSON handling

load_dotenv()

logger = logging.getLogger(__name__)

async def fetch_appointmentReportSamir(session, start_date: str, end_date: str) -> List[Dict]:
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
    api_url = os.getenv('API_CRM_URL', 'https://open-api.eprocorpo.com.br/graphql')

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
                logger.error("No 'appointmentsReport' field in GraphQL data")
                return all_appointments
                
            appointments_report = data['data']['appointmentsReport']
            if not appointments_report:
                logger.error("Empty 'appointmentsReport' in response")
                return all_appointments
                
            # Check first for the appointments data
            if 'data' not in appointments_report:
                logger.info("No 'data' field in appointmentsReport")
                return all_appointments
                
            appointments_data = appointments_report.get('data', [])
            if not appointments_data:
                logger.info("No appointments found in the specified date range")
                return all_appointments
            
            # Print the JSON structure of the first appointment if one exists
            if appointments_data and len(appointments_data) > 0:
                first_appointment = appointments_data[0]
                
                # # Print full structure with pretty formatting
                # logger.info("First appointment data structure:")
                # logger.info(json.dumps(first_appointment, indent=2, ensure_ascii=False))
                
                # # Also print specific nested structures we're interested in
                # logger.info("oldestParent structure:")
                # logger.info(json.dumps(first_appointment.get('oldestParent', {}), indent=2, ensure_ascii=False))
                
                # logger.info("updatedBy structure:")
                # logger.info(json.dumps(first_appointment.get('updatedBy', {}), indent=2, ensure_ascii=False))
            
            meta = appointments_report.get('meta', {})
            if meta is None:
                meta = {}  # Provide a default empty dict if meta is None
            
            # Get pagination info with fallbacks
            current_page = meta.get('currentPage', 1)
            per_page = meta.get('perPage', 0)
            last_page = meta.get('lastPage', 1)
            
            # Copy raw appointments directly
            all_appointments.extend(appointments_data)
            
            # If there are more pages, fetch them all
            if current_page < last_page:
                logger.info(f"Fetching remaining {last_page - current_page} pages")
                
                for page in range(current_page + 1, last_page + 1):
                    logger.info(f"Fetching page {page} of {last_page}")
                    
                    # Update pagination variables
                    variables['currentPage'] = page
                    
                    try:
                        # Make the GraphQL request for this page
                        page_data = await fetch_graphql(session, api_url, query, variables)
                        
                        if page_data and 'data' in page_data and 'appointmentsReport' in page_data['data']:
                            page_appointments_report = page_data['data']['appointmentsReport']
                            if page_appointments_report and 'data' in page_appointments_report:
                                page_appointments = page_appointments_report['data']
                                
                                # Add raw page appointments to our list
                                all_appointments.extend(page_appointments)
                                logger.info(f"Added {len(page_appointments)} appointments from page {page}")
                            else:
                                logger.warning(f"No appointment data found on page {page}")
                        else:
                            logger.warning(f"Invalid response structure for page {page}")
                    except Exception as e:
                        logger.error(f"Error fetching page {page}: {str(e)}")
            
            logger.info(f"Total appointments fetched: {len(all_appointments)}")
            
            # Process the oldestParent data as per Samir's requirements
            processed_appointments = []
            for appointment in all_appointments:
                if appointment is None:
                    continue
                
                try:
                    # Extract necessary objects with fallbacks
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
                    
                    # Implementation of fallback logic for Samir's requirements:
                    # R -> oldestParent.createdAt (quando disponível) ou updatedAt (caso contrário)
                    # S -> oldestParent.createdBy.name (quando disponível) ou updatedBy.name (caso contrário)
                    # T -> oldestParent.createdBy.group.name (quando disponível) - no good fallback
                    created_at = oldestParent.get('createdAt') or appointment.get('updatedAt', '')

                    created_by = (oldestParent.get('createdBy') or {}).get('name') or updatedBy.get('name', '')
                    created_by_group = "Não disponível"

                    created_by = oldestParent.get('createdBy') if oldestParent else None
                    if created_by:
                        group = created_by.get('group')
                        if group:
                            created_by_group = group.get('name', "Não disponível")
                    
                    # Format createdAt as dd/MM/yyyy HH:mm if it's not empty
                    formatted_created_at = ''
                    if created_at:
                        try:
                            # Parse ISO format date and reformat
                            dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                            formatted_created_at = dt.strftime('%d/%m/%Y %H:%M')
                        except Exception as e:
                            logger.error(f"Error formatting createdAt date: {str(e)}")
                            formatted_created_at = created_at  # Fallback to original format

                    
                    
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
                        'createdAt': formatted_created_at,
                        'createdBy': created_by,
                        'createdBy_group': created_by_group,
                        'source': customer_source.get('title', ''),
                        'updatedAt': appointment.get('updatedAt', ''),
                        'updatedBy': updatedBy.get('name', ''),
                        'latestProgressComment': latestProgressComment.get('comment', ''),
                        'latestProgressDate': latestProgressComment.get('createdAt', ''),
                        'latestProgressUser': latestProgressComment_user.get('name', '')
                    }
                    processed_appointments.append(transformed_appointment)
                except Exception as e:
                    logger.error(f"Error processing appointment: {str(e)}")
                    continue
            logger.info("DEBUG - oldestParent format:")
            logger.info(json.dumps(oldestParent, indent=2, ensure_ascii=False))
            return processed_appointments
            
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

async def fetch_and_process_appointment_reportSamir(start_date: str, end_date: str) -> List[Dict]:
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
        appointments = await fetch_appointmentReportSamir(session, start_date, end_date)
    
    return appointments