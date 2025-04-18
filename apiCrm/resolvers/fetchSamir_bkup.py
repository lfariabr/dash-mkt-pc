import asyncio
import logging
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from .fetch_graphql import fetch_graphql
from dotenv import load_dotenv
import os
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
            
            meta = appointments_report.get('meta', {})
            if meta is None:
                meta = {}  # Provide a default empty dict if meta is None
            
            # New block to transform appointments
            transformed_appointments = []
            for appointment in appointments_data:

                try:
                    id = appointment.get('id', '')
                    startDate = appointment.get('startDate', '')
                    endDate = appointment.get('endDate', '')
                    beforePhotoUrl = appointment.get('beforePhotoUrl', '')
                    batchPhotoUrl = appointment.get('batchPhotoUrl', '')
                    afterPhotoUrl = appointment.get('afterPhotoUrl', '')
                    updatedAt = appointment.get('updatedAt', '')
                    status = appointment.get('status', {})
                    oldestParent = appointment.get('oldestParent', {})
                    customer = appointment.get('customer', {})
                    store = appointment.get('store', {})
                    procedure = appointment.get('procedure', {})
                    employee = appointment.get('employee', {})
                    comments = appointment.get('comments', [])
                    updatedBy = appointment.get('updatedBy', {})
                    latestProgressComment = appointment.get('latestProgressComment', {})

                    # Nested objects
                    customer_source = customer.get('source', {})
                    latestProgressComment_user = latestProgressComment.get('user', {})
                    
                    # Telephones - join with forward slash as specified in the rules
                    telephones_data = customer.get('telephones', [])
                    telephones = '/'.join([tel.get('number', '') for tel in telephones_data if tel and isinstance(tel, dict)]) if telephones_data else ''

                    # Comments
                    comments_data = appointment.get('comments', [])
                    
                    # Sort comments by createdAt if available to get the latest one
                    sorted_comments = sorted(
                        [c for c in comments_data if c and isinstance(c, dict)],
                        key=lambda x: x.get('createdAt', ''),
                        reverse=True
                    ) if comments_data else []
                    
                    latest_comment = sorted_comments[0].get('comment', '') if sorted_comments else ''
                    
                    # Formatted date-times
                    formatted_start_date = ''
                    formatted_start_time = ''
                    duration = ''
                    
                    if startDate:
                        try:
                            dt_start = datetime.fromisoformat(startDate.replace('Z', '+00:00'))
                            formatted_start_date = dt_start.strftime('%d/%m/%Y')
                            formatted_start_time = dt_start.strftime('%H:%M:%S')
                            
                            if endDate:
                                dt_end = datetime.fromisoformat(endDate.replace('Z', '+00:00'))
                                delta = dt_end - dt_start
                                hours, remainder = divmod(delta.seconds, 3600)
                                minutes, seconds = divmod(remainder, 60)
                                duration = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
                        except (ValueError, TypeError) as e:
                            logger.error(f"Error formatting date {startDate}: {str(e)}")
                    
                    # Format the oldestParent.createdAt date
                    formatted_created_at = ''
                    created_at = oldestParent.get('createdAt', '')
                    if created_at:
                        try:
                            dt_created = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                            formatted_created_at = dt_created.strftime('%d/%m/%Y %H:%M')
                        except (ValueError, TypeError) as e:
                            logger.error(f"Error formatting date {created_at}: {str(e)}")
                    
                    # Format the updatedAt date
                    formatted_updated_at = ''
                    if updatedAt:
                        try:
                            dt_updated = datetime.fromisoformat(updatedAt.replace('Z', '+00:00'))
                            formatted_updated_at = dt_updated.strftime('%d/%m/%Y %H:%M')
                        except (ValueError, TypeError) as e:
                            logger.error(f"Error formatting date {updatedAt}: {str(e)}")
                    
                    # Format latestProgressComment.createdAt
                    formatted_progress_date = ''
                    progress_created_at = latestProgressComment.get('createdAt', '')
                    if progress_created_at:
                        try:
                            dt_progress = datetime.fromisoformat(progress_created_at.replace('Z', '+00:00'))
                            formatted_progress_date = dt_progress.strftime('%d/%m/%Y %H:%M')
                        except (ValueError, TypeError) as e:
                            logger.error(f"Error formatting date {progress_created_at}: {str(e)}")

                    # Map fields to the expected column names for the UI, with default values
                    transformed_appointment = {
                        'ID agendamento': id,  # A -> id
                        'ID cliente': customer.get('id', ''),  # B -> customer.id
                        'Nome cliente': customer.get('name', ''),  # C -> customer.name
                        'CPF': customer.get('taxvatFormatted', ''),  # D -> customer.taxvatFormatted
                        'Email': customer.get('email', ''),  # E -> customer.email
                        'Telefone': telephones,  # F -> customer.telephones.*.number joined with /
                        'Endereço': customer.get('addressLine', ''),  # G -> customer.addressLine
                        'Fonte de cadastro do cliente': customer_source.get('title', ''),  # H -> customer.source.title
                        'Unidade do agendamento': store.get('name', ''),  # I -> store.name
                        'Procedimento': procedure.get('name', ''),  # J -> procedure.name
                        'Prestador': employee.get('name', ''),  # K -> employee.name
                        'Grupo do procedimento': procedure.get('groupLabel', ''),  # L -> procedure.groupLabel
                        'Data': formatted_start_date,  # M -> startDate formatted as dd/MM/yyyy
                        'Hora': formatted_start_time,  # N -> startDate formatted as HH:mm:ss
                        'Status': status.get('label', ''),  # O -> status.label
                        'Duração': duration,  # P -> startDate + endDate (calculate duration)
                        'Máquina': None,  # Q -> NULL
                        'Data primeira atendente': formatted_created_at,  # R -> oldestParent.createdAt (format dd/MM/yyyy HH:mm)
                        'Nome da primeira atendente': oldestParent.get('createdBy', {}).get('name', ''),  # S -> oldestParent.createdBy.name
                        'Grupo da primeira atendente': oldestParent.get('createdBy', {}).get('group', {}).get('name', ''),  # T -> oldestParent.createdBy.group.name
                        'Observação (mais recente)': latest_comment,  # U -> comments.comment (most recent comment)
                        'Última data de alteração do status': formatted_updated_at,  # V -> updatedAt formatted as dd/MM/yyyy HH:mm
                        'Último usuário a alterar o status': updatedBy.get('name', ''),  # W -> updatedBy.name
                        'Possui evolução?': 'Sim' if latestProgressComment.get('comment', '') else 'Não',  # X -> latestProgressComment.comment (existence check)
                        'Comentário mais recente da evolução': latestProgressComment.get('comment', ''),  # Y -> latestProgressComment.comment
                        'Data mais recente da evolução': formatted_progress_date,  # Z -> latestProgressComment.createdAt
                        'Usuário mais recente da evolução': latestProgressComment_user.get('name', ''),  # AA -> latestProgressComment.user.name
                        'Tem foto do lote?': 'Sim' if batchPhotoUrl else 'Não',  # AB -> batchPhotoUrl (if exists "Sim", otherwise "Não")
                        'Tem foto do antes?': 'Sim' if beforePhotoUrl else 'Não',  # AC -> beforePhotoUrl (if exists "Sim", otherwise "Não")
                        'Tem foto do depois?': 'Sim' if afterPhotoUrl else 'Não'  # AD -> afterPhotoUrl (if exists "Sim", otherwise "Não")
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
                                
                                # Transform page appointments before adding to our list
                                page_transformed_appointments = []
                                for appointment in page_appointments:
                                    try:
                                        # Extract top-level fields with proper defaults
                                        id = appointment.get('id', '')
                                        startDate = appointment.get('startDate', '')
                                        endDate = appointment.get('endDate', '')
                                        beforePhotoUrl = appointment.get('beforePhotoUrl', '')
                                        batchPhotoUrl = appointment.get('batchPhotoUrl', '')
                                        afterPhotoUrl = appointment.get('afterPhotoUrl', '')
                                        updatedAt = appointment.get('updatedAt', '')
                                        
                                        # Extract nested objects with proper defaults
                                        status = appointment.get('status', {}) or {}
                                        oldestParent = appointment.get('oldestParent', {}) or {}
                                        customer = appointment.get('customer', {}) or {}
                                        store = appointment.get('store', {}) or {}
                                        procedure = appointment.get('procedure', {}) or {}
                                        employee = appointment.get('employee', {}) or {}
                                        comments_data = appointment.get('comments', []) or []
                                        updatedBy = appointment.get('updatedBy', {}) or {}
                                        latestProgressComment = appointment.get('latestProgressComment', {}) or {}

                                        # Handle nested objects
                                        customer_source = customer.get('source', {}) or {}
                                        latestProgressComment_user = latestProgressComment.get('user', {}) or {}
                                        oldest_parent_created_by = oldestParent.get('createdBy', {}) or {}
                                        oldest_parent_created_by_group = oldest_parent_created_by.get('group', {}) or {}
                                        
                                        # F -> customer.telephones.*.number (join with /)
                                        telephones_data = customer.get('telephones', []) or []
                                        telephones = '/'.join([tel.get('number', '') for tel in telephones_data if tel and isinstance(tel, dict)]) if telephones_data else ''

                                        # U -> comments.comment (most recent comment)
                                        # Sort comments by createdAt to get the most recent one
                                        sorted_comments = sorted(
                                            [c for c in comments_data if c and isinstance(c, dict) and c.get('comment')],
                                            key=lambda x: x.get('createdAt', ''),
                                            reverse=True
                                        ) if comments_data else []
                                        
                                        latest_comment = sorted_comments[0].get('comment', '') if sorted_comments else ''
                                        
                                        # M -> startDate (formatado como dd/MM/yyyy)
                                        # N -> startDate (formato HH:mm:ss)
                                        # P -> startDate + endDate (calculate duration)
                                        formatted_start_date = ''
                                        formatted_start_time = ''
                                        duration = ''
                                        
                                        if startDate:
                                            try:
                                                dt_start = datetime.fromisoformat(startDate.replace('Z', '+00:00'))
                                                formatted_start_date = dt_start.strftime('%d/%m/%Y')
                                                formatted_start_time = dt_start.strftime('%H:%M:%S')
                                                
                                                if endDate:
                                                    dt_end = datetime.fromisoformat(endDate.replace('Z', '+00:00'))
                                                    delta = dt_end - dt_start
                                                    hours, remainder = divmod(delta.seconds, 3600)
                                                    minutes, seconds = divmod(remainder, 60)
                                                    duration = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
                                            except (ValueError, TypeError) as e:
                                                logger.error(f"Error formatting date {startDate}: {str(e)}")
                                        
                                        # R -> oldestParent.createdAt (format dd/MM/yyyy HH:mm)
                                        formatted_created_at = ''
                                        created_at = oldestParent.get('createdAt', '')
                                        if created_at:
                                            try:
                                                dt_created = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                                                formatted_created_at = dt_created.strftime('%d/%m/%Y %H:%M')
                                            except (ValueError, TypeError) as e:
                                                logger.error(f"Error formatting date {created_at}: {str(e)}")
                                        
                                        # V -> updatedAt (format dd/MM/yyyy HH:mm)
                                        formatted_updated_at = ''
                                        if updatedAt:
                                            try:
                                                dt_updated = datetime.fromisoformat(updatedAt.replace('Z', '+00:00'))
                                                formatted_updated_at = dt_updated.strftime('%d/%m/%Y %H:%M')
                                            except (ValueError, TypeError) as e:
                                                logger.error(f"Error formatting date {updatedAt}: {str(e)}")
                                        
                                        # Z -> latestProgressComment.createdAt (format dd/MM/yyyy HH:mm)
                                        formatted_progress_date = ''
                                        progress_created_at = latestProgressComment.get('createdAt', '')
                                        if progress_created_at:
                                            try:
                                                dt_progress = datetime.fromisoformat(progress_created_at.replace('Z', '+00:00'))
                                                formatted_progress_date = dt_progress.strftime('%d/%m/%Y %H:%M')
                                            except (ValueError, TypeError) as e:
                                                logger.error(f"Error formatting date {progress_created_at}: {str(e)}")

                                        # Map fields to the expected column names according to lines 311-339
                                        transformed_appointment = {
                                            'ID agendamento': id,                                             # A -> id
                                            'ID cliente': customer.get('id', ''),                             # B -> customer.id
                                            'Nome cliente': customer.get('name', ''),                         # C -> customer.name
                                            'CPF': customer.get('taxvatFormatted', ''),                       # D -> customer.taxvatFormatted
                                            'Email': customer.get('email', ''),                               # E -> customer.email
                                            'Telefone': telephones,                                           # F -> customer.telephones.*.number joined with /
                                            'Endereço': customer.get('addressLine', ''),                      # G -> customer.addressLine
                                            'Fonte de cadastro do cliente': customer_source.get('title', ''), # H -> customer.source.title
                                            'Unidade do agendamento': store.get('name', ''),                  # I -> store.name
                                            'Procedimento': procedure.get('name', ''),                        # J -> procedure.name
                                            'Prestador': employee.get('name', ''),                            # K -> employee.name
                                            'Grupo do procedimento': procedure.get('groupLabel', ''),         # L -> procedure.groupLabel
                                            'Data': formatted_start_date,                                     # M -> startDate formatted as dd/MM/yyyy
                                            'Hora': formatted_start_time,                                     # N -> startDate formatted as HH:mm:ss
                                            'Status': status.get('label', ''),                                # O -> status.label
                                            'Duração': duration,                                              # P -> startDate + endDate (calculate duration)
                                            'Máquina': None,                                                  # Q -> NULL
                                            'Data primeira atendente': formatted_created_at,                  # R -> oldestParent.createdAt (format dd/MM/yyyy HH:mm)
                                            'Nome da primeira atendente': oldest_parent_created_by.get('name', ''), # S -> oldestParent.createdBy.name
                                            'Grupo da primeira atendente': oldest_parent_created_by_group.get('name', ''), # T -> oldestParent.createdBy.group.name
                                            'Observação (mais recente)': latest_comment,                      # U -> comments.comment (most recent comment)
                                            'Última data de alteração do status': formatted_updated_at,       # V -> updatedAt formatted as dd/MM/yyyy HH:mm
                                            'Último usuário a alterar o status': updatedBy.get('name', ''),   # W -> updatedBy.name
                                            'Possui evolução?': 'Sim' if latestProgressComment.get('comment', '') else 'Não', # X -> latestProgressComment.comment (existence check)
                                            'Comentário mais recente da evolução': latestProgressComment.get('comment', ''),  # Y -> latestProgressComment.comment
                                            'Data mais recente da evolução': formatted_progress_date,         # Z -> latestProgressComment.createdAt
                                            'Usuário mais recente da evolução': latestProgressComment_user.get('name', ''), # AA -> latestProgressComment.user.name
                                            'Tem foto do lote?': 'Sim' if batchPhotoUrl else 'Não',           # AB -> batchPhotoUrl (if exists "Sim", otherwise "Não")
                                            'Tem foto do antes?': 'Sim' if beforePhotoUrl else 'Não',         # AC -> beforePhotoUrl (if exists "Sim", otherwise "Não")
                                            'Tem foto do depois?': 'Sim' if afterPhotoUrl else 'Não'          # AD -> afterPhotoUrl (if exists "Sim", otherwise "Não")
                                        }
                                        page_transformed_appointments.append(transformed_appointment)
                                    except Exception as e:
                                        logger.error(f"Error processing individual appointment from page {page}: {str(e)}")
                                        # Continue with the next appointment rather than failing entirely
                                        continue
                                
                                # Add only transformed appointments to our list
                                all_appointments.extend(page_transformed_appointments)
                                logger.info(f"Added {len(page_transformed_appointments)} transformed appointments from page {page}")
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
            return all_appointments
            
    except Exception as e:
        logger.error(f"Error making GraphQL request: {str(e)}")
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


"""
ID agendamento: A -> id
ID cliente: B -> customer.id
Nome cliente: C -> customer.name
CPF: D -> customer.taxvatFormatted
Email: E -> customer.email
Telefone: F -> customer.telephones.*.number (customer.telephones.*.number (nós damos um join em todos telefones e separamos por /)
Endereço: G -> customer.addressLine
Fonte de cadastro do cliente: H -> customer.source.title
Unidade do agendamento: I -> store.name
Procedimento: J -> procedure.name
Prestador: K -> employee.name
Grupo do procedimento: L -> procedure.groupLabel
Data: M -> startDate  (formatado como dd/MM/yyyy)
Hora: N -> startDate (formato HH:mm:ss)  (startDate + endDate (nós fazemos um diff entre as duas datas)
Status: O -> status.label
Duração: P -> startDate + endDate
Máquina: Q -> NULL
Data primeira atendente: R -> oldestParent.createdAt (quando disponível) ou createdAt (caso contrário) | (formato dd/MM/yyyy HH:mm)
Nome da primeira atendente: S -> oldestParent.createdBy.name (quando disponível) ou createdBy.name (caso contrário)
Grupo da primeira atendente: T -> oldestParent.createdBy.group.name (quando disponível) ou createdBy.group.name (caso contrário)
Observação (mais recente): U -> comments.comment (nós pegamos o comentário mais recente, ordenando pela coluna comments.createdAt em ordem decrescente)
Última data de alteração do status: V -> updatedAt  (formato dd/MM/yyyy HH:mm)
Último usuário a alterar o status: W -> updatedBy.name (se existir nós exibimos Sim, caso contrário Não)
Possui evolução?: X -> latestProgressComment.comment 
Comentário mais recente da evolução: Y -> latestProgressComment.comment
Data mais recente da evolução: Z -> latestProgressComment.createdAt
Usuário mais recente da evolução: AA -> latestProgressComment.user.name
Tem foto do lote?: AB -> batchPhotoUrl (se existir nós exibimos Sim, caso contrário Não)
Tem foto do antes?: AC -> beforePhotoUrl (se existir nós exibimos Sim, caso contrário Não)
Tem foto do depois?: AD -> afterPhotoUrl (se existir nós exibimos Sim, caso contrário Não)
"""

def calculate_duration(start_date, end_date):
    start = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
    end = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
    duration = end - start
    hours, remainder = divmod(duration.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

def format_date_time(date_time):
    if date_time:
        return datetime.fromisoformat(date_time.replace('Z', '+00:00')).strftime('%d/%m/%Y %H:%M:%S')
    return ''

def get_latest_comment(comments):
    if comments:
        comments.sort(key=lambda x: x.get('createdAt', ''), reverse=True)
        return comments[0].get('comment', '')
    return ''