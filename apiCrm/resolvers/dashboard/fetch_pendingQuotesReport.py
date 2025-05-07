"""
cd /Users/luisfaria/Desktop/sEngineer/dash
python -m apiCrm.tests.fetch_pendingQuotesReport_test
"""

import asyncio
import logging
from typing import List, Dict
from ..fetch_graphql import fetch_graphql
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

logger = logging.getLogger(__name__)

async def fetch_pendingQuotesReport(session, start_date: str, end_date: str) -> List[Dict]:
    """
    Fetches pending quotes report data from the CRM API within a specified date range.
    
    Args:
        session: The aiohttp ClientSession object
        start_date: Start date in ISO format (YYYY-MM-DD)
        end_date: End date in ISO format (YYYY-MM-DD)
        
    Returns:
        List of pending quotes report dictionaries
    """
    current_page = 1
    all_pending_quotes = []
    api_url = os.getenv('API_CRM_URL', 'https://open-api.queromeubotox.com.br/graphql')

    # Updated query with non-nullable types (Date!) for date parameters
    query = '''
    query pendingQuotesReport($start: Date!, $end: Date!, $currentPage: Int = 1) {
      pendingQuotesReport(
        filters: { 
          createdAtRange: { 
            start: $start, 
            end: $end 
          } 
        }
        pagination: { 
          currentPage: $currentPage, 
          perPage: 100 
        }
      ) {
        data {
            id
            createdAt
            statusLabel
            isReseller
            subtotal
            discountAmount
            total
            comments
            expirationDate

            store {
                name
            }

            customer {
                name
                email
                primaryTelephone
                taxvatFormatted
                addressLine
                address {
                    street
                    number
                    additional
                    neighborhood
                    city
                    state {
                        name
                    }
                    postcode
                }
            }

            createdBy {
                name
            }

            procedures {
                name
                groupLabel
            }
        }
        meta {
          currentPage
          lastPage
          total
        }
      }
    }
    '''

    variables = {
        'start': start_date,
        'end': end_date,
        'currentPage': current_page
    }
    
    logger.info(f"Attempting to fetch pending quotes from {start_date} to {end_date}")
    
    data = await fetch_graphql(session, api_url, query, variables)

    if data is None or 'errors' in data:
        error_msg = data.get('errors', [{'message': 'Unknown error'}])[0]['message'] if data else 'No data returned'
        logger.error(f"Failed initial pending quotes fetch: {error_msg}")
        return all_pending_quotes  # Return empty list on initial failure
    
    # If we got here, the query structure works
    try:
        if 'data' in data and 'pendingQuotesReport' in data['data']:
            pending_quotes_data = data['data']['pendingQuotesReport']['data']
            meta = data['data']['pendingQuotesReport']['meta']

            transformed_quotes = []

            for quote in pending_quotes_data:
                # Extract nested objects safely
                customer = quote.get('customer', {}) or {}
                store = quote.get('store', {}) or {}
                createdBy = quote.get('createdBy', {}) or {}
                procedures = quote.get('procedures', []) or []
                
                # Extract address data if available
                address = customer.get('address', {}) or {}
                state = address.get('state', {}) or {}
                
                # Format procedures
                procedure_names = ', '.join([proc.get('name', '') for proc in procedures if proc.get('name')]) if procedures else None
                procedure_groups = ', '.join([proc.get('groupLabel', '') for proc in procedures if proc.get('groupLabel')]) if procedures else None
                
                # Create complete address string if address data is available
                full_address = None
                if address:
                    address_parts = []
                    if address.get('street'):
                        address_parts.append(f"{address.get('street')}")
                    if address.get('number'):
                        address_parts.append(f"{address.get('number')}")
                    if address.get('additional'):
                        address_parts.append(f"{address.get('additional')}")
                    if address.get('neighborhood'):
                        address_parts.append(f"{address.get('neighborhood')}")
                    if address.get('city'):
                        address_parts.append(f"{address.get('city')}")
                    if state.get('name'):
                        address_parts.append(f"{state.get('name')}")
                    if address.get('postcode'):
                        address_parts.append(f"{address.get('postcode')}")
                    
                    if address_parts:
                        full_address = ', '.join(address_parts)
                
                # Fallback to addressLine if full address couldn't be constructed
                if not full_address and customer.get('addressLine'):
                    full_address = customer.get('addressLine')

                transformed_quote = {
                    'id': quote.get('id'),
                    'createdAt': quote.get('createdAt'),
                    'statusLabel': quote.get('statusLabel'),
                    'isReseller': quote.get('isReseller'),
                    'subtotal': quote.get('subtotal'),
                    'discountAmount': quote.get('discountAmount'),
                    'total': quote.get('total'),
                    'comments': quote.get('comments'),
                    'expirationDate': quote.get('expirationDate'),
                    
                    # Store and creator info
                    'store_name': store.get('name'),
                    'createdBy': createdBy.get('name'),
                    
                    # Customer info
                    'customer_name': customer.get('name'),
                    'customer_email': customer.get('email'),
                    'telephone': customer.get('primaryTelephone'),
                    'taxvatFormatted': customer.get('taxvatFormatted'),
                    'address': full_address,
                    
                    # Procedures
                    'procedures': procedure_names,
                    'procedure_groups': procedure_groups,
                }

                transformed_quotes.append(transformed_quote)
        
            # Add first page to results
            all_pending_quotes.extend(transformed_quotes)
                
            last_page = meta.get('lastPage', 1)
            total_items = meta.get('total', len(pending_quotes_data))
                
            logger.info(f"Successfully fetched page 1/{last_page}, got {len(pending_quotes_data)} quotes")
                
            # If we have more pages, fetch them
            if last_page > 1:
                for page in range(2, last_page + 1):
                    variables['currentPage'] = page
                    page_data = await fetch_graphql(session, api_url, query, variables)
                    
                    if not page_data or 'errors' in page_data:
                        logger.error(f"Error fetching page {page}/{last_page}")
                        continue
                    
                    if 'data' in page_data and 'pendingQuotesReport' in page_data['data']:
                        page_quotes = page_data['data']['pendingQuotesReport']['data']
                        
                        # Transform data for this page
                        page_transformed = []
                        for quote in page_quotes:
                            # Extract nested objects safely
                            customer = quote.get('customer', {}) or {}
                            store = quote.get('store', {}) or {}
                            createdBy = quote.get('createdBy', {}) or {}
                            procedures = quote.get('procedures', []) or []
                            
                            # Extract address data if available
                            address = customer.get('address', {}) or {}
                            state = address.get('state', {}) or {}
                            
                            # Format procedures
                            procedure_names = ', '.join([proc.get('name', '') for proc in procedures if proc.get('name')]) if procedures else None
                            procedure_groups = ', '.join([proc.get('groupLabel', '') for proc in procedures if proc.get('groupLabel')]) if procedures else None
                            
                            # Create complete address string if address data is available
                            full_address = None
                            if address:
                                address_parts = []
                                if address.get('street'):
                                    address_parts.append(f"{address.get('street')}")
                                if address.get('number'):
                                    address_parts.append(f"{address.get('number')}")
                                if address.get('additional'):
                                    address_parts.append(f"{address.get('additional')}")
                                if address.get('neighborhood'):
                                    address_parts.append(f"{address.get('neighborhood')}")
                                if address.get('city'):
                                    address_parts.append(f"{address.get('city')}")
                                if state.get('name'):
                                    address_parts.append(f"{state.get('name')}")
                                if address.get('postcode'):
                                    address_parts.append(f"{address.get('postcode')}")
                                
                                if address_parts:
                                    full_address = ', '.join(address_parts)
                            
                            # Fallback to addressLine if full address couldn't be constructed
                            if not full_address and customer.get('addressLine'):
                                full_address = customer.get('addressLine')

                            transformed_quote = {
                                'id': quote.get('id'),
                                'createdAt': quote.get('createdAt'),
                                'statusLabel': quote.get('statusLabel'),
                                'isReseller': quote.get('isReseller'),
                                'subtotal': quote.get('subtotal'),
                                'discountAmount': quote.get('discountAmount'),
                                'total': quote.get('total'),
                                'comments': quote.get('comments'),
                                'expirationDate': quote.get('expirationDate'),
                                
                                # Store and creator info
                                'store_name': store.get('name'),
                                'createdBy': createdBy.get('name'),
                                
                                # Customer info
                                'customer_name': customer.get('name'),
                                'customer_email': customer.get('email'),
                                'telephone': customer.get('primaryTelephone'),
                                'taxvatFormatted': customer.get('taxvatFormatted'),
                                'address': full_address,
                                
                                # Procedures
                                'procedures': procedure_names,
                                'procedure_groups': procedure_groups,
                            }
                            page_transformed.append(transformed_quote)
                        
                        all_pending_quotes.extend(page_transformed)
                        logger.info(f"Successfully fetched page {page}/{last_page}, got {len(page_quotes)} quotes")
                    
                    # Add a delay between requests to avoid rate limiting
                    await asyncio.sleep(2)
        else:
            logger.error(f"Unexpected API response structure: {data}")
    
    except Exception as e:
        logger.error(f"Error processing pending quotes data: {str(e)}")
    
    logger.info(f"Total pending quotes fetched: {len(all_pending_quotes)}")
    return all_pending_quotes

async def fetch_and_process_pendingQuotes_report(start_date: str, end_date: str) -> List[Dict]:
    """
    Creates a session and fetches pending quotes report data.
    
    Args:
        start_date: Start date in ISO format (YYYY-MM-DD)
        end_date: End date in ISO format (YYYY-MM-DD)
        
    Returns:
        List of processed pending quotes report dictionaries ready for database insertion
    """
    import aiohttp
    
    async with aiohttp.ClientSession() as session:
        quotes = await fetch_pendingQuotesReport(session, start_date, end_date)
    
    return quotes

# For testing purposes
if __name__ == "__main__":
    async def test_fetch():
        start_date = "2023-01-01"
        end_date = "2023-01-31"
        results = await fetch_and_process_pendingQuotes_report(start_date, end_date)
        print(f"Fetched {len(results)} pending quotes records")
        if results:
            # Print first result as sample
            import json
            print(json.dumps(results[0], indent=2))
    
    asyncio.run(test_fetch())
