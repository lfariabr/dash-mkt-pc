"""
cd /Users/luisfaria/Desktop/sEngineer/dash
python -m apiCrm.tests.fetch_salesByPaymentMethodReports_test
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

async def fetch_salesByPaymentMethodReport(session, start_date: str, end_date: str) -> List[Dict]:
    """
    Fetches sales by payment method report data from the CRM API within a specified date range.
    
    Args:
        session: The aiohttp ClientSession object
        start_date: Start date in ISO format (YYYY-MM-DD)
        end_date: End date in ISO format (YYYY-MM-DD)
        
    Returns:
        List of sales by payment method report dictionaries
    """
    current_page = 1
    all_payment_reports = []
    api_url = os.getenv('API_CRM_URL', 'https://open-api.queromeubotox.com.br/graphql')

    # Updated query with non-nullable types (Date!) for date parameters
    query = '''
    query salesByPaymentMethodReport($start: Date!, $end: Date!, $currentPage: Int = 1) {
      salesByPaymentMethodReport(
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
            amount
            dueAt
            paidAmount
            isPaid

            bill {
                quote {
                    comments
                    createdAt
                    customerSignedAt
                    discountAmount
                    id
                    isReseller
                    statusLabel
                    subtotal

                    store {
                        name
                    }

                    createdBy {
                        name
                    }

                    cancelledBy {
                        name
                    }
                }

                items {
                    description

                    procedure {
                        groupLabel
                    }
                }

                customer {
                    addressLine
                    email
                    name
                    taxvatFormatted

                    address {
                        street
                        number
                        additional
                        neighborhood
                        city
                        postcode

                        state {
                            name
                        }
                    }
                }
            }

            paymentMethod {
                displayAmountOnReport
                name
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
    
    logger.info(f"Attempting to fetch sales by payment method from {start_date} to {end_date}")
    
    data = await fetch_graphql(session, api_url, query, variables)

    if data is None or 'errors' in data:
        error_msg = data.get('errors', [{'message': 'Unknown error'}])[0]['message'] if data else 'No data returned'
        logger.error(f"Failed initial sales by payment method fetch: {error_msg}")
        return all_payment_reports  # Return empty list on initial failure
    
    # If we got here, the query structure works
    try:
        if 'data' in data and 'salesByPaymentMethodReport' in data['data']:
            payment_reports_data = data['data']['salesByPaymentMethodReport']['data']
            meta = data['data']['salesByPaymentMethodReport']['meta']

            transformed_reports = []

            for report in payment_reports_data:
                # Extract nested objects safely
                bill = report.get('bill', {}) or {}
                quote = bill.get('quote', {}) or {}
                store = quote.get('store', {}) or {}
                createdBy = quote.get('createdBy', {}) or {}
                cancelledBy = quote.get('cancelledBy', {}) or {}
                customer = bill.get('customer', {}) or {}
                paymentMethod = report.get('paymentMethod', {}) or {}
                bill_items = bill.get('items', []) or []
                
                # Extract address data if available
                address = customer.get('address', {}) or {}
                state = address.get('state', {}) or {}
                
                # Format bill items and procedures
                items_descriptions = ', '.join([
                    item.get('description', '') 
                    for item in bill_items if item.get('description')
                ]) if bill_items else None
                
                procedure_group_labels = ', '.join([
                    item.get('procedure', {}).get('groupLabel', '') 
                    for item in bill_items if item.get('procedure', {}).get('groupLabel')
                ]) if bill_items else None
                
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

                transformed_report = {
                    # Payment details
                    'amount': report.get('amount'),
                    'dueAt': report.get('dueAt'),
                    'paidAmount': report.get('paidAmount'),
                    'isPaid': report.get('isPaid'),
                    'payment_method': paymentMethod.get('name'),
                    'display_amount_on_report': paymentMethod.get('displayAmountOnReport'),
                    
                    # Quote details
                    'quote_id': quote.get('id'),
                    'createdAt': quote.get('createdAt'),
                    'customerSignedAt': quote.get('customerSignedAt'),
                    'discountAmount': quote.get('discountAmount'),
                    'isReseller': quote.get('isReseller'),
                    'statusLabel': quote.get('statusLabel'),
                    'subtotal': quote.get('subtotal'),
                    'comments': quote.get('comments'),
                    
                    # Store and user info
                    'store_name': store.get('name'),
                    'createdBy': createdBy.get('name'),
                    'cancelledBy': cancelledBy.get('name') if cancelledBy else None,
                    
                    # Bill items and procedures
                    'items_descriptions': items_descriptions,
                    'procedure_group_labels': procedure_group_labels,
                    
                    # Customer info
                    'customer_name': customer.get('name'),
                    'customer_email': customer.get('email'),
                    'taxvatFormatted': customer.get('taxvatFormatted'),
                    'address': full_address,
                }

                transformed_reports.append(transformed_report)
        
            # Add first page to results
            all_payment_reports.extend(transformed_reports)
                
            last_page = meta.get('lastPage', 1)
            total_items = meta.get('total', len(payment_reports_data))
                
            logger.info(f"Successfully fetched page 1/{last_page}, got {len(payment_reports_data)} payment reports")
                
            # If we have more pages, fetch them
            if last_page > 1:
                for page in range(2, last_page + 1):
                    variables['currentPage'] = page
                    page_data = await fetch_graphql(session, api_url, query, variables)
                    
                    if not page_data or 'errors' in page_data:
                        logger.error(f"Error fetching page {page}/{last_page}")
                        continue
                    
                    if 'data' in page_data and 'salesByPaymentMethodReport' in page_data['data']:
                        page_reports = page_data['data']['salesByPaymentMethodReport']['data']
                        
                        # Transform data for this page
                        page_transformed = []
                        for report in page_reports:
                            # Extract nested objects safely
                            bill = report.get('bill', {}) or {}
                            quote = bill.get('quote', {}) or {}
                            store = quote.get('store', {}) or {}
                            createdBy = quote.get('createdBy', {}) or {}
                            cancelledBy = quote.get('cancelledBy', {}) or {}
                            customer = bill.get('customer', {}) or {}
                            paymentMethod = report.get('paymentMethod', {}) or {}
                            bill_items = bill.get('items', []) or []
                            
                            # Extract address data if available
                            address = customer.get('address', {}) or {}
                            state = address.get('state', {}) or {}
                            
                            # Format bill items and procedures
                            items_descriptions = ', '.join([
                                item.get('description', '') 
                                for item in bill_items if item.get('description')
                            ]) if bill_items else None
                            
                            procedure_group_labels = ', '.join([
                                item.get('procedure', {}).get('groupLabel', '') 
                                for item in bill_items if item.get('procedure', {}).get('groupLabel')
                            ]) if bill_items else None
                            
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

                            transformed_report = {
                                # Payment details
                                'amount': report.get('amount'),
                                'dueAt': report.get('dueAt'),
                                'paidAmount': report.get('paidAmount'),
                                'isPaid': report.get('isPaid'),
                                'payment_method': paymentMethod.get('name'),
                                'display_amount_on_report': paymentMethod.get('displayAmountOnReport'),
                                
                                # Quote details
                                'quote_id': quote.get('id'),
                                'createdAt': quote.get('createdAt'),
                                'customerSignedAt': quote.get('customerSignedAt'),
                                'discountAmount': quote.get('discountAmount'),
                                'isReseller': quote.get('isReseller'),
                                'statusLabel': quote.get('statusLabel'),
                                'subtotal': quote.get('subtotal'),
                                'comments': quote.get('comments'),
                                
                                # Store and user info
                                'store_name': store.get('name'),
                                'createdBy': createdBy.get('name'),
                                'cancelledBy': cancelledBy.get('name') if cancelledBy else None,
                                
                                # Bill items and procedures
                                'items_descriptions': items_descriptions,
                                'procedure_group_labels': procedure_group_labels,
                                
                                # Customer info
                                'customer_name': customer.get('name'),
                                'customer_email': customer.get('email'),
                                'taxvatFormatted': customer.get('taxvatFormatted'),
                                'address': full_address,
                            }
                            page_transformed.append(transformed_report)
                        
                        all_payment_reports.extend(page_transformed)
                        logger.info(f"Successfully fetched page {page}/{last_page}, got {len(page_reports)} payment reports")
                    
                    # Add a delay between requests to avoid rate limiting
                    await asyncio.sleep(2)
        else:
            logger.error(f"Unexpected API response structure: {data}")
    
    except Exception as e:
        logger.error(f"Error processing sales by payment method data: {str(e)}")
    
    logger.info(f"Total sales by payment method fetched: {len(all_payment_reports)}")
    return all_payment_reports

async def fetch_and_process_salesByPaymentMethod_report(start_date: str, end_date: str) -> List[Dict]:
    """
    Creates a session and fetches sales by payment method report data.
    
    Args:
        start_date: Start date in ISO format (YYYY-MM-DD)
        end_date: End date in ISO format (YYYY-MM-DD)
        
    Returns:
        List of processed sales by payment method report dictionaries ready for database insertion
    """
    import aiohttp
    
    async with aiohttp.ClientSession() as session:
        reports = await fetch_salesByPaymentMethodReport(session, start_date, end_date)
    
    return reports

# For testing purposes
if __name__ == "__main__":
    async def test_fetch():
        start_date = "2023-01-01"
        end_date = "2023-01-31"
        results = await fetch_and_process_salesByPaymentMethod_report(start_date, end_date)
        print(f"Fetched {len(results)} sales by payment method records")
        if results:
            # Print first result as sample
            import json
            print(json.dumps(results[0], indent=2, default=str))
    
    asyncio.run(test_fetch())
