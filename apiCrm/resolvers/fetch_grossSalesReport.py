"""
cd /Users/luisfaria/Desktop/sEngineer/dash
python -m apiCrm.tests.fetch_grossSalesReport_test
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

async def fetch_grossSalesReport(session, start_date: str, end_date: str) -> List[Dict]:
    """
    Fetches gross sales report data from the CRM API within a specified date range.
    
    Args:
        session: The aiohttp ClientSession object
        start_date: Start date in ISO format (YYYY-MM-DD)
        end_date: End date in ISO format (YYYY-MM-DD)
        
    Returns:
        List of gross sales report dictionaries
    """
    current_page = 1
    all_gross_sales = []
    api_url = os.getenv('API_CRM_URL', 'https://open-api.queromeubotox.com.br/graphql')

    # Updated query with non-nullable types (Date!) for date parameters
    query = '''
    query GrossSalesReport($start: Date!, $end: Date!, $currentPage: Int = 1) {
      grossSalesReport(
        filters: { 
          createdAtRange: { 
            start: $start, 
            end: $end 
          } 
        }
        pagination: { 
          currentPage: $currentPage, 
          perPage: 400 
        }
      ) {
        data {
          id
          isFree
          createdAt
          status
          statusLabel
          isReseller
          customerSignedAt
          store {
            name
          }
          createdBy {
            name
          }
          evaluations {
            employee {
              name
            }
          }
          bill {
            chargableTotal
            items {
              amount
              quantity
              discountAmount
              discountPercentage
              description
              procedure {
                groupLabel
              }
            }
          }
          customer {
            name
            email
            taxvat
            taxvatFormatted
            source {
              title
            }
            telephones {
              number
            }
            birthdate
            occupation {
              title
            }
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
    
    logger.info(f"Attempting to fetch gross sales from {start_date} to {end_date}")
    
    data = await fetch_graphql(session, api_url, query, variables)

    if data is None or 'errors' in data:
        error_msg = 'No data returned' if data is None else data.get('errors', [{'message': 'Unknown error'}])[0]['message']
        logger.error(f"Failed initial gross sales fetch: {error_msg}")
        return all_gross_sales  # Return empty list on initial failure
    
    # If we got here, the query structure works
    try:
        if 'data' in data and 'grossSalesReport' in data['data']:
            sales_data = data['data']['grossSalesReport']['data']
            meta = data['data']['grossSalesReport']['meta']

            transformed_sales = []

            for sale in sales_data:
                customer = sale.get('customer', {}) or {}
                store = sale.get('store', {}) or {}
                createdBy = sale.get('createdBy', {}) or {}
                evaluations = sale.get('evaluations', []) or []
                bill = sale.get('bill', {}) or {}
                bill_items = bill.get('items', []) or []

                # Format telephones
                telephones_data = customer.get('telephones', [])
                telephones = ', '.join([tel.get('number', '') for tel in telephones_data]) if telephones_data else None

                # Format evaluations (list of employees)
                employees = ', '.join([
                    (eval.get('employee') or {}).get('name', '') 
                    for eval in evaluations if eval.get('employee')
                ]) if evaluations else None

                # Format bill items
                items_descriptions = '; '.join([
                    f"{item.get('description', '')} (Q:{item.get('quantity', '')}, V:{item.get('amount', '')}, D:{item.get('discountAmount', '')} - {item.get('discountPercentage', '')}%)"
                    for item in bill_items
                ]) if bill_items else None

                # Format group labels from procedures in bill items
                procedure_group_labels = ', '.join([
                    (item.get('procedure') or {}).get('groupLabel', '') 
                    for item in bill_items if item.get('procedure')
                ]) if bill_items else None

                transformed_sale = {
                    'id': sale.get('id'),
                    'createdAt': sale.get('createdAt'),
                    'customerSignedAt': sale.get('customerSignedAt'),
                    'isFree': sale.get('isFree'),
                    'isReseller': sale.get('isReseller'),
                    
                    # Status
                    'status': sale.get('status'),
                    'statusLabel': sale.get('statusLabel'),

                    # Store and user
                    'store_name': store.get('name'),
                    'createdBy': createdBy.get('name'),

                    # Evaluations
                    'employees': employees,

                    # Bill
                    'chargableTotal': bill.get('chargableTotal'),
                    'bill_items': items_descriptions,
                    'procedure_groupLabels': procedure_group_labels,

                    # Customer
                    'customer_name': customer.get('name'),
                    'customer_email': customer.get('email'),
                    'taxvat': customer.get('taxvat'),
                    'taxvatFormatted': customer.get('taxvatFormatted'),
                    'birthdate': customer.get('birthdate'),
                    'telephones': telephones,
                    'source': (customer.get('source') or {}).get('title'),
                    'occupation': (customer.get('occupation') or {}).get('title'),
                }

                transformed_sales.append(transformed_sale)
        
            # Add first page to results
            all_gross_sales.extend(transformed_sales)
                
            last_page = meta.get('lastPage', 1)
            total_items = meta.get('total', len(sales_data))
                
            logger.info(f"Successfully fetched page 1/{last_page}, got {len(sales_data)} sales")
                
            # If we have more pages, fetch them
            if last_page > 1:
                for page in range(2, last_page + 1):
                    variables['currentPage'] = page
                    page_data = await fetch_graphql(session, api_url, query, variables)
                    
                    if not page_data or 'errors' in page_data:
                        logger.error(f"Error fetching page {page}/{last_page}")
                        continue
                    
                    if 'data' in page_data and 'grossSalesReport' in page_data['data']:
                        page_sales = page_data['data']['grossSalesReport']['data']
                        
                        # Transform data for this page
                        page_transformed = []
                        for sale in page_sales:
                            # Check if required fields exist to avoid None errors
                            customer = sale.get('customer', {}) or {}
                            store = sale.get('store', {}) or {}
                            createdBy = sale.get('createdBy', {}) or {}
                            evaluations = sale.get('evaluations', []) or []
                            bill = sale.get('bill', {}) or {}
                            bill_items = bill.get('items', []) or []

                            # Format telephones
                            telephones_data = customer.get('telephones', [])
                            telephones = ', '.join([tel.get('number', '') for tel in telephones_data]) if telephones_data else None

                            # Format evaluations (list of employees)
                            employees = ', '.join([
                                (eval.get('employee') or {}).get('name', '') 
                                for eval in evaluations if eval.get('employee')
                            ]) if evaluations else None

                            # Format bill items
                            items_descriptions = '; '.join([
                                f"{item.get('description', '')} (Q:{item.get('quantity', '')}, V:{item.get('amount', '')}, D:{item.get('discountAmount', '')} - {item.get('discountPercentage', '')}%)"
                                for item in bill_items
                            ]) if bill_items else None

                            # Format group labels from procedures in bill items
                            procedure_group_labels = ', '.join([
                                (item.get('procedure') or {}).get('groupLabel', '') 
                                for item in bill_items if item.get('procedure')
                            ]) if bill_items else None

                            transformed_sale = {
                                'id': sale.get('id'),
                                'createdAt': sale.get('createdAt'),
                                'customerSignedAt': sale.get('customerSignedAt'),
                                'isFree': sale.get('isFree'),
                                'isReseller': sale.get('isReseller'),
                                
                                # Status
                                'status': sale.get('status'),
                                'statusLabel': sale.get('statusLabel'),

                                # Store and user
                                'store_name': store.get('name'),
                                'createdBy': createdBy.get('name'),

                                # Evaluations
                                'employees': employees,

                                # Bill
                                'chargableTotal': bill.get('chargableTotal'),
                                'bill_items': items_descriptions,
                                'procedure_groupLabels': procedure_group_labels,

                                # Customer
                                'customer_name': customer.get('name'),
                                'customer_email': customer.get('email'),
                                'taxvat': customer.get('taxvat'),
                                'taxvatFormatted': customer.get('taxvatFormatted'),
                                'birthdate': customer.get('birthdate'),
                                'telephones': telephones,
                                'source': (customer.get('source') or {}).get('title'),
                                'occupation': (customer.get('occupation') or {}).get('title'),
                            }
                            page_transformed.append(transformed_sale)
                        
                        all_gross_sales.extend(page_transformed)
                        logger.info(f"Successfully fetched page {page}/{last_page}, got {len(page_sales)} sales")
                    
                    # Add a delay between requests to avoid rate limiting
                    await asyncio.sleep(2)
        else:
            logger.error(f"Unexpected API response structure: {data}")
    
    except Exception as e:
        logger.error(f"Error processing gross sales data: {str(e)}")
    
    logger.info(f"Total gross sales fetched: {len(all_gross_sales)}")
    return all_gross_sales

async def fetch_and_process_grossSales_report(start_date: str, end_date: str) -> List[Dict]:
    """
    Creates a session and fetches gross sales report data.
    
    Args:
        start_date: Start date in ISO format (YYYY-MM-DD)
        end_date: End date in ISO format (YYYY-MM-DD)
        
    Returns:
        List of processed gross sales report dictionaries ready for database insertion
    """
    import aiohttp
    
    async with aiohttp.ClientSession() as session:
        sales = await fetch_grossSalesReport(session, start_date, end_date)
    
    return sales

# For testing purposes
if __name__ == "__main__":
    async def test_fetch():
        start_date = "2023-01-01"
        end_date = "2023-01-31"
        results = await fetch_and_process_grossSales_report(start_date, end_date)
        print(f"Fetched {len(results)} gross sales records")
        if results:
            # Print first result as sample
            import json
            print(json.dumps(results[0], indent=2))
    
    asyncio.run(test_fetch())
