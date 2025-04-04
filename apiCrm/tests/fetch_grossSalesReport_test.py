#!/usr/bin/env python
import asyncio
import logging
import json
from datetime import datetime, timedelta
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Import the function we want to test
from apiCrm.resolvers.fetch_grossSalesReport import fetch_and_process_grossSales_report

async def main():
    # Calculate date range for last 7 days
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
    
    print(f"Fetching sales reports from {start_date} to {end_date}...")
    
    # Call the function we want to test
    sales = await fetch_and_process_grossSales_report(start_date, end_date)
    
    # Print summary of results
    print(f"Successfully fetched {len(sales)} sales")
    
    # Print first lead as sample (if available)
    if sales and len(sales) > 0:
        print("\nSample sales data:")
        print(json.dumps(sales[0], indent=2, default=str))

if __name__ == "__main__":
    # Run the async function
    asyncio.run(main())