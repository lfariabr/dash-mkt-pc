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
from apiCrm.resolvers.fetch_salesByPaymentMethodReports import fetch_and_process_salesByPaymentMethod_report

async def main():
    # Calculate date range for last 7 days
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
    
    print(f"Fetching pending quotes from {start_date} to {end_date}...")
    
    # Call the function we want to test
    pending_quotes = await fetch_and_process_salesByPaymentMethod_report(start_date, end_date)
    
    # Print summary of results
    print(f"Successfully fetched {len(pending_quotes)} pending quotes")
    
    # Print first pending quote as sample (if available)
    if pending_quotes and len(pending_quotes) > 0:
        print("\nSample pending quote data:")
        print(json.dumps(pending_quotes[0], indent=2, default=str))

if __name__ == "__main__":
    # Run the async function
    asyncio.run(main())