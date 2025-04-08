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
from apiCrm.resolvers.fetch_leadsByUserReport import fetch_and_process_leadsByUserReport

async def main():
    # Calculate date range for last 7 days
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=70)).strftime('%Y-%m-%d')
    
    print(f"Fetching leads by user report from {start_date} to {end_date}...")
    
    # Call the function we want to test
    leads = await fetch_and_process_leadsByUserReport(start_date, end_date)
    
    # Print summary of results
    print(f"Successfully fetched {len(leads)} leads")
    
    # Print first lead as sample (if available)
    if leads and len(leads) > 0:
        print("\nSample lead data:")
        print(json.dumps(leads[0], indent=2, default=str))

if __name__ == "__main__":
    # Run the async function
    asyncio.run(main())