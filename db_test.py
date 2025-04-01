#!/usr/bin/env python
"""
Quick test script to check MktLead database records
"""

import logging
import sys
from backend.database import SessionLocal, engine
from backend.models.mkt_lead import MktLead
from sqlalchemy import inspect

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Check database connection and MktLead records"""
    logger.info("Testing database connection...")
    
    try:
        # Create a session
        session = SessionLocal()
        
        # Get table information
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        logger.info(f"Database tables: {tables}")
        
        if 'mkt_leads' in tables:
            # Get column information
            columns = inspector.get_columns('mkt_leads')
            logger.info(f"MktLead columns: {[col['name'] for col in columns]}")
            
            # Count records
            count = session.query(MktLead).count()
            logger.info(f"Total MktLead records: {count}")
            
            # Sample records
            if count > 0:
                sample = session.query(MktLead).limit(3).all()
                for i, lead in enumerate(sample):
                    logger.info(f"Sample record {i+1}:")
                    for col in columns:
                        col_name = col['name']
                        value = getattr(lead, col_name, None)
                        logger.info(f"  {col_name}: {value}")
            
            # Test query with field names used in view
            test_fields = [
                'lead_source', 'lead_store', 'lead_entry_day', 
                'lead_category', 'sales_purchased'
            ]
            
            for field in test_fields:
                if any(col['name'] == field for col in columns):
                    logger.info(f"Field '{field}' exists in table")
                else:
                    logger.info(f"Field '{field}' DOES NOT exist in table")
                    
        else:
            logger.error("MktLead table not found in database")
            
    except Exception as e:
        logger.error(f"Database error: {str(e)}")
    finally:
        session.close()
        
if __name__ == "__main__":
    main()
