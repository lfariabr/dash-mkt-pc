from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from ..database import Base

class MktLead(Base):
    __tablename__ = "mkt_leads"
    
    lead_id = Column(Integer, nullable=False, primary_key=True)
    lead_email = Column(String(100), nullable=True)
    lead_phone = Column(String(20), nullable=True)
    lead_message = Column(Text, nullable=True)
    lead_store = Column(String(50), nullable=True)
    lead_source = Column(String(100), nullable=True)
    lead_entry_day = Column(Integer, nullable=True)
    lead_mkt_source = Column(String(100), nullable=True)
    lead_mkt_medium = Column(String(100), nullable=True)
    lead_mkt_term = Column(String(100), nullable=True)
    lead_mkt_content = Column(String(100), nullable=True)
    lead_mkt_campaign = Column(String(100), nullable=True)
    lead_month = Column(String(100), nullable=True)
    lead_category = Column(String(100), nullable=True)

    # appointment data
    appointment_date = Column(DateTime, nullable=True)
    appointment_procedure = Column(String(100), nullable=True)
    appointment_status = Column(String(100), nullable=True)
    appointment_store = Column(String(50), nullable=True)
    
    # sales data
    sale_cleaned_phone = Column(String(20), nullable=True)
    sales_phone = Column(String(20), nullable=True)
    sales_quote_id = Column(String(20), nullable=True)
    sales_date = Column(DateTime, nullable=True)
    sales_store = Column(String(50), nullable=True)
    sales_first_quote = Column(String(20), nullable=True)
    sales_total_bought = Column(String(20), nullable=True)
    sales_number_of_quotes = Column(String(20), nullable=True)
    sales_day = Column(Integer, nullable=True)
    sales_month = Column(String(100), nullable=True)
    sales_day_of_week = Column(String(100), nullable=True)
    sales_purchased = Column(Boolean, nullable=True)
    sales_interval = Column(Integer, nullable=True)