from pydantic import BaseModel, EmailStr, constr
from typing import Optional
from datetime import datetime

# Base MktLead schema with shared attributes
class MktLeadBase(BaseModel):
    
    # lead data
    lead_id: Optional[str] = None
    lead_email: Optional[EmailStr] = None
    lead_phone: Optional[constr(max_length=20)] = None
    lead_message: Optional[str] = None
    lead_store: Optional[str] = None
    lead_source: Optional[str] = None
    lead_entry_day: Optional[int] = None
    lead_mkt_source: Optional[str] = None
    lead_mkt_medium: Optional[str] = None
    lead_mkt_term: Optional[str] = None
    lead_mkt_content: Optional[str] = None
    lead_mkt_campaign: Optional[str] = None
    lead_month: Optional[str] = None
    lead_category: Optional[str] = None

    # appointment data
    appointment_date: Optional[datetime] = None
    appointment_procedure: Optional[str] = None
    appointment_status: Optional[str] = None
    appointment_store: Optional[str] = None

    # sales data
    sale_cleaned_phone: Optional[constr(max_length=20)] = None
    sales_phone: Optional[constr(max_length=20)] = None
    sales_quote_id: Optional[str] = None
    sales_date: Optional[datetime] = None
    sales_store: Optional[str] = None
    sales_first_quote: Optional[str] = None
    sales_total_bought: Optional[str] = None
    sales_number_of_quotes: Optional[str] = None
    sales_day: Optional[int] = None
    sales_month: Optional[str] = None
    sales_day_of_week: Optional[str] = None
    sales_purchased: Optional[bool] = None
    sales_interval: Optional[int] = None

# Additional fields for MktLead
class MktLeadCreate(MktLeadBase):
    pass

class MktLeadUpdate(MktLeadBase):
    pass

# Schema for MktLead in response
class MktLead(MktLeadBase):
    lead_id: int
    appointment_count: int
    last_appointment: Optional[datetime]
    created_at: datetime
    updated_at: Optional[datetime]

    class Config:
        from_attributes = True
    
# Schema for MktLead list response

class MktLeadList(BaseModel):
    total: int
    mkt_leads: list[MktLead]

    class Config:
        from_attributes = True
