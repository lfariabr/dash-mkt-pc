from pydantic import BaseModel, EmailStr, constr
from typing import Optional
from datetime import datetime

# Base Lead schema with shared attributes
class LeadBase(BaseModel):
    name: Optional[str] = None
    email: Optional[EmailStr] = None
    phone: Optional[constr(max_length=20)] = None
    message: Optional[str] = None
    region: Optional[str] = None
    unit: Optional[str] = None
    source: Optional[str] = None
    entry_day: Optional[int] = None
    status: Optional[str] = None
    procedure_group: Optional[str] = None
    gclid: Optional[str] = None
    
    # UTM fields
    utm_source: Optional[str] = None
    utm_medium: Optional[str] = None
    utm_term: Optional[str] = None
    utm_content: Optional[str] = None
    utm_campaign: Optional[str] = None
    
    # Tracking fields
    referral_site: Optional[str] = None
    search_term: Optional[str] = None
    device: Optional[str] = None
    page_name: Optional[str] = None
    last_attendant: Optional[str] = None

# Schema for creating a Lead
class LeadCreate(LeadBase):
    pass

# Schema for updating a Lead
class LeadUpdate(LeadBase):
    pass

# Schema for Lead in response
class Lead(LeadBase):
    id: int
    appointment_count: int
    last_appointment: Optional[datetime]
    created_at: datetime
    updated_at: Optional[datetime]

    class Config:
        orm_mode = True
        
# Schema for Lead list response
class LeadList(BaseModel):
    total: int
    leads: list[Lead]
    
    class Config:
        orm_mode = True