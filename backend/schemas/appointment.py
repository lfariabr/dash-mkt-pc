from pydantic import BaseModel, condecimal, ConfigDict
from typing import Optional
from datetime import datetime

# Base Appointment schema with shared attributes
class AppointmentBase(BaseModel):
    lead_id: int
    date: datetime
    status: str
    unit: str
    procedure: Optional[str] = None
    value: Optional[condecimal(max_digits=10, decimal_places=2)] = None
    payment_method: Optional[str] = None
    notes: Optional[str] = None
    attendant: Optional[str] = None

# Schema for creating an Appointment
class AppointmentCreate(AppointmentBase):
    pass

# Schema for updating an Appointment
class AppointmentUpdate(AppointmentBase):
    lead_id: Optional[int] = None
    date: Optional[datetime] = None
    status: Optional[str] = None
    unit: Optional[str] = None

# Schema for Appointment in response
class Appointment(AppointmentBase):
    id: int
    created_at: datetime
    updated_at: Optional[datetime]
    model_config = ConfigDict(from_attributes=True)

# Schema for Appointment list response
class AppointmentList(BaseModel):
    total: int
    appointments: list[Appointment]
    model_config = ConfigDict(from_attributes=True)