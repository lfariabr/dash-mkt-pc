from pydantic import BaseModel, condecimal
from typing import Optional
from datetime import datetime

# Base Sale schema with shared attributes
class SaleBase(BaseModel):
    lead_id: int
    appointment_id: Optional[int] = None
    date: datetime
    procedure: str
    unit: str
    total_value: condecimal(max_digits=10, decimal_places=2)
    discount: Optional[condecimal(max_digits=10, decimal_places=2)] = None
    final_value: condecimal(max_digits=10, decimal_places=2)
    payment_method: str
    installments: Optional[int] = 1
    is_confirmed: Optional[bool] = False
    status: str
    seller: str
    commission: Optional[condecimal(max_digits=10, decimal_places=2)] = None
    notes: Optional[str] = None

# Schema for creating a Sale
class SaleCreate(SaleBase):
    pass

# Schema for updating a Sale
class SaleUpdate(SaleBase):
    lead_id: Optional[int] = None
    date: Optional[datetime] = None
    procedure: Optional[str] = None
    unit: Optional[str] = None
    total_value: Optional[condecimal(max_digits=10, decimal_places=2)] = None
    final_value: Optional[condecimal(max_digits=10, decimal_places=2)] = None
    payment_method: Optional[str] = None
    seller: Optional[str] = None

# Schema for Sale in response
class Sale(SaleBase):
    id: int
    created_at: datetime
    updated_at: Optional[datetime]

    class Config:
        orm_mode = True

# Schema for Sale list response
class SaleList(BaseModel):
    total: int
    sales: list[Sale]

    class Config:
        orm_mode = True