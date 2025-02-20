from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Numeric, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from ..database import Base

class Appointment(Base):
    __tablename__ = "appointments"

    id = Column(Integer, primary_key=True, index=True)
    lead_id = Column(Integer, ForeignKey("leads.id"), nullable=False)
    
    # Appointment details
    date = Column(DateTime(timezone=True), nullable=False)
    status = Column(String(50), nullable=False)  # scheduled, completed, cancelled, etc.
    unit = Column(String(50), nullable=False)
    procedure = Column(String(100), nullable=True)
    
    # Financial information
    value = Column(Numeric(10, 2), nullable=True)
    payment_method = Column(String(50), nullable=True)
    
    # Additional information
    notes = Column(Text, nullable=True)
    attendant = Column(String(100), nullable=True)
    
    # Metadata
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Relationships
    lead = relationship("Lead", back_populates="appointments")
    sales = relationship("Sale", back_populates="appointment")

    def __repr__(self):
        return f"<Appointment {self.id} for Lead {self.lead_id}>"