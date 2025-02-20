from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Numeric, Text, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from ..database import Base

class Sale(Base):
    __tablename__ = "sales"

    id = Column(Integer, primary_key=True, index=True)
    lead_id = Column(Integer, ForeignKey("leads.id"), nullable=False)
    appointment_id = Column(Integer, ForeignKey("appointments.id"), nullable=True)
    
    # Sale details
    date = Column(DateTime(timezone=True), nullable=False)
    procedure = Column(String(100), nullable=False)
    unit = Column(String(50), nullable=False)
    
    # Financial information
    total_value = Column(Numeric(10, 2), nullable=False)
    discount = Column(Numeric(10, 2), default=0)
    final_value = Column(Numeric(10, 2), nullable=False)
    payment_method = Column(String(50), nullable=False)
    installments = Column(Integer, default=1)
    
    # Status tracking
    is_confirmed = Column(Boolean, default=False)
    status = Column(String(50), nullable=False)  # pending, completed, cancelled, etc.
    
    # Sales agent information
    seller = Column(String(100), nullable=False)
    commission = Column(Numeric(10, 2), nullable=True)
    
    # Additional information
    notes = Column(Text, nullable=True)
    
    # Metadata
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # Relationships
    lead = relationship("Lead", back_populates="sales")
    appointment = relationship("Appointment", back_populates="sales")

    def __repr__(self):
        return f"<Sale {self.id} - {self.procedure} - R${self.final_value}>"