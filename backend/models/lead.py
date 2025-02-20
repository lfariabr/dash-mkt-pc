from sqlalchemy import Column, Integer, String, DateTime, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from ..database import Base

class Lead(Base):
    __tablename__ = "leads"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=True)
    email = Column(String(100), nullable=True)
    phone = Column(String(20), nullable=True)
    message = Column(Text, nullable=True)
    region = Column(String(50), nullable=True)
    unit = Column(String(50), nullable=True)
    source = Column(String(100), nullable=True)
    entry_day = Column(Integer, nullable=True)
    status = Column(String(50), nullable=True)
    procedure_group = Column(String(100), nullable=True)
    appointment_count = Column(Integer, default=0)
    gclid = Column(String(100), nullable=True)
    
    # UTM and tracking fields
    utm_source = Column(String(100), nullable=True)
    utm_medium = Column(String(100), nullable=True)
    utm_term = Column(String(100), nullable=True)
    utm_content = Column(String(100), nullable=True)
    utm_campaign = Column(String(100), nullable=True)
    
    # Additional tracking information
    referral_site = Column(String(200), nullable=True)
    search_term = Column(String(200), nullable=True)
    device = Column(String(50), nullable=True)
    page_name = Column(String(200), nullable=True)
    
    # Attendance information
    last_attendant = Column(String(100), nullable=True)
    last_appointment = Column(DateTime(timezone=True), nullable=True)
    
    # Metadata
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Relationships
    appointments = relationship("Appointment", back_populates="lead", cascade="all, delete-orphan")
    sales = relationship("Sale", back_populates="lead", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Lead {self.name} ({self.email})>"