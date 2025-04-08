# https://crm.eprocorpo.com.br/reports/appointments-by-user

from sqlalchemy import Column, Integer, String, DateTime, Text, JSON
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from ..database import Base

class AppointmentsByUserReport(Base):
    __tablename__ = "appointments_by_user_report"

    id = Column(Integer, primary_key=True, index=True)
    
    # User info
    name = Column(String(255), nullable=True)
    shift_number = Column(String(100), nullable=True)
    appointments_count = Column(Integer, default=0)
    
    # Procedure counts by group
    # Stored as JSON: {"surgery": 0, "hair_removal": 0, "aesthetic": 0, "invasive": 0, "tattoo": 0}
    procedure_counts = Column(JSON, nullable=True)
    
    # Individual procedure counts for easier querying
    surgery_count = Column(Integer, default=0)
    hair_removal_count = Column(Integer, default=0)
    aesthetic_count = Column(Integer, default=0)
    invasive_count = Column(Integer, default=0)
    tattoo_count = Column(Integer, default=0)
    
    # Report metadata
    report_start_date = Column(String(10), nullable=True)  # YYYY-MM-DD
    report_end_date = Column(String(10), nullable=True)    # YYYY-MM-DD
    
    # Record metadata
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    class Meta:
        indexes = [
            ("id", "name"),
            ("report_start_date", "report_end_date"),
        ]