# https://crm.eprocorpo.com.br/reports/appointments

from sqlalchemy import Column, Integer, String, DateTime, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from ..database import Base

class AppointmentsReport(Base):
    __tablename__ = "appointments_report"
    id = Column(Integer, primary_key=True, index=True)
    client_id = Column(String(100), nullable=True)
    name = Column(String(100), nullable=True)
    telephones = Column(String(100), nullable=True)
    email = Column(String(100), nullable=True)
    store = Column(String(100), nullable=True)
    procedure = Column(String(100), nullable=True)
    employee = Column(String(100), nullable=True)
    startDate = Column(DateTime, nullable=True)
    status = Column(String(50), nullable=True)
    updatedBy_name = Column(String(100), nullable=True)
    updatedBy_createdAt = Column(DateTime, nullable=True)
    updatedBy_group_name = Column(String(100), nullable=True)
    updatedBy_group_createdBy_name = Column(String(100), nullable=True)

    class Meta:
        indexes = [
            ("id", "client_id", "procedure"),
        ]