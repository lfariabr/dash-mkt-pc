# https://crm.eprocorpo.com.br/reports/leads

from sqlalchemy import Column, Integer, String, DateTime, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from ..database import Base


class LeadsReport(Base):
    __tablename__ = "leads_report"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=True)
    email = Column(String(100), nullable=True)
    telephone = Column(String(20), nullable=True)
    message = Column(Text, nullable=True)
    store = Column(String(100), nullable=True)
    source = Column(String(100), nullable=True)
    createdAt = Column(DateTime(timezone=True), nullable=True)
    status = Column(String(50), nullable=True)
    utmSource = Column(String(100), nullable=True)
    utmMedium = Column(String(100), nullable=True)
    utmTerm = Column(String(100), nullable=True)
    utmContent = Column(String(100), nullable=True)
    utmCampaign = Column(String(100), nullable=True)
    searchTerm = Column(String(100), nullable=True)

    class Meta:
        indexes = [
            ("id", "store", "source"),
        ]