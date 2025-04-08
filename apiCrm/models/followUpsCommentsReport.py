# https://crm.eprocorpo.com.br/reports/follow-ups-comments

from sqlalchemy import Column, Integer, String, DateTime, Text, JSON
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from ..database import Base

class FollowUpsCommentsReport(Base):
    __tablename__ = "follow_ups_comments_report"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # User info
    name = Column(String(255), nullable=True)
    comments_count = Column(Integer, default=0)
    comments_customer_ids = Column(JSON, nullable=True)  # Stored as JSON array
    
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