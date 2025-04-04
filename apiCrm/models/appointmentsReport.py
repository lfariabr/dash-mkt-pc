# https://crm.eprocorpo.com.br/reports/appointments

from sqlalchemy import Column, Integer, String, DateTime, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from ..database import Base

class AppointmentsReport(Base):
    __tablename__ = "appointments_report"

    id = Column(Integer, primary_key=True, index=True)
    startDate = Column(DateTime, nullable=True)
    endDate = Column(DateTime, nullable=True)
    updatedAt = Column(DateTime, nullable=True)
    
    beforePhotoUrl = Column(String(255), nullable=True)
    batchPhotoUrl = Column(String(255), nullable=True)
    afterPhotoUrl = Column(String(255), nullable=True)

    # Status
    status_code = Column(String(50), nullable=True)
    status_label = Column(String(100), nullable=True)

    # Customer info
    client_id = Column(String(100), nullable=True)
    name = Column(String(100), nullable=True)
    email = Column(String(100), nullable=True)
    telephones = Column(String(255), nullable=True)
    addressLine = Column(String(255), nullable=True)
    taxvatFormatted = Column(String(100), nullable=True)
    source = Column(String(100), nullable=True)

    # Store and procedure
    store = Column(String(100), nullable=True)
    procedure = Column(String(100), nullable=True)
    procedure_groupLabel = Column(String(100), nullable=True)

    # Employee
    employee = Column(String(100), nullable=True)

    # Updated by
    updatedBy_name = Column(String(100), nullable=True)

    # Comments
    comments = Column(String(500), nullable=True)

    # Latest progress comment
    latest_comment = Column(String(500), nullable=True)
    latest_comment_createdAt = Column(DateTime, nullable=True)
    latest_comment_user = Column(String(100), nullable=True)

    # Oldest parent info
    oldestParent_createdAt = Column(DateTime, nullable=True)
    oldestParent_createdBy_name = Column(String(100), nullable=True)
    oldestParent_createdBy_group = Column(String(100), nullable=True)

    class Meta:
        indexes = [
            ("id", "client_id", "procedure"),
        ]