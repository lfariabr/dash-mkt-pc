# https://crm.eprocorpo.com.br/reports/pending-quotes

from sqlalchemy import Column, Integer, String, DateTime, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from ..database import Base


class PendingQuotesReport(Base):
    __tablename__ = "pending_quotes_report"

    id = Column(Integer, primary_key=True, index=True)

    # Timestamps & status
    createdAt = Column(DateTime, nullable=True)
    expirationDate = Column(DateTime, nullable=True)
    statusLabel = Column(String(100), nullable=True)
    isReseller = Column(Boolean, nullable=True)

    # Financial fields
    subtotal = Column(Float, nullable=True)
    discountAmount = Column(Float, nullable=True)
    total = Column(Float, nullable=True)

    # Comments
    comments = Column(String(1000), nullable=True)

    # Store
    store_name = Column(String(100), nullable=True)

    # Created by
    createdBy = Column(String(100), nullable=True)

    # Customer info
    customer_name = Column(String(100), nullable=True)
    customer_email = Column(String(100), nullable=True)
    customer_primaryTelephone = Column(String(50), nullable=True)
    taxvatFormatted = Column(String(50), nullable=True)
    addressLine = Column(String(255), nullable=True)

    # Customer address (nested fields)
    address_street = Column(String(100), nullable=True)
    address_number = Column(String(20), nullable=True)
    address_additional = Column(String(100), nullable=True)
    address_neighborhood = Column(String(100), nullable=True)
    address_city = Column(String(100), nullable=True)
    address_state = Column(String(50), nullable=True)
    address_postcode = Column(String(20), nullable=True)

    # Procedures (concatenated string for simplicity)
    procedure_names = Column(String(500), nullable=True)
    procedure_groupLabels = Column(String(500), nullable=True)

    class Meta:
        indexes = [
            ("id", "statusLabel", "store_name"),
        ]