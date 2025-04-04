# https://crm.eprocorpo.com.br/reports/gross-sales

from sqlalchemy import Column, Integer, String, DateTime, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from ..database import Base


class GrossSalesReport(Base):
    __tablename__ = "gross_sales_report"

    id = Column(Integer, primary_key=True, index=True)

    # Status and flags
    status = Column(String(50), nullable=True)
    statusLabel = Column(String(100), nullable=True)
    isFree = Column(Boolean, nullable=True)
    isReseller = Column(Boolean, nullable=True)

    # Timestamps
    createdAt = Column(DateTime, nullable=True)
    customerSignedAt = Column(DateTime, nullable=True)

    # Store and user
    store_name = Column(String(100), nullable=True)
    createdBy = Column(String(100), nullable=True)

    # Evaluations
    employees = Column(String(255), nullable=True)  # Lista de nomes de funcionários

    # Bill
    chargableTotal = Column(Float, nullable=True)
    bill_items = Column(String(1000), nullable=True)  # Descrição formatada dos itens
    procedure_groupLabels = Column(String(500), nullable=True)  # Labels dos grupos de procedimentos

    # Customer
    customer_name = Column(String(100), nullable=True)
    customer_email = Column(String(100), nullable=True)
    taxvat = Column(String(50), nullable=True)
    taxvatFormatted = Column(String(50), nullable=True)
    birthdate = Column(DateTime, nullable=True)
    occupation = Column(String(100), nullable=True)
    telephones = Column(String(255), nullable=True)
    source = Column(String(100), nullable=True)

    class Meta:
        indexes = [
            ("id", "status", "store_name"),
        ]