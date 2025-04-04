# https://crm.eprocorpo.com.br/reports/sales-by-payment-method

# https://crm.eprocorpo.com.br/reports/pending-quotes

from sqlalchemy import Column, Integer, String, DateTime, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from ..database import Base


class SalesByPaymentMethodReport(Base):
    __tablename__ = "sales_by_payment_method_report"

    id = Column(Integer, primary_key=True, index=True)

    # Payment info
    amount = Column(Float, nullable=True)
    dueAt = Column(DateTime, nullable=True)
    paidAmount = Column(Float, nullable=True)
    isPaid = Column(Boolean, nullable=True)

    # Payment method
    paymentMethod_name = Column(String(100), nullable=True)
    paymentMethod_displayAmountOnReport = Column(Boolean, nullable=True)

    # Quote info (inside bill -> quote)
    quote_id = Column(Integer, nullable=True)
    quote_createdAt = Column(DateTime, nullable=True)
    quote_customerSignedAt = Column(DateTime, nullable=True)
    quote_discountAmount = Column(Float, nullable=True)
    quote_subtotal = Column(Float, nullable=True)
    quote_statusLabel = Column(String(100), nullable=True)
    quote_isReseller = Column(Boolean, nullable=True)
    quote_comments = Column(String(1000), nullable=True)

    # Store and quote creators
    store_name = Column(String(100), nullable=True)
    createdBy = Column(String(100), nullable=True)
    cancelledBy = Column(String(100), nullable=True)

    # Customer info
    customer_name = Column(String(100), nullable=True)
    customer_email = Column(String(100), nullable=True)
    customer_taxvatFormatted = Column(String(50), nullable=True)
    customer_addressLine = Column(String(255), nullable=True)

    # Customer address (nested)
    address_street = Column(String(100), nullable=True)
    address_number = Column(String(20), nullable=True)
    address_additional = Column(String(100), nullable=True)
    address_neighborhood = Column(String(100), nullable=True)
    address_city = Column(String(100), nullable=True)
    address_state = Column(String(50), nullable=True)
    address_postcode = Column(String(20), nullable=True)

    # Bill items (flattened)
    item_descriptions = Column(String(1000), nullable=True)
    procedure_groupLabels = Column(String(500), nullable=True)

    class Meta:
        indexes = [
            ("id", "isPaid", "paymentMethod_name"),
        ]