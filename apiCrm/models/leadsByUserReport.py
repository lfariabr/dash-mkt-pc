# https://crm.eprocorpo.com.br/reports/leads-by-user

from sqlalchemy import Column, Integer, String, DateTime, Text, JSON
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from ..database import Base

class LeadsByUserReport(Base):
    __tablename__ = "leads_by_user_report"
    
    id = Column(Integer, primary_key=True, index=True)
    
    # User info
    name = Column(String(255), nullable=True)
    shift_number = Column(String(50), nullable=True)
    
    # Message statistics
    messages_count = Column(Integer, default=0)
    unique_messages_count = Column(Integer, default=0)
    success_rate = Column(String(20), nullable=True)  # Stored as formatted string (e.g., "50,00%")
    
    # Status breakdown (JSON object with counts by status code)
    messages_count_by_status = Column(JSON, nullable=True)
    
    # Individual status counts for easier querying
    agd_count = Column(Integer, default=0)  # Agendado (Scheduled)
    bas_count = Column(Integer, default=0)  # Baixado (Downloaded)
    cur_count = Column(Integer, default=0)  # Curando (Healing)
    des_count = Column(Integer, default=0)  # Descartado (Discarded)
    err_count = Column(Integer, default=0)  # Erro (Error)
    jag_count = Column(Integer, default=0)  # Já Agendado (Already Scheduled)
    ncc_count = Column(Integer, default=0)  # Não Consegui Contato (Could Not Contact)
    pro_count = Column(Integer, default=0)  # Processando (Processing)
    rec_count = Column(Integer, default=0)  # Recusou (Refused)
    rej_count = Column(Integer, default=0)  # Rejeitado (Rejected)
    ret_count = Column(Integer, default=0)  # Retorno (Return)
    
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