from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import List, Optional
from datetime import datetime

from ..models.lead import Lead
from ..schemas.lead import LeadCreate, LeadUpdate

def get_lead(db: Session, lead_id: int) -> Optional[Lead]:
    return db.query(Lead).filter(Lead.id == lead_id).first()

def get_leads(
    db: Session, 
    skip: int = 0, 
    limit: int = 100,
    unit: Optional[str] = None,
    status: Optional[str] = None,
    source: Optional[str] = None
) -> List[Lead]:
    query = db.query(Lead)
    
    if unit:
        query = query.filter(Lead.unit == unit)
    if status:
        query = query.filter(Lead.status == status)
    if source:
        query = query.filter(Lead.source == source)
        
    return query.offset(skip).limit(limit).all()

def create_lead(db: Session, lead: LeadCreate) -> Lead:
    db_lead = Lead(**lead.dict())
    db.add(db_lead)
    db.commit()
    db.refresh(db_lead)
    return db_lead

def update_lead(db: Session, lead_id: int, lead: LeadUpdate) -> Optional[Lead]:
    db_lead = get_lead(db, lead_id)
    if not db_lead:
        return None
        
    for key, value in lead.dict(exclude_unset=True).items():
        setattr(db_lead, key, value)
    
    db.commit()
    db.refresh(db_lead)
    return db_lead

def delete_lead(db: Session, lead_id: int) -> bool:
    db_lead = get_lead(db, lead_id)
    if not db_lead:
        return False
        
    db.delete(db_lead)
    db.commit()
    return True

# Additional utility functions

def get_leads_count(
    db: Session,
    unit: Optional[str] = None,
    status: Optional[str] = None,
    source: Optional[str] = None
) -> int:
    query = db.query(func.count(Lead.id))
    
    if unit:
        query = query.filter(Lead.unit == unit)
    if status:
        query = query.filter(Lead.status == status)
    if source:
        query = query.filter(Lead.source == source)
        
    return query.scalar()

def get_leads_by_source(db: Session) -> List[dict]:
    return db.query(
        Lead.source,
        func.count(Lead.id).label('count')
    ).group_by(Lead.source).all()

def get_leads_by_status(db: Session) -> List[dict]:
    return db.query(
        Lead.status,
        func.count(Lead.id).label('count')
    ).group_by(Lead.status).all()