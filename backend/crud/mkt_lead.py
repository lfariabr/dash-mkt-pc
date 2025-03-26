from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import List, Optional
from datetime import datetime

from ..models.mkt_lead import MktLead
from ..schemas.mkt_lead import MktLeadCreate, MktLeadUpdate

def get_mkt_lead(db: Session, lead_id: int) -> Optional[MktLead]:
    return db.query(MktLead).filter(MktLead.lead_id == lead_id).first()

def get_mkt_leads(
    db: Session, 
    skip: int = 0, 
    limit: int = 100,
    unit: Optional[str] = None,
    status: Optional[str] = None,
    source: Optional[str] = None
) -> List[MktLead]:
    query = db.query(MktLead)
    
    if unit:
        query = query.filter(MktLead.unit == unit)
    if status:
        query = query.filter(MktLead.status == status)
    if source:
        query = query.filter(MktLead.source == source)
        
    return query.offset(skip).limit(limit).all()

def create_mkt_lead(db: Session, mkt_lead: MktLeadCreate) -> MktLead:
    db_mkt_lead = MktLead(**mkt_lead.dict())
    db.add(db_mkt_lead)
    db.commit()
    db.refresh(db_mkt_lead)
    return db_mkt_lead

def update_mkt_lead(db: Session, lead_id: int, mkt_lead: MktLeadUpdate) -> Optional[MktLead]:
    db_mkt_lead = get_mkt_lead(db, lead_id)
    if not db_mkt_lead:
        return None
        
    for key, value in mkt_lead.dict(exclude_unset=True).items():
        setattr(db_mkt_lead, key, value)
    
    db.commit()
    db.refresh(db_mkt_lead)
    return db_mkt_lead

def delete_mkt_lead(db: Session, lead_id: int) -> bool:
    db_mkt_lead = get_mkt_lead(db, lead_id)
    if not db_mkt_lead:
        return False
        
    db.delete(db_mkt_lead)
    db.commit()
    return True

# Additional utility functions

def get_mkt_leads_count(
    db: Session,
    unit: Optional[str] = None,
    status: Optional[str] = None,
    source: Optional[str] = None
) -> int:
    query = db.query(func.count(MktLead.lead_id))
    
    if unit:
        query = query.filter(MktLead.unit == unit)
    if status:
        query = query.filter(MktLead.status == status)
    if source:
        query = query.filter(MktLead.source == source)
        
    return query.scalar()

def get_mkt_leads_by_source(db: Session) -> List[dict]:
    return db.query(
        MktLead.source,
        func.count(MktLead.lead_id).label('count')
    ).group_by(MktLead.source).all()

def get_mkt_leads_by_status(db: Session) -> List[dict]:
    return db.query(
        MktLead.status,
        func.count(MktLead.lead_id).label('count')
    ).group_by(MktLead.status).all()