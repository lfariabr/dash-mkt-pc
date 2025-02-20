from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import List, Optional
from datetime import datetime, date

from ..models.appointment import Appointment
from ..schemas.appointment import AppointmentCreate, AppointmentUpdate

def get_appointment(db: Session, appointment_id: int) -> Optional[Appointment]:
    return db.query(Appointment).filter(Appointment.id == appointment_id).first()

def get_appointments(
    db: Session,
    skip: int = 0,
    limit: int = 100,
    lead_id: Optional[int] = None,
    unit: Optional[str] = None,
    status: Optional[str] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None
) -> List[Appointment]:
    query = db.query(Appointment)
    
    if lead_id:
        query = query.filter(Appointment.lead_id == lead_id)
    if unit:
        query = query.filter(Appointment.unit == unit)
    if status:
        query = query.filter(Appointment.status == status)
    if date_from:
        query = query.filter(Appointment.date >= date_from)
    if date_to:
        query = query.filter(Appointment.date <= date_to)
        
    return query.offset(skip).limit(limit).all()

def create_appointment(db: Session, appointment: AppointmentCreate) -> Appointment:
    db_appointment = Appointment(**appointment.dict())
    db.add(db_appointment)
    db.commit()
    db.refresh(db_appointment)
    return db_appointment

def update_appointment(
    db: Session, 
    appointment_id: int, 
    appointment: AppointmentUpdate
) -> Optional[Appointment]:
    db_appointment = get_appointment(db, appointment_id)
    if not db_appointment:
        return None
        
    for key, value in appointment.dict(exclude_unset=True).items():
        setattr(db_appointment, key, value)
    
    db.commit()
    db.refresh(db_appointment)
    return db_appointment

def delete_appointment(db: Session, appointment_id: int) -> bool:
    db_appointment = get_appointment(db, appointment_id)
    if not db_appointment:
        return False
        
    db.delete(db_appointment)
    db.commit()
    return True

# Additional utility functions

def get_appointments_count(
    db: Session,
    unit: Optional[str] = None,
    status: Optional[str] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None
) -> int:
    query = db.query(func.count(Appointment.id))
    
    if unit:
        query = query.filter(Appointment.unit == unit)
    if status:
        query = query.filter(Appointment.status == status)
    if date_from:
        query = query.filter(Appointment.date >= date_from)
    if date_to:
        query = query.filter(Appointment.date <= date_to)
        
    return query.scalar()

def get_appointments_by_unit(db: Session) -> List[dict]:
    return db.query(
        Appointment.unit,
        func.count(Appointment.id).label('count')
    ).group_by(Appointment.unit).all()

def get_appointments_by_status(db: Session) -> List[dict]:
    return db.query(
        Appointment.status,
        func.count(Appointment.id).label('count')
    ).group_by(Appointment.status).all()