from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import List, Optional
from datetime import datetime, date
from decimal import Decimal

from ..models.sale import Sale
from ..schemas.sale import SaleCreate, SaleUpdate

def get_sale(db: Session, sale_id: int) -> Optional[Sale]:
    return db.query(Sale).filter(Sale.id == sale_id).first()

def get_sales(
    db: Session,
    skip: int = 0,
    limit: int = 100,
    lead_id: Optional[int] = None,
    unit: Optional[str] = None,
    status: Optional[str] = None,
    seller: Optional[str] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None
) -> List[Sale]:
    query = db.query(Sale)
    
    if lead_id:
        query = query.filter(Sale.lead_id == lead_id)
    if unit:
        query = query.filter(Sale.unit == unit)
    if status:
        query = query.filter(Sale.status == status)
    if seller:
        query = query.filter(Sale.seller == seller)
    if date_from:
        query = query.filter(Sale.date >= date_from)
    if date_to:
        query = query.filter(Sale.date <= date_to)
        
    return query.offset(skip).limit(limit).all()

def create_sale(db: Session, sale: SaleCreate) -> Sale:
    db_sale = Sale(**sale.dict())
    db.add(db_sale)
    db.commit()
    db.refresh(db_sale)
    return db_sale

def update_sale(db: Session, sale_id: int, sale: SaleUpdate) -> Optional[Sale]:
    db_sale = get_sale(db, sale_id)
    if not db_sale:
        return None
        
    for key, value in sale.dict(exclude_unset=True).items():
        setattr(db_sale, key, value)
    
    db.commit()
    db.refresh(db_sale)
    return db_sale

def delete_sale(db: Session, sale_id: int) -> bool:
    db_sale = get_sale(db, sale_id)
    if not db_sale:
        return False
        
    db.delete(db_sale)
    db.commit()
    return True

# Additional utility functions

def get_sales_count(
    db: Session,
    unit: Optional[str] = None,
    status: Optional[str] = None,
    seller: Optional[str] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None
) -> int:
    query = db.query(func.count(Sale.id))
    
    if unit:
        query = query.filter(Sale.unit == unit)
    if status:
        query = query.filter(Sale.status == status)
    if seller:
        query = query.filter(Sale.seller == seller)
    if date_from:
        query = query.filter(Sale.date >= date_from)
    if date_to:
        query = query.filter(Sale.date <= date_to)
        
    return query.scalar()

def get_total_sales_value(
    db: Session,
    unit: Optional[str] = None,
    seller: Optional[str] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None
) -> Decimal:
    query = db.query(func.sum(Sale.final_value))
    
    if unit:
        query = query.filter(Sale.unit == unit)
    if seller:
        query = query.filter(Sale.seller == seller)
    if date_from:
        query = query.filter(Sale.date >= date_from)
    if date_to:
        query = query.filter(Sale.date <= date_to)
        
    return query.scalar() or Decimal('0.0')

def get_sales_by_unit(db: Session) -> List[dict]:
    return db.query(
        Sale.unit,
        func.count(Sale.id).label('count'),
        func.sum(Sale.final_value).label('total_value')
    ).group_by(Sale.unit).all()

def get_sales_by_seller(db: Session) -> List[dict]:
    return db.query(
        Sale.seller,
        func.count(Sale.id).label('count'),
        func.sum(Sale.final_value).label('total_value'),
        func.sum(Sale.commission).label('total_commission')
    ).group_by(Sale.seller).all()