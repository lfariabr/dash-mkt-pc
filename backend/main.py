from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import Optional, List
from datetime import date

from .database import engine, get_db, create_tables
from .schemas.lead import Lead as LeadSchema, LeadCreate, LeadUpdate, LeadList
from .schemas.appointment import Appointment as AppointmentSchema, AppointmentCreate, AppointmentUpdate, AppointmentList
from .schemas.sale import Sale as SaleSchema, SaleCreate, SaleUpdate, SaleList
from .crud import lead as lead_crud
from .crud import appointment as appointment_crud
from .crud import sale as sale_crud

# Create FastAPI app
app = FastAPI(
    title="Dash Analytics API",
    description="API for managing leads, appointments, and sales data",
    version="1.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create database tables
create_tables()

# Lead endpoints
@app.post("/leads/", response_model=LeadSchema, tags=["Leads"])
def create_lead(lead: LeadCreate, db: Session = Depends(get_db)):
    """Create a new lead"""
    return lead_crud.create_lead(db=db, lead=lead)

@app.get("/leads/", response_model=LeadList, tags=["Leads"])
def read_leads(
    skip: int = 0,
    limit: int = 100,
    unit: Optional[str] = None,
    status: Optional[str] = None,
    source: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Get all leads with optional filtering"""
    leads = lead_crud.get_leads(db, skip=skip, limit=limit, unit=unit, status=status, source=source)
    total = lead_crud.get_leads_count(db, unit=unit, status=status, source=source)
    return {"total": total, "leads": leads}

@app.get("/leads/{lead_id}", response_model=LeadSchema, tags=["Leads"])
def read_lead(lead_id: int, db: Session = Depends(get_db)):
    """Get a specific lead by ID"""
    db_lead = lead_crud.get_lead(db, lead_id=lead_id)
    if db_lead is None:
        raise HTTPException(status_code=404, detail="Lead not found")
    return db_lead

@app.put("/leads/{lead_id}", response_model=LeadSchema, tags=["Leads"])
def update_lead(lead_id: int, lead: LeadUpdate, db: Session = Depends(get_db)):
    """Update a lead"""
    db_lead = lead_crud.update_lead(db, lead_id=lead_id, lead=lead)
    if db_lead is None:
        raise HTTPException(status_code=404, detail="Lead not found")
    return db_lead

@app.delete("/leads/{lead_id}", tags=["Leads"])
def delete_lead(lead_id: int, db: Session = Depends(get_db)):
    """Delete a lead"""
    success = lead_crud.delete_lead(db, lead_id=lead_id)
    if not success:
        raise HTTPException(status_code=404, detail="Lead not found")
    return {"detail": "Lead deleted successfully"}

# Appointment endpoints
@app.post("/appointments/", response_model=AppointmentSchema, tags=["Appointments"])
def create_appointment(appointment: AppointmentCreate, db: Session = Depends(get_db)):
    """Create a new appointment"""
    return appointment_crud.create_appointment(db=db, appointment=appointment)

@app.get("/appointments/", response_model=AppointmentList, tags=["Appointments"])
def read_appointments(
    skip: int = 0,
    limit: int = 100,
    lead_id: Optional[int] = None,
    unit: Optional[str] = None,
    status: Optional[str] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    db: Session = Depends(get_db)
):
    """Get all appointments with optional filtering"""
    appointments = appointment_crud.get_appointments(
        db, skip=skip, limit=limit, lead_id=lead_id,
        unit=unit, status=status, date_from=date_from, date_to=date_to
    )
    total = appointment_crud.get_appointments_count(
        db, unit=unit, status=status, date_from=date_from, date_to=date_to
    )
    return {"total": total, "appointments": appointments}

@app.get("/appointments/{appointment_id}", response_model=AppointmentSchema, tags=["Appointments"])
def read_appointment(appointment_id: int, db: Session = Depends(get_db)):
    """Get a specific appointment by ID"""
    db_appointment = appointment_crud.get_appointment(db, appointment_id=appointment_id)
    if db_appointment is None:
        raise HTTPException(status_code=404, detail="Appointment not found")
    return db_appointment

@app.put("/appointments/{appointment_id}", response_model=AppointmentSchema, tags=["Appointments"])
def update_appointment(
    appointment_id: int,
    appointment: AppointmentUpdate,
    db: Session = Depends(get_db)
):
    """Update an appointment"""
    db_appointment = appointment_crud.update_appointment(
        db, appointment_id=appointment_id, appointment=appointment
    )
    if db_appointment is None:
        raise HTTPException(status_code=404, detail="Appointment not found")
    return db_appointment

@app.delete("/appointments/{appointment_id}", tags=["Appointments"])
def delete_appointment(appointment_id: int, db: Session = Depends(get_db)):
    """Delete an appointment"""
    success = appointment_crud.delete_appointment(db, appointment_id=appointment_id)
    if not success:
        raise HTTPException(status_code=404, detail="Appointment not found")
    return {"detail": "Appointment deleted successfully"}

# Sale endpoints
@app.post("/sales/", response_model=SaleSchema, tags=["Sales"])
def create_sale(sale: SaleCreate, db: Session = Depends(get_db)):
    """Create a new sale"""
    return sale_crud.create_sale(db=db, sale=sale)

@app.get("/sales/", response_model=SaleList, tags=["Sales"])
def read_sales(
    skip: int = 0,
    limit: int = 100,
    lead_id: Optional[int] = None,
    unit: Optional[str] = None,
    status: Optional[str] = None,
    seller: Optional[str] = None,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
    db: Session = Depends(get_db)
):
    """Get all sales with optional filtering"""
    sales = sale_crud.get_sales(
        db, skip=skip, limit=limit, lead_id=lead_id,
        unit=unit, status=status, seller=seller,
        date_from=date_from, date_to=date_to
    )
    total = sale_crud.get_sales_count(
        db, unit=unit, status=status, seller=seller,
        date_from=date_from, date_to=date_to
    )
    return {"total": total, "sales": sales}

@app.get("/sales/{sale_id}", response_model=SaleSchema, tags=["Sales"])
def read_sale(sale_id: int, db: Session = Depends(get_db)):
    """Get a specific sale by ID"""
    db_sale = sale_crud.get_sale(db, sale_id=sale_id)
    if db_sale is None:
        raise HTTPException(status_code=404, detail="Sale not found")
    return db_sale

@app.put("/sales/{sale_id}", response_model=SaleSchema, tags=["Sales"])
def update_sale(sale_id: int, sale: SaleUpdate, db: Session = Depends(get_db)):
    """Update a sale"""
    db_sale = sale_crud.update_sale(db, sale_id=sale_id, sale=sale)
    if db_sale is None:
        raise HTTPException(status_code=404, detail="Sale not found")
    return db_sale

@app.delete("/sales/{sale_id}", tags=["Sales"])
def delete_sale(sale_id: int, db: Session = Depends(get_db)):
    """Delete a sale"""
    success = sale_crud.delete_sale(db, sale_id=sale_id)
    if not success:
        raise HTTPException(status_code=404, detail="Sale not found")
    return {"detail": "Sale deleted successfully"}

# Analytics endpoints
@app.get("/analytics/leads/by-source", tags=["Analytics"])
def get_leads_by_source(db: Session = Depends(get_db)):
    """Get lead count grouped by source"""
    return lead_crud.get_leads_by_source(db)

@app.get("/analytics/leads/by-status", tags=["Analytics"])
def get_leads_by_status(db: Session = Depends(get_db)):
    """Get lead count grouped by status"""
    return lead_crud.get_leads_by_status(db)

@app.get("/analytics/appointments/by-unit", tags=["Analytics"])
def get_appointments_by_unit(db: Session = Depends(get_db)):
    """Get appointment count grouped by unit"""
    return appointment_crud.get_appointments_by_unit(db)

@app.get("/analytics/sales/by-unit", tags=["Analytics"])
def get_sales_by_unit(db: Session = Depends(get_db)):
    """Get sales statistics grouped by unit"""
    return sale_crud.get_sales_by_unit(db)

@app.get("/analytics/sales/by-seller", tags=["Analytics"])
def get_sales_by_seller(db: Session = Depends(get_db)):
    """Get sales statistics grouped by seller"""
    return sale_crud.get_sales_by_seller(db)