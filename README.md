# Dash Analytics

A comprehensive analytics dashboard for tracking and analyzing leads, appointments, and sales data.

## Features

### Data Management
- Lead tracking and management
- Appointment scheduling and status tracking
- Sales performance analytics
- Marketing campaign effectiveness
- Cross-functional data analysis

### Analytics & Reporting
- Real-time dashboard
- Custom report generation
- Performance metrics
- Marketing ROI analysis
- Lead conversion tracking

### Technical Features
- FastAPI backend with PostgreSQL integration
- RESTful API with comprehensive CRUD operations
- SQLAlchemy ORM for database management
- Pydantic models for data validation
- Interactive API documentation (Swagger UI)
- Integration with GraphQL to fetch data asynchronously

## Core Components

### Frontend
- Streamlit application
- Interactive dashboards
- Data visualization
- Real-time updates
- Responsive design

### Backend
- FastAPI application
- PostgreSQL database
- Supabase integration
- RESTful API endpoints

## Getting Started

### Prerequisites
- Python 3.10+
- PostgreSQL
- pip (Python package manager)

### Running the Application
1. Start the FastAPI backend:
   ```bash
   uvicorn backend.main:app --reload
   ```

2. Start the Streamlit frontend:
   ```bash
   streamlit run app.py
   ```

## Useful Resources
- API Documentation: http://127.0.0.1:8000/docs
- Backend Server: uvicorn backend.main:app --reload
- Frontend Server: streamlit run app.py
- Database Management: PostgreSQL on Supabase

## License
This project is proprietary software owned by Luis Faria. All contributions are welcome.