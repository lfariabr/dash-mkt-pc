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

## Core Components

### Frontend
- Interactive dashboards
- Data visualization
- Real-time updates
- Responsive design

### Backend
- FastAPI application
- PostgreSQL database
- AWS RDS integration
- RESTful API endpoints

### Analytics
- Lead metrics
- Appointment tracking
- Sales performance metrics
- Marketing campaign analysis
- Cross-functional insights

## DONE
- Leads Dashboard
- Appointments Management
- Sales Analytics
- Marketing Overview
- Lead-Appointment Cross Analysis
- FastAPI Backend Integration:
  - Complete CRUD operations
  - PostgreSQL database connection
  - AWS RDS deployment
  - Swagger documentation (/docs)
- Database Models:
  - Lead management
  - Appointment tracking
  - Sales recording

## DEV BACKLOG
- [ ] Implement user authentication
- [ ] Add role-based access control
- [ ] Enhance error logging
- [ ] Implement caching layer
- [ ] Add automated testing
- [ ] Set up CI/CD pipeline
- [ ] Integrate marketing expenditure data

### Phase 2: Advanced Analytics
- [X] Implement Fast API Backend
- [X] Connect with AWS RDS postgresDB
- [X] Deploy Marketing Analytics dashboard with database integration
- [X] Implement advanced filtering and segmentation
- [X] Add custom reporting capabilities
- [ ] Create automated data quality checks

### Phase 3: AI/ML Integration
- [ ] Implement lead scoring model
- [ ] Add predictive analytics
- [ ] Create recommendation engine
- [ ] Deploy automated insights

## Getting Started

### Prerequisites
- Python 3.10+
- PostgreSQL
- pip (Python package manager)

### Installation
1. Clone the repository
   ```bash
   git clone <repository-url>
   ```

2. Install dependencies
   ```bash
   pip install -r requirements.txt
   ```

3. Set up environment variables
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

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
- Database Management: PostgreSQL on AWS RDS

## License
This project is proprietary software owned by Luis Faria. All contributions are welcome.