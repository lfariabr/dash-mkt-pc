import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from dotenv import load_dotenv
import os

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Determine if we're in development or production mode
DEV_MODE = os.getenv("DEV_MODE", "True").lower() == "true"

# Create Base class before engine setup
Base = declarative_base()

# Default to SQLite in development mode to avoid connection issues
try:
    if DEV_MODE:
        # Use SQLite for development
        DATABASE_URL = "sqlite:///./database.db"
        engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
        logger.info("Connected to SQLite database")
    else:
        # Use Supabase PostgreSQL for production
        user = os.getenv("DB_USER", "postgres")
        password = os.getenv("DB_PASSWORD", "")
        host = os.getenv("DB_HOST", "")
        port = os.getenv("DB_PORT", "5432")
        dbname = os.getenv("DB_NAME", "postgres")
        
        # Use credentials directly from .env without modification
        # For Supabase, the DB_USER should already be in the format "postgres.project_ref"
        DATABASE_URL = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        
        # Create engine with appropriate settings for Supabase
        engine = create_engine(
            DATABASE_URL,
            connect_args={
                "options": "-c statement_timeout=0 -c idle_in_transaction_session_timeout=0"
            },
            pool_size=10,
            max_overflow=10,
            # Add connection health check
            pool_pre_ping=True
        )
        
        logger.info(f"Connected to PostgreSQL database at {host}")
except Exception as e:
    logger.error(f"Failed to connect to database: {str(e)}")
    raise

# Create SessionLocal class
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Function to create all tables
def create_tables():
    try:
        # Import models here to avoid circular imports
        from .models.lead import Lead
        from .models.appointment import Appointment
        from .models.sale import Sale
        from .models.mkt_lead import MktLead
        
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created successfully")
    except Exception as e:
        logger.error(f"Error creating database tables: {str(e)}")
        if not DEV_MODE:  # Only raise in production mode
            raise