from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from ceab.models import Base

# Path to the SQLite database
DB_PATH = "sqlite:///ceab_data.db"

# Create the SQLAlchemy engine
engine = create_engine(DB_PATH, echo=False)

# Session factory bound to the engine
SessionLocal = sessionmaker(bind=engine)

def init_db():
    """Creates all tables defined in models.py."""
    Base.metadata.create_all(bind=engine)

def get_session():
    """Returns a new session for interacting with the database.

    Returns
    -------
    Session
        A new SQLAlchemy session object.
    """
    return SessionLocal()