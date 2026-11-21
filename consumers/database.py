
import sqlite3
from pathlib import Path


def get_database():
    # Locate the database in the consumers directory
    database_path = Path(__file__).resolve().parent.parent / "history.db"

    # Connect to the SQLite database
    db = sqlite3.connect(database_path)

    # Create the readings table if it does not exist
    db.execute("""
    CREATE TABLE IF NOT EXISTS readings (
        patient TEXT,
        sign TEXT,
        timestamp TEXT,
        value REAL,
        unit TEXT,
        ward TEXT,
        room TEXT,
        PRIMARY KEY (patient, sign, timestamp)
    )
    """)

    # Create the averages table if it does not exist
    db.execute("""
    CREATE TABLE IF NOT EXISTS averages (
        patient TEXT,
        sign TEXT,
        window_start INTEGER,
        average REAL,
        unit TEXT,
        PRIMARY KEY (patient, sign, window_start)
    )
    """)

    # Return the database connection
    return db
