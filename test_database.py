from sqlalchemy import create_engine
from sqlalchemy.sql import text

engine = create_engine("mssql+pyodbc://@KakashiK24/datasets?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes")

try:
    with engine.connect() as conn:
        print("Connection successful!")
        result = conn.execute(text("SELECT 1")).scalar()
        print(f"Query result: {result}")
except Exception as e:
    print(f"Error connecting to the database: {e}")
