"""
Test all database connections
Run: python scripts/test_connections.py
"""

import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

def test_postgres_connection(name, host, port, user, password, db):
    """Test PostgreSQL connection"""
    try:
        url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
        engine = create_engine(url)
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            version = result.fetchone()[0]
            print(f"✅ {name}: Connected")
            print(f"   Version: {version[:50]}...")
            return True
    except Exception as e:
        print(f"❌ {name}: Failed")
        print(f"   Error: {e}")
        return False

def main():
    print("=" * 60)
    print("DATABASE CONNECTION TEST")
    print("=" * 60)
    
    tests = [
    ("Source DB", "localhost", 5434, "school_user", "school_pass", "school_source"),
    ("Warehouse DB", "localhost", 5435, "warehouse_user", "warehouse_pass", "school_warehouse"),
    ("Airflow DB", "localhost", 5433, "airflow", "airflow", "airflow"),
]
    
    results = []
    for test in tests:
        result = test_postgres_connection(*test)
        results.append(result)
        print()
    
    print("=" * 60)
    if all(results):
        print("✅ ALL CONNECTIONS SUCCESSFUL!")
    else:
        print("❌ SOME CONNECTIONS FAILED")

if __name__ == "__main__":
    main()