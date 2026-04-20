import sys
import os
from dotenv import load_dotenv

env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
load_dotenv(env_path)
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.db import get_connection
from app.agent.scheduler import poll_database

def test_multi_job_flow():
    print("Testing multi-job flow (Job 1 fails, Job 2 follows)...")
    
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            # Job 1: Fail (Invalid Table)
            cursor.execute("UPDATE NEXT_MIG_INFO SET FR_TABLE='INVALID_TBL', STATUS='PENDING', USE_YN='Y' WHERE MAP_ID=1")
            # Job 2: Success (or attempt)
            cursor.execute("UPDATE NEXT_MIG_INFO SET FR_TABLE='DEPARTMENTS', STATUS='PENDING', USE_YN='Y' WHERE MAP_ID=201")
            conn.commit()
            print("Setup complete. Running poll...")
            
        poll_database()
        
        print("\nChecking results...")
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT MAP_ID, STATUS, USE_YN FROM NEXT_MIG_INFO WHERE MAP_ID IN (1, 201)")
            for row in cursor.fetchall():
                print(f"Map ID: {row[0]}, Status: {row[1]}, USE_YN: {row[2]}")
                
    except Exception as e:
        print(f"Test Error: {e}")

if __name__ == "__main__":
    test_multi_job_flow()
