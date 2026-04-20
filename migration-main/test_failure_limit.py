import sys
import os
from dotenv import load_dotenv

env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(env_path)
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.db import get_connection
from app.agent.orchestrator import MigrationOrchestrator
from app.domain.mapping.repository import get_pending_jobs

def test_failure_handling():
    print("Testing failure limit (max 3 attempts)...")
    
    # 1. Force Job 1 to be invalid and PENDING
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("UPDATE NEXT_MIG_INFO SET USE_YN='Y', TARGET_YN='Y', STATUS='PENDING', FR_TABLE='NON_EXISTENT_TABLE' WHERE MAP_ID=1")
        conn.commit()
        print("Job 1 activated with INVALID table name.")

    # 2. Get the job
    jobs = get_pending_jobs()
    job = [j for j in jobs if j.map_id == 1][0]
    
    # 3. Process job
    orchestrator = MigrationOrchestrator()
    orchestrator.process_job(job)
    
    # 4. Check final status in DB
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT STATUS, USE_YN, BATCH_CNT, RETRY_COUNT FROM NEXT_MIG_INFO WHERE MAP_ID=1")
        row = cursor.fetchone()
        print(f"Final DB State - Status: {row[0]}, USE_YN: {row[1]}, BATCH_CNT: {row[2]}, RETRY_COUNT: {row[3]}")
        
        if row[0] == 'FAIL' and row[1] == 'N':
            print("SUCCESS: Job correctly marked as FAIL after retries.")
        else:
            print("FAILURE: Job was not correctly marked as FAIL.")

if __name__ == "__main__":
    test_failure_handling()
