import sys
import os
from dotenv import load_dotenv

env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
load_dotenv(env_path)
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.db import get_connection

def restore_job1():
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE NEXT_MIG_INFO SET FR_TABLE='EMPLOYEES', STATUS='PENDING', USE_YN='Y' WHERE MAP_ID=1")
            conn.commit()
            print("Successfully restored Job 1 (FR_TABLE='EMPLOYEES').")
    except Exception as e:
        print(f"Error restoring job: {e}")

if __name__ == "__main__":
    restore_job1()
