import sys
import os
from dotenv import load_dotenv

env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
load_dotenv(env_path)
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.db import get_connection

def fix_stuck_jobs():
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE NEXT_MIG_INFO SET STATUS = 'FAIL' WHERE STATUS = 'RUNNING'")
            conn.commit()
            print("Successfully updated stuck 'RUNNING' jobs to 'FAIL'.")
    except Exception as e:
        print(f"Error restoring jobs: {e}")

if __name__ == "__main__":
    fix_stuck_jobs()
