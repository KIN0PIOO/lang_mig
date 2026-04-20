import sys
import os
from dotenv import load_dotenv

env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(env_path)
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.db import get_connection

def activate():
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE NEXT_MIG_INFO SET USE_YN='Y', TARGET_YN='Y' WHERE MAP_ID=1")
            conn.commit()
            print("Job 1 activated.")
    except Exception as e:
        print(f"Failed to activate job: {e}")

if __name__ == "__main__":
    activate()
