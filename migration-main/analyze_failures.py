import sys
import os
from dotenv import load_dotenv

env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
load_dotenv(env_path)
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.db import get_connection

def analyze():
    print("=== Failure Analysis Report ===\n")
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            
            # 1. Summary of statuses
            cursor.execute("SELECT STATUS, COUNT(*) FROM NEXT_MIG_INFO WHERE USE_YN='N' GROUP BY STATUS")
            print("Status Summary:")
            for status, count in cursor.fetchall():
                print(f" - {status}: {count}")
            
            print("\nRecent Logs for Failed Jobs:")
            query = """
                SELECT * FROM (
                    SELECT L.MAP_ID, L.STEP_NAME, L.STATUS, L.MESSAGE, I.FR_TABLE, I.TO_TABLE
                    FROM NEXT_MIG_LOG L
                    JOIN NEXT_MIG_INFO I ON L.MAP_ID = I.MAP_ID
                    WHERE L.STATUS = 'FAIL' OR L.LOG_LEVEL = 'ERROR'
                    ORDER BY L.LOG_ID DESC
                ) WHERE ROWNUM <= 10
            """
            cursor.execute(query)
            rows = cursor.fetchall()
            if not rows:
                print(" No error logs found.")
            for map_id, step, status, msg, fr, to in rows:
                print(f"\n[Map ID {map_id}] {fr} -> {to}")
                print(f" - Step: {step}")
                print(f" - Status: {status}")
                print(f" - Message: {msg}")

    except Exception as e:
        print(f"Analysis Error: {e}")

if __name__ == "__main__":
    analyze()
