import sys
import os
from dotenv import load_dotenv

env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env")
load_dotenv(env_path)
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.db import get_connection

def check_schema():
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            print("Checking NEXT_MIG_INFO columns...")
            cursor.execute("SELECT COLUMN_NAME FROM USER_TAB_COLUMNS WHERE TABLE_NAME = 'NEXT_MIG_INFO' ORDER BY COLUMN_ID")
            columns = [row[0] for row in cursor.fetchall()]
            for col in columns:
                print(f" - {col}")
            
            if "RETRY_COUNT" in columns:
                print("\n[CONFIRMED] RETRY_COUNT column exists.")
            elif "RETRY_CNT" in columns:
                print("\n[WARNING] RETRY_COUNT NOT FOUND. RETRY_CNT exists instead.")
            else:
                print("\n[ERROR] Neither RETRY_COUNT nor RETRY_CNT found!")
                
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_schema()
