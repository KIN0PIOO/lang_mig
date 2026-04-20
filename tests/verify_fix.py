import os
import sys

# Add project root to sys.path
sys.path.append(r'c:\Users\11827\Desktop\migration-main\migration-main')

from app.domain.history.repository import log_generated_sql
from app.core.logger import logger

def test_fix():
    print("Testing ORA-01484 fix...")
    map_id = 1 # Assuming map_id 1 exists from setup_hr_cases.py
    list_sql = ["SELECT * FROM DUAL", "SELECT SYSDATE FROM DUAL"]
    list_verify = ["SELECT 1 FROM DUAL"]
    
    try:
        log_generated_sql(map_id, list_sql, list_verify)
        print("Success: log_generated_sql handled list input without ORA-01484!")
    except Exception as e:
        print(f"Failed: log_generated_sql still threw error: {e}")

if __name__ == "__main__":
    test_fix()
