from app.core.db import get_connection

def verify_separation():
    query = "SELECT MAP_ID, DDL_SQL, MIG_SQL FROM NEXT_MIG_INFO WHERE STATUS = 'PASS'"
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        for map_id, ddl, mig in cursor.fetchall():
            print(f"\n--- MAP_ID: {map_id} ---")
            print(f"DDL_SQL contains CREATE: {'CREATE' in str(ddl).upper()}")
            print(f"MIG_SQL contains CREATE: {'CREATE' in str(mig).upper()}")
            print(f"MIG_SQL contains INSERT: {'INSERT' in str(mig).upper()}")
            # print(f"MIG_SQL Content: {str(mig)[:100]}...")

if __name__ == "__main__":
    verify_separation()
