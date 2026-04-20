from app.core.db import get_connection

def find_hr_tables():
    possible_tables = ['COUNTRIES', 'DEPARTMENTS', 'EMPLOYEES', 'JOBS', 'JOB_HISTORY', 'LOCATIONS', 'REGIONS']
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # 1. 현재 접속 유저의 테이블 조회
        print("Checking current user's tables...")
        cursor.execute("SELECT table_name FROM user_tables")
        user_tables = [row[0] for row in cursor.fetchall()]
        print(f"User tables: {user_tables}")
        
        # 2. HR 스키마의 테이블 조회
        print("\nChecking tables with 'HR.' prefix...")
        found = []
        for tbl in possible_tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM HR.{tbl}")
                count = cursor.fetchone()[0]
                print(f"HR.{tbl}: Found ({count} rows)")
                found.append(f"HR.{tbl}")
            except Exception:
                print(f"HR.{tbl}: Not found")
        
        return found

if __name__ == "__main__":
    find_hr_tables()
