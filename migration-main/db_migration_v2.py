import oracledb
from app.core.db import get_connection

def migrate_schema():
    print("Starting Database Schema Migration...")
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            
            # 1. NEXT_MIG_INFO 테이블에 DDL_SQL 컬럼 추가
            print("Adding DDL_SQL column to NEXT_MIG_INFO...")
            try:
                cursor.execute("ALTER TABLE NEXT_MIG_INFO ADD (DDL_SQL CLOB)")
                print("Added DDL_SQL column.")
            except oracledb.DatabaseError as e:
                if "ORA-01430" in str(e): # 이미 존재하는 경우
                    print("DDL_SQL column already exists.")
                else:
                    raise e
            
            # 2. NEXT_MIG_INFO_DTL 테이블의 MAP_DTL_ID를 MAP_DTL로 변경
            print("Renaming MAP_DTL_ID to MAP_DTL in NEXT_MIG_INFO_DTL...")
            try:
                cursor.execute("ALTER TABLE NEXT_MIG_INFO_DTL RENAME COLUMN MAP_DTL_ID TO MAP_DTL")
                print("Renamed column to MAP_DTL.")
            except oracledb.DatabaseError as e:
                if "ORA-00904" in str(e): # 이미 다른 이름이거나 없는 경우
                    print("Could not find MAP_DTL_ID. It might have been already renamed or uses a different name.")
                else:
                    raise e
                    
            conn.commit()
            print("Migration completed successfully.")
    except Exception as e:
        print(f"Migration failed: {e}")

if __name__ == "__main__":
    migrate_schema()
