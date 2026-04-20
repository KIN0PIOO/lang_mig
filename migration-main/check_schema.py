from app.core.db import get_connection

def describe_tables():
    tables = ['NEXT_MIG_INFO', 'NEXT_MIG_INFO_DTL', 'NEXT_MIG_INFO2', 'NEXT_MIG_INFO_DTL2']
    with get_connection() as conn:
        cursor = conn.cursor()
        for table in tables:
            print(f"\n--- Table: {table} ---")
            try:
                # Oracle에서 테이블 컬럼 정보를 가져오는 쿼리
                query = f"SELECT column_name FROM user_tab_columns WHERE table_name = '{table.upper()}'"
                cursor.execute(query)
                cols = cursor.fetchall()
                if not cols:
                    print(f"Table {table} not found.")
                for col in cols:
                    print(col[0])
            except Exception as e:
                print(f"Error describing {table}: {e}")

if __name__ == "__main__":
    describe_tables()
