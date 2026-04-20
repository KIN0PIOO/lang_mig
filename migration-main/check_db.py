from app.core.db import get_connection

def check_jobs():
    query = "SELECT MAP_ID, STATUS, RETRY_COUNT FROM NEXT_MIG_INFO"
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        for row in cursor.fetchall():
            print(row)

if __name__ == "__main__":
    check_jobs()
