from app.core.db import get_connection

def reset_job(map_id):
    query = "UPDATE NEXT_MIG_INFO SET STATUS = 'READY', USE_YN = 'Y', RETRY_COUNT = 0 WHERE MAP_ID = :1"
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, (map_id,))
        conn.commit()
    print(f"Job {map_id} reset to 'READY' and USE_YN='Y'")

if __name__ == "__main__":
    reset_job(1)
    reset_job(3)
