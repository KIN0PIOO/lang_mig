from app.core.logger import logger
from app.domain.mapping.models import MappingRule, MappingDetail
from app.core.db import get_connection
import oracledb

def ensure_str(val):
    """LOB 객체인 경우 문자열로 읽어 반환합니다."""
    if val is not None and hasattr(val, 'read'):
        return val.read()
    return val

def get_pending_jobs() -> list[MappingRule]:
    """USE_YN='Y' 이고 TASK_TARGET IS NOT NULL인 작업을 PRIORITY 순으로 가져옵니다."""
    logger.debug("[Repository] DB에서 작업 대상을 스캔합니다...")
    jobs = {}
    
    query = """
        SELECT 
            R.MAP_ID, R.MAP_TYPE, R.FR_TABLE, R.TO_TABLE, 
            R.USE_YN, R.TARGET_YN, R.PRIORITY, 
            R.MIG_SQL, R.VERIFY_SQL, R.STATUS, R.CORRECT_SQL, R.USER_EDITED, 
            R.BATCH_CNT, R.ELAPSED_SECONDS, R.RETRY_COUNT,
            R.CREATED_AT, R.UPD_TS,
            D.MAP_DTL, D.FR_COL, D.TO_COL
        FROM NEXT_MIG_INFO R
        LEFT JOIN NEXT_MIG_INFO_DTL D ON R.MAP_ID = D.MAP_ID
        WHERE R.USE_YN = 'Y' 
          AND R.TARGET_YN IS NOT NULL
        ORDER BY R.PRIORITY ASC, D.FR_COL ASC
    """
    
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            
            for row in rows:
                map_id = row[0]
                if map_id not in jobs:
                    rule = MappingRule(
                        map_id=map_id,
                        map_type=ensure_str(row[1]),
                        fr_table=ensure_str(row[2]),
                        to_table=ensure_str(row[3]),
                        use_yn=ensure_str(row[4]),
                        target_yn=ensure_str(row[5]),
                        priority=row[6],
                        mig_sql=ensure_str(row[7]),
                        verify_sql=ensure_str(row[8]),
                        status=ensure_str(row[9]),
                        correct_sql=ensure_str(row[10]),
                        user_edited=ensure_str(row[11]),
                        batch_cnt=row[12] if row[12] is not None else 0,
                        elapsed_seconds=row[13] if row[13] is not None else 0,
                        retry_count=row[14] if row[14] is not None else 0,
                        created_at=row[15],
                        upd_ts=row[16],
                        details=[]
                    )
                    jobs[map_id] = rule
                
                # 디테일 정보가 있는 경우 추가
                if row[17] is not None:
                    detail = MappingDetail(
                        map_dtl=row[17],
                        map_id=map_id,
                        fr_col=ensure_str(row[18]),
                        to_col=ensure_str(row[19])
                    )
                    jobs[map_id].details.append(detail)
                    
    except Exception as e:
        logger.error(f"[Repository] 작업 대상을 조회하는 중 오류 발생: {e}")
        
    return list(jobs.values())

def increment_batch_count(map_id: int):
    """작업 시작 시 BATCH_CNT를 1 증가시킵니다."""
    logger.debug(f"[Repository] map_id={map_id} | BATCH_CNT +1")
    query = "UPDATE NEXT_MIG_INFO SET BATCH_CNT = COALESCE(BATCH_CNT, 0) + 1, UPD_TS = CURRENT_TIMESTAMP WHERE MAP_ID = :1"
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (map_id,))
            conn.commit()
    except Exception as e:
        logger.error(f"[Repository] BATCH_COUNT 업데이트 중 오류: {e}")

def update_job_status(map_id: int, status: str, elapsed_seconds: int = 0, retry_count: int = 0):
    """작업 통과/실패 시 상태값을 변경하고, 결과를 업데이트합니다."""
    logger.info(f"[Repository] map_id={map_id} | DB 상태를 {status} 로 업데이트 (Retry: {retry_count})")
    
    query = """
        UPDATE NEXT_MIG_INFO 
        SET STATUS = :1, 
            USE_YN = 'N', 
            UPD_TS = CURRENT_TIMESTAMP, 
            ELAPSED_SECONDS = :2,
            RETRY_COUNT = :3
        WHERE MAP_ID = :4
    """
    
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (status, elapsed_seconds, retry_count, map_id))
            conn.commit()
    except Exception as e:
        logger.error(f"[Repository] 작업 상태 업데이트 중 오류 발생 map_id={map_id}: {e}")
