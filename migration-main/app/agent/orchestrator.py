import time
import os
from app.core.logger import logger
from app.agent.graph import migration_graph
from app.domain.mapping.repository import increment_batch_count

class MigrationOrchestrator:
    def __init__(self):
        self.mig_kind = os.getenv("MIG_KIND", "DB_MIG")

    def process_job(self, NEXT_SQL_INFO):
        logger.info(f"\n==========================================")
        logger.info(f"[JOB_START] 대상 작업(map_id={NEXT_SQL_INFO.map_id}) 프로세스 시작 (LangGraph)")
        
        # 0. BATCH_COUNT 증가 (작업 시작 기록)
        increment_batch_count(NEXT_SQL_INFO.map_id)
        
        # 1. 초기 상태 정의
        initial_state = {
            "next_sql_info": NEXT_SQL_INFO,
            "source_ddl": None,
            "last_error": None,
            "last_sql": None,
            "db_attempts": 1,
            "max_attempts": 3,
            "llm_retry_count": 0,
            "current_ddl_sql": None,
            "current_migration_sql": None,
            "current_v_sql": None,
            "error_type": None,
            "status": "RUNNING",
            "elapsed_time": 0,
            "job_start_time": time.time()
        }

        # 2. 그래프 실행
        try:
            final_state = migration_graph.invoke(initial_state)
            logger.info(f"[JOB_DONE] map_id={NEXT_SQL_INFO.map_id} | 최종 상태: {final_state['status']} | 소요시간: {final_state['elapsed_time']}초")
        except Exception as e:
            # BatchAbortError 등 치명적 에러는 그대로 상위(스케줄러)로 전파
            raise e
