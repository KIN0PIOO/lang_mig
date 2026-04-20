import sys
import os
from dotenv import load_dotenv

# .env 환경 변수 로드
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(env_path)

# 모듈 경로 설정
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.logger import logger
from app.agent.orchestrator import MigrationOrchestrator
from app.domain.mapping.repository import get_pending_jobs

def test_single_job():
    logger.info("Starting LangGraph Agent Test...")
    
    jobs = get_pending_jobs()
    if not jobs:
        logger.warning("No pending jobs found in DB. Please ensure there is a job with USE_YN='Y' and TARGET_YN IS NOT NULL.")
        return

    job = jobs[0]
    logger.info(f"Testing with Job Map ID: {job.map_id} ({job.fr_table} -> {job.to_table})")
    
    orchestrator = MigrationOrchestrator()
    try:
        orchestrator.process_job(job)
        logger.info("Test completed successfully.")
    except Exception as e:
        logger.error(f"Test failed with error: {e}")

if __name__ == "__main__":
    test_single_job()
