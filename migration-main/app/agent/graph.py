import time
import os
import re
from typing import Literal
from langgraph.graph import StateGraph, END
from app.core.logger import logger
from app.core.exceptions import (
    LLMBaseError, LLMAuthenticationError, LLMTokenLimitError, LLMInvalidRequestError,
    DBSqlError, VerificationFailError, BatchAbortError
)
from app.agent.llm_client import generate_sqls
from app.agent.executor import execute_migration, drop_table_if_exists
from app.agent.verifier import execute_verification
from app.domain.mapping.repository import update_job_status
from app.domain.history.repository import log_generated_sql, log_business_history
from app.core.db import fetch_table_ddl
from app.agent.state import MigrationState

def _extract_table_names(fr_table: str) -> list:
    """FR_TABLE 표현식에서 실제 테이블명만 추출합니다."""
    parts = re.split(
        r'\b(?:(?:LEFT|RIGHT|FULL|INNER|CROSS)\s+(?:OUTER\s+)?)?JOIN\b',
        fr_table, flags=re.IGNORECASE
    )
    tables = []
    for part in parts:
        part = re.split(r'\bON\b', part, flags=re.IGNORECASE)[0].strip()
        tokens = part.split()
        if tokens and tokens[0].upper() not in ('SELECT', 'WITH', 'FROM', '('):
            tables.append(tokens[0])
    return tables

# Nodes
def fetch_ddl_node(state: MigrationState) -> dict:
    job = state["next_sql_info"]
    source_ddl = {}
    for tbl_name in _extract_table_names(job.fr_table):
        rows = fetch_table_ddl(tbl_name)
        if rows:
            source_ddl[tbl_name] = rows
            logger.info(f"[Graph:DDL] {tbl_name} 컬럼 {len(rows)}개 조회 완료")
    return {"source_ddl": source_ddl if source_ddl else None}

def generate_sql_node(state: MigrationState) -> dict:
    job = state["next_sql_info"]
    job.retry_count = state["db_attempts"] - 1
    
    logger.info(f"[Graph:LLM] Attempt {state['db_attempts']}/{state['max_attempts']} | SQL 생성 요청")
    try:
        ddl_sql, migration_sql, v_sql = generate_sqls(
            job, 
            state["last_error"], 
            state["last_sql"], 
            state["source_ddl"]
        )
        
        # DB 기록
        log_generated_sql(job.map_id, migration_sql, v_sql)
        
        return {
            "last_sql": migration_sql,
            "current_ddl_sql": ddl_sql,
            "current_migration_sql": migration_sql,
            "current_v_sql": v_sql,
            "error_type": None
        }
    except (LLMAuthenticationError, LLMTokenLimitError, LLMInvalidRequestError) as e:
        logger.error(f"[Graph:LLM_FATAL] {str(e)}")
        raise BatchAbortError(f"LLM 치명적 에러: {str(e)}") from e
    except LLMBaseError as e:
        return {"error_type": "LLM_RETRY", "last_error": str(e)}

def execute_sql_node(state: MigrationState) -> dict:
    job = state["next_sql_info"]
    to_table = job.to_table
    
    try:
        logger.info(f"[Graph:EXEC] 클린업 및 실행 시작")
        drop_table_if_exists(to_table)
        
        if state.get("current_ddl_sql"):
            execute_migration(state["current_ddl_sql"])
            
        execute_migration(state["current_migration_sql"])
        return {"status": "EXECUTED", "error_type": None}
    except DBSqlError as e:
        logger.error(f"[Graph:EXEC_FAIL] {str(e)}")
        return {"error_type": "BIZ_RETRY", "last_error": str(e)}

def verify_sql_node(state: MigrationState) -> dict:
    v_sql = state.get("current_v_sql")
    if not v_sql:
        return {"status": "PASS"}
        
    try:
        logger.info(f"[Graph:VERIFY] 데이터 정합성 검증 시작")
        is_valid, v_msg = execute_verification(v_sql)
        if not is_valid:
            return {"error_type": "BIZ_RETRY", "last_error": f"데이터 불일치: {v_msg}"}
        return {"status": "PASS", "error_type": None}
    except (VerificationFailError, DBSqlError) as e:
        return {"error_type": "BIZ_RETRY", "last_error": str(e)}

def finalize_node(state: MigrationState) -> dict:
    job = state["next_sql_info"]
    elapsed = int(time.time() - state["job_start_time"])
    mig_kind = os.getenv("MIG_KIND", "DB_MIG")
    
    if state["status"] == "PASS":
        update_job_status(job.map_id, "PASS", elapsed, state["db_attempts"])
        log_business_history(job.map_id, "INFO", "INFO", "VERIFY", "PASS", "Migration Success", state["db_attempts"], mig_kind)
        logger.info(f"[Graph:FINISH] map_id={job.map_id} | >>> 성공 <<<")
    else:
        update_job_status(job.map_id, "FAIL", elapsed, state["db_attempts"])
        log_business_history(job.map_id, "JOB_FAIL", "ERROR", "FINAL", "FAIL", "Max Attempts Reached", state["db_attempts"], mig_kind)
        logger.error(f"[Graph:FINISH] map_id={job.map_id} | >>> 실패 <<<")
    
    return {"elapsed_time": elapsed}

# Routing Logic
def should_continue(state: MigrationState) -> Literal["generate", "finalize", "verify", "execute", "llm_retry_wait"]:
    error_type = state.get("error_type")
    
    if state.get("status") == "PASS":
        return "finalize"
        
    if error_type == "LLM_RETRY":
        if state["llm_retry_count"] < 2:
            return "llm_retry_wait"
        else:
            raise BatchAbortError(f"LLM 재시도 초과: {state['last_error']}")

    if error_type == "BIZ_RETRY":
        if state["db_attempts"] < state["max_attempts"]:
            return "generate"
        else:
            return "finalize"
            
    # 에러가 없고 현재 상태에 따라 다음 단계 진행
    if state.get("status") == "EXECUTED":
        return "verify"
    
    return "execute"

def llm_retry_wait_node(state: MigrationState) -> dict:
    time.sleep(1)
    return {"llm_retry_count": state["llm_retry_count"] + 1}

def biz_retry_prepare_node(state: MigrationState) -> dict:
    # 비즈니스 에러 발생 시 시도 횟수 증가 및 로그 기록
    job = state["next_sql_info"]
    mig_kind = os.getenv("MIG_KIND", "DB_MIG")
    step_name = "SQL_EXEC" if "DBSqlError" in state["last_error"] else "VERIFY"
    
    log_business_history(job.map_id, "ROW_ERROR", "WARN", step_name, "FAIL", state["last_error"], state["db_attempts"], mig_kind)
    time.sleep(1)
    return {"db_attempts": state["db_attempts"] + 1, "error_type": None}

# Graph Construction
workflow = StateGraph(MigrationState)

workflow.add_node("fetch_ddl", fetch_ddl_node)
workflow.add_node("generate", generate_sql_node)
workflow.add_node("execute", execute_sql_node)
workflow.add_node("verify", verify_sql_node)
workflow.add_node("finalize", finalize_node)
workflow.add_node("llm_retry_wait", llm_retry_wait_node)
workflow.add_node("biz_retry_prepare", biz_retry_prepare_node)

workflow.set_entry_point("fetch_ddl")
workflow.add_edge("fetch_ddl", "generate")

workflow.add_conditional_edges(
    "generate",
    should_continue,
    {
        "execute": "execute",
        "llm_retry_wait": "llm_retry_wait",
        "finalize": "finalize"
    }
)

workflow.add_edge("llm_retry_wait", "generate")

workflow.add_conditional_edges(
    "execute",
    should_continue,
    {
        "verify": "verify",
        "generate": "biz_retry_prepare",
        "finalize": "finalize"
    }
)

workflow.add_conditional_edges(
    "verify",
    should_continue,
    {
        "finalize": "finalize",
        "generate": "biz_retry_prepare"
    }
)

workflow.add_edge("biz_retry_prepare", "generate")
workflow.add_edge("finalize", END)

migration_graph = workflow.compile()
