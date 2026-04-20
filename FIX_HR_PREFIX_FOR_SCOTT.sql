-- ============================================================
-- FR_TABLE / SQL CLOB 에서 'HR.' 스키마 접두어 제거
-- 실행 대상: 회사 Oracle 21c DB (SCOTT 스키마 단독 환경)
-- 실행 순서: META_MIGRATION_TO_21C.sql 실행 완료 후 이 스크립트 실행
-- ============================================================

-- 1. FR_TABLE: HR. 제거 + 기존 생성 SQL 초기화 (LLM이 재생성하도록)
UPDATE NEXT_MIG_INFO
SET
    FR_TABLE        = REPLACE(FR_TABLE, 'HR.', ''),
    MIG_SQL         = NULL,
    VERIFY_SQL      = NULL,
    DDL_SQL         = NULL,
    STATUS          = 'WAIT',
    USE_YN          = 'Y',
    BATCH_CNT       = 0,
    RETRY_COUNT     = 0,
    ELAPSED_SECONDS = 0
WHERE FR_TABLE LIKE '%HR.%';

-- 2. CORRECT_SQL: 힌트 SQL 안에도 HR. 참조가 있으면 제거
UPDATE NEXT_MIG_INFO
SET CORRECT_SQL = REPLACE(TO_CHAR(CORRECT_SQL), 'HR.', '')
WHERE CORRECT_SQL LIKE '%HR.%';

COMMIT;

-- 3. 결과 확인
SELECT MAP_ID, FR_TABLE, STATUS, USE_YN
FROM NEXT_MIG_INFO
ORDER BY MAP_ID;
