-- ============================================================
-- Simple Test Cases 추가 삽입
-- 대상: NEXT_MIG_INFO (MAP_ID 200~204), NEXT_MIG_INFO_DTL (MAP_DTL 2001~2017)
-- 실행 계정: scott
-- 주의: 기존 데이터 미수정 — INSERT ONLY
-- ============================================================


-- ============================================================
-- MAP_ID = 200 : JOBS → TGT_JOBS  (단순 1:1 직접 복사)
-- STATUS: PENDING (아직 실행 안 됨)
-- ============================================================
DECLARE
    v_mig    CLOB;
    v_verify CLOB;
BEGIN
    v_mig :=
        'INSERT INTO TGT_JOBS (JOB_ID, JOB_TITLE, MIN_SALARY, MAX_SALARY)' || CHR(10) ||
        'SELECT JOB_ID, JOB_TITLE, MIN_SALARY, MAX_SALARY' || CHR(10) ||
        'FROM JOBS;';

    v_verify :=
        'SELECT SUM(CASE WHEN NVL(src.JOB_TITLE, ''X'') <> NVL(tgt.JOB_TITLE, ''X'') THEN 1 ELSE 0 END)' || CHR(10) ||
        '     + SUM(CASE WHEN NVL(src.MIN_SALARY, -1) <> NVL(tgt.MIN_SALARY, -1) THEN 1 ELSE 0 END)' || CHR(10) ||
        '     + SUM(CASE WHEN NVL(src.MAX_SALARY, -1) <> NVL(tgt.MAX_SALARY, -1) THEN 1 ELSE 0 END)' || CHR(10) ||
        '     AS DIFF' || CHR(10) ||
        'FROM JOBS src' || CHR(10) ||
        'FULL OUTER JOIN TGT_JOBS tgt ON src.JOB_ID = tgt.JOB_ID;';

    INSERT INTO NEXT_MIG_INFO (
        MAP_ID, MAP_TYPE, FR_TABLE, TO_TABLE, USE_YN, TARGET_YN, PRIORITY,
        MIG_SQL, VERIFY_SQL, STATUS, BATCH_CNT, CORRECT_SQL, USER_EDITED,
        UPD_TS, ELAPSED_SECONDS, RETRY_COUNT, CREATED_AT, DDL_SQL
    ) VALUES (
        200, 'SIMPLE', 'JOBS', 'TGT_JOBS',
        'Y', 'Y', 10,
        v_mig, v_verify,
        'PENDING', 0, NULL, 'N',
        TO_TIMESTAMP('2026-04-16 09:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.FF6'), 0, 0,
        TO_TIMESTAMP('2026-04-16 09:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.FF6'), NULL
    );
END;
/


-- ============================================================
-- MAP_ID = 201 : DEPARTMENTS → TGT_DEPARTMENTS  (컬럼명 변환)
-- STATUS: PASS
-- ============================================================
DECLARE
    v_mig    CLOB;
    v_verify CLOB;
BEGIN
    v_mig :=
        'INSERT INTO TGT_DEPARTMENTS (DEPT_ID, DEPT_NAME, MGR_ID, LOC_ID)' || CHR(10) ||
        'SELECT DEPARTMENT_ID, DEPARTMENT_NAME, MANAGER_ID, LOCATION_ID' || CHR(10) ||
        'FROM DEPARTMENTS;';

    v_verify :=
        'SELECT SUM(CASE WHEN NVL(src.DEPARTMENT_NAME, ''X'') <> NVL(tgt.DEPT_NAME, ''X'') THEN 1 ELSE 0 END)' || CHR(10) ||
        '     + SUM(CASE WHEN NVL(src.MANAGER_ID, -1)    <> NVL(tgt.MGR_ID, -1)           THEN 1 ELSE 0 END)' || CHR(10) ||
        '     + SUM(CASE WHEN NVL(src.LOCATION_ID, -1)   <> NVL(tgt.LOC_ID, -1)           THEN 1 ELSE 0 END)' || CHR(10) ||
        '     AS DIFF' || CHR(10) ||
        'FROM DEPARTMENTS src' || CHR(10) ||
        'FULL OUTER JOIN TGT_DEPARTMENTS tgt ON src.DEPARTMENT_ID = tgt.DEPT_ID;';

    INSERT INTO NEXT_MIG_INFO (
        MAP_ID, MAP_TYPE, FR_TABLE, TO_TABLE, USE_YN, TARGET_YN, PRIORITY,
        MIG_SQL, VERIFY_SQL, STATUS, BATCH_CNT, CORRECT_SQL, USER_EDITED,
        UPD_TS, ELAPSED_SECONDS, RETRY_COUNT, CREATED_AT, DDL_SQL
    ) VALUES (
        201, 'SIMPLE', 'DEPARTMENTS', 'TGT_DEPARTMENTS',
        'Y', 'Y', 11,
        v_mig, v_verify,
        'PASS', 1, NULL, 'N',
        TO_TIMESTAMP('2026-04-16 09:01:23.000000', 'YYYY-MM-DD HH24:MI:SS.FF6'), 2, 1,
        TO_TIMESTAMP('2026-04-16 09:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.FF6'), NULL
    );
END;
/


-- ============================================================
-- MAP_ID = 202 : LOCATIONS → TGT_LOCATIONS  (컬럼 일부 생략)
-- STATUS: PENDING
-- ============================================================
DECLARE
    v_mig    CLOB;
    v_verify CLOB;
BEGIN
    v_mig :=
        'INSERT INTO TGT_LOCATIONS (LOC_ID, CITY, POSTAL_CODE, COUNTRY_ID)' || CHR(10) ||
        'SELECT LOCATION_ID, CITY, POSTAL_CODE, COUNTRY_ID' || CHR(10) ||
        'FROM LOCATIONS;';

    v_verify :=
        'SELECT SUM(CASE WHEN NVL(src.CITY, ''X'')       <> NVL(tgt.CITY, ''X'')       THEN 1 ELSE 0 END)' || CHR(10) ||
        '     + SUM(CASE WHEN NVL(src.POSTAL_CODE, ''X'') <> NVL(tgt.POSTAL_CODE, ''X'') THEN 1 ELSE 0 END)' || CHR(10) ||
        '     + SUM(CASE WHEN NVL(src.COUNTRY_ID, ''X'')  <> NVL(tgt.COUNTRY_ID, ''X'')  THEN 1 ELSE 0 END)' || CHR(10) ||
        '     AS DIFF' || CHR(10) ||
        'FROM LOCATIONS src' || CHR(10) ||
        'FULL OUTER JOIN TGT_LOCATIONS tgt ON src.LOCATION_ID = tgt.LOC_ID;';

    INSERT INTO NEXT_MIG_INFO (
        MAP_ID, MAP_TYPE, FR_TABLE, TO_TABLE, USE_YN, TARGET_YN, PRIORITY,
        MIG_SQL, VERIFY_SQL, STATUS, BATCH_CNT, CORRECT_SQL, USER_EDITED,
        UPD_TS, ELAPSED_SECONDS, RETRY_COUNT, CREATED_AT, DDL_SQL
    ) VALUES (
        202, 'SIMPLE', 'LOCATIONS', 'TGT_LOCATIONS',
        'Y', 'Y', 12,
        v_mig, v_verify,
        'PENDING', 0, NULL, 'N',
        TO_TIMESTAMP('2026-04-16 09:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.FF6'), 0, 0,
        TO_TIMESTAMP('2026-04-16 09:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.FF6'), NULL
    );
END;
/


-- ============================================================
-- MAP_ID = 203 : COUNTRIES → TGT_COUNTRIES  (컬럼명 변환 + FAIL 케이스)
-- STATUS: FAIL (타겟 테이블 컬럼명 오류 시뮬레이션)
-- ============================================================
DECLARE
    v_mig     CLOB;
    v_verify  CLOB;
    v_correct CLOB;
BEGIN
    v_mig :=
        'INSERT INTO TGT_COUNTRIES (COUNTRY_CODE, COUNTRY_NAME, REGION_ID)' || CHR(10) ||
        'SELECT COUNTRY_ID, COUNTRY_NAME, REGION_ID' || CHR(10) ||
        'FROM COUNTRIES;';

    v_verify :=
        'SELECT SUM(CASE WHEN NVL(src.COUNTRY_NAME, ''X'') <> NVL(tgt.COUNTRY_NAME, ''X'') THEN 1 ELSE 0 END)' || CHR(10) ||
        '     + SUM(CASE WHEN NVL(src.REGION_ID, -1)       <> NVL(tgt.REGION_ID, -1)       THEN 1 ELSE 0 END)' || CHR(10) ||
        '     AS DIFF' || CHR(10) ||
        'FROM COUNTRIES src' || CHR(10) ||
        'FULL OUTER JOIN TGT_COUNTRIES tgt ON src.COUNTRY_ID = tgt.COUNTRY_CODE;';

    v_correct :=
        '-- 컬럼명 불일치 수정: COUNTRY_ID → COUNTRY_CODE' || CHR(10) ||
        'INSERT INTO TGT_COUNTRIES (COUNTRY_CODE, COUNTRY_NAME, REGION_ID)' || CHR(10) ||
        'SELECT COUNTRY_ID, COUNTRY_NAME, REGION_ID FROM COUNTRIES;';

    INSERT INTO NEXT_MIG_INFO (
        MAP_ID, MAP_TYPE, FR_TABLE, TO_TABLE, USE_YN, TARGET_YN, PRIORITY,
        MIG_SQL, VERIFY_SQL, STATUS, BATCH_CNT, CORRECT_SQL, USER_EDITED,
        UPD_TS, ELAPSED_SECONDS, RETRY_COUNT, CREATED_AT, DDL_SQL
    ) VALUES (
        203, 'SIMPLE', 'COUNTRIES', 'TGT_COUNTRIES',
        'Y', 'Y', 13,
        v_mig, v_verify,
        'FAIL', 2, v_correct, 'N',
        TO_TIMESTAMP('2026-04-16 09:02:45.000000', 'YYYY-MM-DD HH24:MI:SS.FF6'), 3, 2,
        TO_TIMESTAMP('2026-04-16 09:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.FF6'), NULL
    );
END;
/


-- ============================================================
-- MAP_ID = 204 : REGIONS → TGT_REGIONS  (가장 단순한 2컬럼 직접 복사)
-- STATUS: PASS
-- ============================================================
DECLARE
    v_mig    CLOB;
    v_verify CLOB;
BEGIN
    v_mig :=
        'INSERT INTO TGT_REGIONS (REGION_ID, REGION_NAME)' || CHR(10) ||
        'SELECT REGION_ID, REGION_NAME' || CHR(10) ||
        'FROM REGIONS;';

    v_verify :=
        'SELECT SUM(CASE WHEN NVL(src.REGION_NAME, ''X'') <> NVL(tgt.REGION_NAME, ''X'') THEN 1 ELSE 0 END)' || CHR(10) ||
        '     AS DIFF' || CHR(10) ||
        'FROM REGIONS src' || CHR(10) ||
        'FULL OUTER JOIN TGT_REGIONS tgt ON src.REGION_ID = tgt.REGION_ID;';

    INSERT INTO NEXT_MIG_INFO (
        MAP_ID, MAP_TYPE, FR_TABLE, TO_TABLE, USE_YN, TARGET_YN, PRIORITY,
        MIG_SQL, VERIFY_SQL, STATUS, BATCH_CNT, CORRECT_SQL, USER_EDITED,
        UPD_TS, ELAPSED_SECONDS, RETRY_COUNT, CREATED_AT, DDL_SQL
    ) VALUES (
        204, 'SIMPLE', 'REGIONS', 'TGT_REGIONS',
        'Y', 'Y', 14,
        v_mig, v_verify,
        'PASS', 1, NULL, 'N',
        TO_TIMESTAMP('2026-04-16 09:03:10.000000', 'YYYY-MM-DD HH24:MI:SS.FF6'), 1, 1,
        TO_TIMESTAMP('2026-04-16 09:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.FF6'), NULL
    );
END;
/

COMMIT;


-- ============================================================
-- NEXT_MIG_INFO_DTL 삽입 (MAP_DTL 2001~2017)
-- ============================================================

-- MAP_ID = 200 (JOBS → TGT_JOBS)
INSERT INTO NEXT_MIG_INFO_DTL (MAP_DTL, MAP_ID, FR_COL, TO_COL) VALUES (2001, 200, 'JOB_ID',    'JOB_ID');
INSERT INTO NEXT_MIG_INFO_DTL (MAP_DTL, MAP_ID, FR_COL, TO_COL) VALUES (2002, 200, 'JOB_TITLE', 'JOB_TITLE');
INSERT INTO NEXT_MIG_INFO_DTL (MAP_DTL, MAP_ID, FR_COL, TO_COL) VALUES (2003, 200, 'MIN_SALARY','MIN_SALARY');
INSERT INTO NEXT_MIG_INFO_DTL (MAP_DTL, MAP_ID, FR_COL, TO_COL) VALUES (2004, 200, 'MAX_SALARY','MAX_SALARY');

-- MAP_ID = 201 (DEPARTMENTS → TGT_DEPARTMENTS)
INSERT INTO NEXT_MIG_INFO_DTL (MAP_DTL, MAP_ID, FR_COL, TO_COL) VALUES (2005, 201, 'DEPARTMENT_ID',   'DEPT_ID');
INSERT INTO NEXT_MIG_INFO_DTL (MAP_DTL, MAP_ID, FR_COL, TO_COL) VALUES (2006, 201, 'DEPARTMENT_NAME', 'DEPT_NAME');
INSERT INTO NEXT_MIG_INFO_DTL (MAP_DTL, MAP_ID, FR_COL, TO_COL) VALUES (2007, 201, 'MANAGER_ID',      'MGR_ID');
INSERT INTO NEXT_MIG_INFO_DTL (MAP_DTL, MAP_ID, FR_COL, TO_COL) VALUES (2008, 201, 'LOCATION_ID',     'LOC_ID');

-- MAP_ID = 202 (LOCATIONS → TGT_LOCATIONS)
INSERT INTO NEXT_MIG_INFO_DTL (MAP_DTL, MAP_ID, FR_COL, TO_COL) VALUES (2009, 202, 'LOCATION_ID', 'LOC_ID');
INSERT INTO NEXT_MIG_INFO_DTL (MAP_DTL, MAP_ID, FR_COL, TO_COL) VALUES (2010, 202, 'CITY',        'CITY');
INSERT INTO NEXT_MIG_INFO_DTL (MAP_DTL, MAP_ID, FR_COL, TO_COL) VALUES (2011, 202, 'POSTAL_CODE', 'POSTAL_CODE');
INSERT INTO NEXT_MIG_INFO_DTL (MAP_DTL, MAP_ID, FR_COL, TO_COL) VALUES (2012, 202, 'COUNTRY_ID',  'COUNTRY_ID');

-- MAP_ID = 203 (COUNTRIES → TGT_COUNTRIES)
INSERT INTO NEXT_MIG_INFO_DTL (MAP_DTL, MAP_ID, FR_COL, TO_COL) VALUES (2013, 203, 'COUNTRY_ID',   'COUNTRY_CODE');
INSERT INTO NEXT_MIG_INFO_DTL (MAP_DTL, MAP_ID, FR_COL, TO_COL) VALUES (2014, 203, 'COUNTRY_NAME', 'COUNTRY_NAME');
INSERT INTO NEXT_MIG_INFO_DTL (MAP_DTL, MAP_ID, FR_COL, TO_COL) VALUES (2015, 203, 'REGION_ID',    'REGION_ID');

-- MAP_ID = 204 (REGIONS → TGT_REGIONS)
INSERT INTO NEXT_MIG_INFO_DTL (MAP_DTL, MAP_ID, FR_COL, TO_COL) VALUES (2016, 204, 'REGION_ID',   'REGION_ID');
INSERT INTO NEXT_MIG_INFO_DTL (MAP_DTL, MAP_ID, FR_COL, TO_COL) VALUES (2017, 204, 'REGION_NAME', 'REGION_NAME');

COMMIT;

-- ============================================================
-- 완료: NEXT_MIG_INFO(+5건) | NEXT_MIG_INFO_DTL(+17건)
-- MAP_ID: 200, 201, 202, 203, 204
-- MAP_DTL: 2001~2017
-- ============================================================
