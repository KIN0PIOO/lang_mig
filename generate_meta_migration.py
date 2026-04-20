"""
NEXT_MIG_INFO, NEXT_MIG_INFO_DTL, NEXT_MIG_LOG 를
Oracle 21c에서 재생성하는 SQL 스크립트 생성기.

CLOB 컬럼(MIG_SQL, VERIFY_SQL, CORRECT_SQL, DDL_SQL)은
PL/SQL DECLARE 블록으로 처리합니다.
"""
import sys
import datetime

sys.path.insert(0, 'migration-main')
from app.core.db import get_connection


def esc(val):
    """단순 문자열 → SQL 작은따옴표 이스케이프"""
    if val is None:
        return None
    return str(val).replace("'", "''")


def to_sql(val):
    """Python 값 → SQL 리터럴 (CLOB 제외 일반 컬럼용)"""
    if val is None:
        return 'NULL'
    if isinstance(val, datetime.datetime):
        return f"TO_TIMESTAMP('{val.strftime('%Y-%m-%d %H:%M:%S.%f')}', 'YYYY-MM-DD HH24:MI:SS.FF6')"
    if isinstance(val, datetime.date):
        return f"TO_DATE('{val.strftime('%Y-%m-%d')}', 'YYYY-MM-DD')"
    if isinstance(val, (int, float)):
        return str(val)
    return "'" + esc(str(val)) + "'"


def clob_assign(var_name, value):
    """CLOB 값을 PL/SQL 변수에 대입하는 라인 생성"""
    if value is None:
        return f"    {var_name} := NULL;"
    # 32767자 초과 시 청크 분할 (현재 데이터는 모두 이하이므로 단일 대입)
    escaped = esc(str(value))
    return f"    {var_name} := '{escaped}';"


def main():
    conn = get_connection()
    cur = conn.cursor()
    lines = []

    # ----------------------------------------------------------------
    # 헤더
    # ----------------------------------------------------------------
    lines += [
        '-- ============================================================',
        '-- Migration Metadata Tables: Oracle 11.2 XE -> Oracle 21c',
        '-- 생성일: 2026-04-15',
        '-- 대상: NEXT_MIG_INFO, NEXT_MIG_INFO_DTL, NEXT_MIG_LOG',
        '-- 실행 계정: 해당 스키마 소유자 (scott 또는 동등 권한)',
        '-- ============================================================',
        '',
        '-- 기존 테이블 삭제 (FK 역순)',
    ]
    for t in ['NEXT_MIG_LOG', 'NEXT_MIG_INFO_DTL', 'NEXT_MIG_INFO']:
        lines.append(
            f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {t} CASCADE CONSTRAINTS PURGE';"
            f" EXCEPTION WHEN OTHERS THEN NULL; END;"
        )
        lines.append('/')
    lines.append('')

    # ----------------------------------------------------------------
    # 시퀀스 삭제 (있으면)
    # ----------------------------------------------------------------
    for seq in ['MAPPING_RULES_SEQ', 'MAPPING_RULE_DETAIL_SEQ', 'MAP_DTL_SEQ', 'MIGRATION_LOG_SEQ']:
        lines.append(
            f"BEGIN EXECUTE IMMEDIATE 'DROP SEQUENCE {seq}';"
            f" EXCEPTION WHEN OTHERS THEN NULL; END;"
        )
        lines.append('/')
    lines.append('')

    # ----------------------------------------------------------------
    # 1. CREATE TABLES
    # ----------------------------------------------------------------
    lines += [
        '-- ============================================================',
        '-- 1. CREATE TABLES',
        '-- ============================================================',
        '',
        'CREATE TABLE NEXT_MIG_INFO (',
        '    MAP_ID           NUMBER            NOT NULL,',
        '    MAP_TYPE         VARCHAR2(20),',
        '    FR_TABLE         VARCHAR2(200),',
        '    TO_TABLE         VARCHAR2(200),',
        '    USE_YN           CHAR(1)           DEFAULT \'Y\',',
        '    TARGET_YN        VARCHAR2(100),',
        '    PRIORITY         NUMBER,',
        '    MIG_SQL          CLOB,',
        '    VERIFY_SQL       CLOB,',
        '    STATUS           VARCHAR2(20),',
        '    BATCH_CNT        NUMBER            DEFAULT 0,',
        '    CORRECT_SQL      CLOB,',
        '    USER_EDITED      CHAR(1)           DEFAULT \'N\',',
        '    UPD_TS           TIMESTAMP         DEFAULT CURRENT_TIMESTAMP,',
        '    ELAPSED_SECONDS  NUMBER            DEFAULT 0,',
        '    RETRY_COUNT      NUMBER            DEFAULT 0,',
        '    CREATED_AT       TIMESTAMP         DEFAULT CURRENT_TIMESTAMP,',
        '    DDL_SQL          CLOB,',
        '    CONSTRAINT NEXT_MIG_INFO_PK PRIMARY KEY (MAP_ID)',
        ');',
        '/',
        '',
        'CREATE TABLE NEXT_MIG_INFO_DTL (',
        '    MAP_DTL  NUMBER         NOT NULL,',
        '    MAP_ID   NUMBER,',
        '    FR_COL   VARCHAR2(200),',
        '    TO_COL   VARCHAR2(200),',
        '    CONSTRAINT NEXT_MIG_INFO_DTL_PK PRIMARY KEY (MAP_DTL),',
        '    CONSTRAINT FK_MAPPING_RULE FOREIGN KEY (MAP_ID)',
        '        REFERENCES NEXT_MIG_INFO(MAP_ID)',
        ');',
        '/',
        '',
        'CREATE TABLE NEXT_MIG_LOG (',
        '    LOG_ID       NUMBER            NOT NULL,',
        '    MAP_ID       NUMBER,',
        '    MIG_KIND     VARCHAR2(20),',
        '    LOG_TYPE     VARCHAR2(20),',
        '    LOG_LEVEL    VARCHAR2(20),',
        '    STEP_NAME    VARCHAR2(50),',
        '    STATUS       VARCHAR2(20),',
        '    MESSAGE      VARCHAR2(4000),',
        '    RETRY_COUNT  NUMBER            DEFAULT 0,',
        '    CREATED_AT   TIMESTAMP         DEFAULT CURRENT_TIMESTAMP,',
        '    CONSTRAINT NEXT_MIG_LOG_PK PRIMARY KEY (LOG_ID)',
        ');',
        '/',
        '',
    ]

    # ----------------------------------------------------------------
    # 2. INSERT NEXT_MIG_INFO (CLOB → PL/SQL 블록)
    # ----------------------------------------------------------------
    lines += [
        '-- ============================================================',
        '-- 2. INSERT NEXT_MIG_INFO (CLOB 포함, PL/SQL 블록 사용)',
        '-- ============================================================',
        '',
    ]

    cur.execute("""
        SELECT MAP_ID, MAP_TYPE, FR_TABLE, TO_TABLE, USE_YN, TARGET_YN, PRIORITY,
               MIG_SQL, VERIFY_SQL, STATUS, BATCH_CNT, CORRECT_SQL, USER_EDITED,
               UPD_TS, ELAPSED_SECONDS, RETRY_COUNT, CREATED_AT, DDL_SQL
        FROM NEXT_MIG_INFO
        ORDER BY MAP_ID
    """)
    info_rows = cur.fetchall()
    info_cols = [d[0] for d in cur.description]

    for row in info_rows:
        d = dict(zip(info_cols, row))
        map_id      = d['MAP_ID']
        mig_sql     = d['MIG_SQL']
        verify_sql  = d['VERIFY_SQL']
        correct_sql = d['CORRECT_SQL']
        ddl_sql     = d['DDL_SQL']

        lines += [
            f'-- MAP_ID = {map_id} ({d["TO_TABLE"]})',
            'DECLARE',
            '    v_mig     CLOB;',
            '    v_verify  CLOB;',
            '    v_correct CLOB;',
            '    v_ddl     CLOB;',
            'BEGIN',
            clob_assign('v_mig',     mig_sql),
            clob_assign('v_verify',  verify_sql),
            clob_assign('v_correct', correct_sql),
            clob_assign('v_ddl',     ddl_sql),
            '    INSERT INTO NEXT_MIG_INFO (',
            '        MAP_ID, MAP_TYPE, FR_TABLE, TO_TABLE, USE_YN, TARGET_YN, PRIORITY,',
            '        MIG_SQL, VERIFY_SQL, STATUS, BATCH_CNT, CORRECT_SQL, USER_EDITED,',
            '        UPD_TS, ELAPSED_SECONDS, RETRY_COUNT, CREATED_AT, DDL_SQL',
            '    ) VALUES (',
            f'        {to_sql(d["MAP_ID"])}, {to_sql(d["MAP_TYPE"])}, {to_sql(d["FR_TABLE"])}, {to_sql(d["TO_TABLE"])},',
            f'        {to_sql(d["USE_YN"])}, {to_sql(d["TARGET_YN"])}, {to_sql(d["PRIORITY"])},',
            '        v_mig, v_verify,',
            f'        {to_sql(d["STATUS"])}, {to_sql(d["BATCH_CNT"])}, v_correct, {to_sql(d["USER_EDITED"])},',
            f'        {to_sql(d["UPD_TS"])}, {to_sql(d["ELAPSED_SECONDS"])}, {to_sql(d["RETRY_COUNT"])},',
            f'        {to_sql(d["CREATED_AT"])}, v_ddl',
            '    );',
            'END;',
            '/',
            '',
        ]

    lines.append('COMMIT;')
    lines.append('')

    # ----------------------------------------------------------------
    # 3. INSERT NEXT_MIG_INFO_DTL
    # ----------------------------------------------------------------
    lines += [
        '-- ============================================================',
        '-- 3. INSERT NEXT_MIG_INFO_DTL (103건)',
        '-- ============================================================',
        '',
    ]

    cur.execute("""
        SELECT MAP_DTL, MAP_ID, FR_COL, TO_COL
        FROM NEXT_MIG_INFO_DTL
        ORDER BY MAP_DTL
    """)
    dtl_rows = cur.fetchall()

    for row in dtl_rows:
        map_dtl, map_id, fr_col, to_col = row
        lines.append(
            f'INSERT INTO NEXT_MIG_INFO_DTL (MAP_DTL, MAP_ID, FR_COL, TO_COL)'
            f' VALUES ({to_sql(map_dtl)}, {to_sql(map_id)}, {to_sql(fr_col)}, {to_sql(to_col)});'
        )

    lines.append('COMMIT;')
    lines.append('')

    # ----------------------------------------------------------------
    # 4. INSERT NEXT_MIG_LOG
    # ----------------------------------------------------------------
    lines += [
        '-- ============================================================',
        '-- 4. INSERT NEXT_MIG_LOG (실행 이력)',
        '-- ============================================================',
        '',
    ]

    cur.execute("""
        SELECT LOG_ID, MAP_ID, MIG_KIND, LOG_TYPE, LOG_LEVEL, STEP_NAME,
               STATUS, MESSAGE, RETRY_COUNT, CREATED_AT
        FROM NEXT_MIG_LOG
        ORDER BY LOG_ID
    """)
    log_rows = cur.fetchall()

    for row in log_rows:
        log_id, map_id, mig_kind, log_type, log_level, step_name, status, message, retry_cnt, created_at = row
        lines.append(
            f'INSERT INTO NEXT_MIG_LOG (LOG_ID, MAP_ID, MIG_KIND, LOG_TYPE, LOG_LEVEL,'
            f' STEP_NAME, STATUS, MESSAGE, RETRY_COUNT, CREATED_AT) VALUES ('
            f'{to_sql(log_id)}, {to_sql(map_id)}, {to_sql(mig_kind)}, {to_sql(log_type)},'
            f' {to_sql(log_level)}, {to_sql(step_name)}, {to_sql(status)},'
            f' {to_sql(message)}, {to_sql(retry_cnt)}, {to_sql(created_at)});'
        )

    lines.append('COMMIT;')
    lines.append('')

    # ----------------------------------------------------------------
    # 5. 시퀀스
    # ----------------------------------------------------------------
    lines += [
        '-- ============================================================',
        '-- 5. CREATE SEQUENCES',
        '-- ============================================================',
        '',
    ]

    cur.execute("""
        SELECT SEQUENCE_NAME, MIN_VALUE, MAX_VALUE, INCREMENT_BY, LAST_NUMBER, CYCLE_FLAG, CACHE_SIZE
        FROM USER_SEQUENCES
        WHERE SEQUENCE_NAME IN ('MAPPING_RULES_SEQ','MAPPING_RULE_DETAIL_SEQ','MAP_DTL_SEQ','MIGRATION_LOG_SEQ')
        ORDER BY SEQUENCE_NAME
    """)
    for sname, minv, maxv, incr, last, cyc, cache in cur.fetchall():
        maxv_str = str(maxv) if maxv < 10**28 else 'NOMAXVALUE'
        cache_str = f'CACHE {cache}' if cache > 1 else 'NOCACHE'
        cyc_str = 'CYCLE' if cyc == 'Y' else 'NOCYCLE'
        lines += [
            f'CREATE SEQUENCE {sname}',
            f'    START WITH  {last}',
            f'    INCREMENT BY {incr}',
            f'    MINVALUE    {minv}',
            f'    MAXVALUE    {maxv_str}',
            f'    {cache_str} {cyc_str};',
            '/',
            '',
        ]

    # ----------------------------------------------------------------
    # 푸터
    # ----------------------------------------------------------------
    lines += [
        '-- ============================================================',
        f'-- 완료: NEXT_MIG_INFO({len(info_rows)}건)'
        f' | NEXT_MIG_INFO_DTL({len(dtl_rows)}건)'
        f' | NEXT_MIG_LOG({len(log_rows)}건)',
        '-- ============================================================',
    ]

    output_path = 'META_MIGRATION_TO_21C.sql'
    content = '\n'.join(lines)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(content)

    print(f'생성 완료: {output_path}')
    print(f'  NEXT_MIG_INFO    : {len(info_rows)}건')
    print(f'  NEXT_MIG_INFO_DTL: {len(dtl_rows)}건')
    print(f'  NEXT_MIG_LOG     : {len(log_rows)}건')
    print(f'  총 {len(lines)}줄')
    conn.close()


if __name__ == '__main__':
    main()
