"""HR 스키마 전체를 Oracle 21c로 이관하는 SQL 스크립트 생성기"""
import sys
import datetime

sys.path.insert(0, 'migration-main')
from app.core.db import get_connection


def to_sql_literal(val):
    """Python 값을 Oracle SQL 리터럴 문자열로 변환"""
    if val is None:
        return 'NULL'
    if isinstance(val, datetime.datetime):
        return f"TO_DATE('{val.strftime('%Y-%m-%d %H:%M:%S')}', 'YYYY-MM-DD HH24:MI:SS')"
    if isinstance(val, datetime.date):
        return f"TO_DATE('{val.strftime('%Y-%m-%d')}', 'YYYY-MM-DD')"
    if isinstance(val, (int, float)):
        return str(val)
    return "'" + str(val).replace("'", "''") + "'"


def main():
    conn = get_connection()
    cur = conn.cursor()
    lines = []

    # ----------------------------------------------------------------
    # 헤더
    # ----------------------------------------------------------------
    lines += [
        '-- ============================================================',
        '-- HR Schema Migration: Oracle 11.2 XE -> Oracle 21c',
        '-- 생성일: 2026-04-15',
        '-- 대상: REGIONS, COUNTRIES, LOCATIONS, JOBS,',
        '--        DEPARTMENTS, EMPLOYEES, JOB_HISTORY (7개 테이블)',
        '-- 실행 계정: HR 스키마 소유자 또는 DBA',
        '-- 전략: FK 없이 테이블 생성 -> 데이터 입력 -> FK 추가',
        '--   (DEPARTMENTS<->EMPLOYEES 순환 FK 문제 해결)',
        '-- ============================================================',
        '',
        '-- [사전 준비] HR 스키마가 없으면 아래 주석 해제 후 DBA 계정으로 실행',
        '-- CREATE USER HR IDENTIFIED BY "비밀번호"',
        '--   DEFAULT TABLESPACE USERS TEMPORARY TABLESPACE TEMP;',
        '-- GRANT CONNECT, RESOURCE TO HR;',
        '-- GRANT UNLIMITED TABLESPACE TO HR;',
        '',
        '-- 기존 테이블이 있다면 삭제 (FK 참조 역순)',
    ]
    for t in ['JOB_HISTORY', 'EMPLOYEES', 'DEPARTMENTS', 'LOCATIONS', 'COUNTRIES', 'JOBS', 'REGIONS']:
        lines.append(f"BEGIN EXECUTE IMMEDIATE 'DROP TABLE {t} CASCADE CONSTRAINTS PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;")
        lines.append('/')
    lines.append('')

    # ----------------------------------------------------------------
    # 1. CREATE TABLE (FK 없이)
    # ----------------------------------------------------------------
    lines += [
        '-- ============================================================',
        '-- 1. CREATE TABLES (FK 제약 없이 생성)',
        '-- ============================================================',
        '',
    ]

    tables_ddl = [
        ('REGIONS', """CREATE TABLE REGIONS (
    REGION_ID    NUMBER           NOT NULL,
    REGION_NAME  VARCHAR2(25),
    CONSTRAINT REGIONS_PK PRIMARY KEY (REGION_ID)
)"""),
        ('COUNTRIES', """CREATE TABLE COUNTRIES (
    COUNTRY_ID    CHAR(2)         NOT NULL,
    COUNTRY_NAME  VARCHAR2(40),
    REGION_ID     NUMBER,
    CONSTRAINT COUNTRIES_PK PRIMARY KEY (COUNTRY_ID)
)"""),
        ('LOCATIONS', """CREATE TABLE LOCATIONS (
    LOCATION_ID     NUMBER(4)      NOT NULL,
    STREET_ADDRESS  VARCHAR2(40),
    POSTAL_CODE     VARCHAR2(12),
    CITY            VARCHAR2(30)   NOT NULL,
    STATE_PROVINCE  VARCHAR2(25),
    COUNTRY_ID      CHAR(2),
    CONSTRAINT LOCATIONS_PK PRIMARY KEY (LOCATION_ID)
)"""),
        ('JOBS', """CREATE TABLE JOBS (
    JOB_ID     VARCHAR2(10)   NOT NULL,
    JOB_TITLE  VARCHAR2(35)   NOT NULL,
    MIN_SALARY NUMBER(6),
    MAX_SALARY NUMBER(6),
    CONSTRAINT JOBS_PK PRIMARY KEY (JOB_ID)
)"""),
        ('DEPARTMENTS', """CREATE TABLE DEPARTMENTS (
    DEPARTMENT_ID    NUMBER(4)     NOT NULL,
    DEPARTMENT_NAME  VARCHAR2(30)  NOT NULL,
    MANAGER_ID       NUMBER(6),
    LOCATION_ID      NUMBER(4),
    CONSTRAINT DEPARTMENTS_PK PRIMARY KEY (DEPARTMENT_ID)
)"""),
        ('EMPLOYEES', """CREATE TABLE EMPLOYEES (
    EMPLOYEE_ID    NUMBER(6)     NOT NULL,
    FIRST_NAME     VARCHAR2(20),
    LAST_NAME      VARCHAR2(25)  NOT NULL,
    EMAIL          VARCHAR2(25)  NOT NULL,
    PHONE_NUMBER   VARCHAR2(20),
    HIRE_DATE      DATE          NOT NULL,
    JOB_ID         VARCHAR2(10)  NOT NULL,
    SALARY         NUMBER(8,2),
    COMMISSION_PCT NUMBER(2,2),
    MANAGER_ID     NUMBER(6),
    DEPARTMENT_ID  NUMBER(4),
    CONSTRAINT EMPLOYEES_PK    PRIMARY KEY (EMPLOYEE_ID),
    CONSTRAINT EMPLOYEES_EMAIL UNIQUE      (EMAIL)
)"""),
        ('JOB_HISTORY', """CREATE TABLE JOB_HISTORY (
    EMPLOYEE_ID    NUMBER(6)     NOT NULL,
    START_DATE     DATE          NOT NULL,
    END_DATE       DATE          NOT NULL,
    JOB_ID         VARCHAR2(10)  NOT NULL,
    DEPARTMENT_ID  NUMBER(4),
    CONSTRAINT JOB_HISTORY_PK PRIMARY KEY (EMPLOYEE_ID, START_DATE)
)"""),
    ]

    for tbl, ddl in tables_ddl:
        lines.append(ddl + ';')
        lines.append('/')
        lines.append('')

    # ----------------------------------------------------------------
    # 2. INSERT DATA
    # ----------------------------------------------------------------
    lines += [
        '-- ============================================================',
        '-- 2. INSERT DATA',
        '-- ============================================================',
        '',
    ]

    insert_order = ['REGIONS', 'COUNTRIES', 'LOCATIONS', 'JOBS', 'DEPARTMENTS', 'EMPLOYEES', 'JOB_HISTORY']
    total_rows = 0

    for tbl in insert_order:
        cur.execute(f'SELECT * FROM HR.{tbl}')
        col_names = [d[0] for d in cur.description]
        rows = cur.fetchall()
        total_rows += len(rows)
        lines.append(f'-- {tbl} ({len(rows)}건)')
        for row in rows:
            vals = ', '.join(to_sql_literal(v) for v in row)
            cols = ', '.join(col_names)
            lines.append(f'INSERT INTO {tbl} ({cols}) VALUES ({vals});')
        lines.append('COMMIT;')
        lines.append('')

    # ----------------------------------------------------------------
    # 3. FK 제약조건 추가
    # ----------------------------------------------------------------
    lines += [
        '-- ============================================================',
        '-- 3. ADD FOREIGN KEY CONSTRAINTS',
        '-- (데이터 입력 후 추가 - 순환 FK 문제 해결)',
        '-- ============================================================',
        '',
    ]

    fk_list = [
        ('COUNTRIES',   'FK_COUNTRIES_REGION',    'REGION_ID',     'REGIONS(REGION_ID)'),
        ('LOCATIONS',   'FK_LOCATIONS_COUNTRY',   'COUNTRY_ID',    'COUNTRIES(COUNTRY_ID)'),
        ('DEPARTMENTS', 'FK_DEPT_LOCATION',        'LOCATION_ID',   'LOCATIONS(LOCATION_ID)'),
        ('EMPLOYEES',   'FK_EMP_JOB',              'JOB_ID',        'JOBS(JOB_ID)'),
        ('EMPLOYEES',   'FK_EMP_DEPT',             'DEPARTMENT_ID', 'DEPARTMENTS(DEPARTMENT_ID)'),
        ('EMPLOYEES',   'FK_EMP_MANAGER',          'MANAGER_ID',    'EMPLOYEES(EMPLOYEE_ID)'),
        ('DEPARTMENTS', 'FK_DEPT_MANAGER',         'MANAGER_ID',    'EMPLOYEES(EMPLOYEE_ID)'),
        ('JOB_HISTORY', 'FK_JHIST_EMP',            'EMPLOYEE_ID',   'EMPLOYEES(EMPLOYEE_ID)'),
        ('JOB_HISTORY', 'FK_JHIST_JOB',            'JOB_ID',        'JOBS(JOB_ID)'),
        ('JOB_HISTORY', 'FK_JHIST_DEPT',           'DEPARTMENT_ID', 'DEPARTMENTS(DEPARTMENT_ID)'),
    ]

    for tbl, cname, col, ref in fk_list:
        lines.append(f'ALTER TABLE {tbl}')
        lines.append(f'    ADD CONSTRAINT {cname}')
        lines.append(f'    FOREIGN KEY ({col}) REFERENCES {ref};')
        lines.append('/')
        lines.append('')

    # ----------------------------------------------------------------
    # 4. 시퀀스
    # ----------------------------------------------------------------
    lines += [
        '-- ============================================================',
        '-- 4. CREATE SEQUENCES',
        '-- ============================================================',
        '',
    ]

    cur.execute("""
        SELECT SEQUENCE_NAME, MIN_VALUE, MAX_VALUE, INCREMENT_BY, LAST_NUMBER, CYCLE_FLAG, ORDER_FLAG, CACHE_SIZE
        FROM ALL_SEQUENCES
        WHERE SEQUENCE_OWNER = 'HR'
        ORDER BY SEQUENCE_NAME
    """)
    for row in cur.fetchall():
        sname, minv, maxv, incr, last, cyc, ordr, cache = row
        maxv_str = str(maxv) if maxv < 10**28 else 'NOMAXVALUE'
        cyc_str = 'CYCLE' if cyc == 'Y' else 'NOCYCLE'
        ord_str = 'ORDER' if ordr == 'Y' else 'NOORDER'
        lines += [
            f'CREATE SEQUENCE {sname}',
            f'    START WITH  {last}',
            f'    INCREMENT BY {incr}',
            f'    MINVALUE    {minv}',
            f'    MAXVALUE    {maxv_str}',
            f'    CACHE       {cache}',
            f'    {cyc_str} {ord_str};',
            '/',
            '',
        ]

    # ----------------------------------------------------------------
    # 푸터
    # ----------------------------------------------------------------
    lines += [
        '-- ============================================================',
        f'-- 완료: 7개 테이블, 총 {total_rows}건 데이터, FK 10개, 시퀀스 3개',
        '-- REGIONS:4  COUNTRIES:25  LOCATIONS:23  JOBS:19',
        '-- DEPARTMENTS:27  EMPLOYEES:107  JOB_HISTORY:10',
        '-- ============================================================',
    ]

    sql_content = '\n'.join(lines)
    output_path = 'HR_MIGRATION_TO_21C.sql'
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(sql_content)

    print(f'생성 완료: {output_path}')
    print(f'총 {len(lines)}줄, 데이터 {total_rows}건')
    conn.close()


if __name__ == '__main__':
    main()
