import oracledb
import sys
import os

# 모듈 경로 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.core.db import get_connection

# HR 소스 테이블 스키마 접두어 설정
# - 로컬(11.2 XE): HR 계정이 별도 존재 → "HR."
# - 회사 DB(21c): SCOTT 단독 스키마 → "" (빈 문자열)
_HR_OWNER = os.getenv("HR_SCHEMA_PREFIX", "HR")
HR = f"{_HR_OWNER}." if _HR_OWNER else ""


def _h(sql: str) -> str:
    """SQL 문자열 내 'HR.' 접두어를 환경 변수에 맞게 치환합니다.
    - 로컬(기본): HR_SCHEMA_PREFIX=HR  → 'HR.' 유지
    - 회사 DB:    HR_SCHEMA_PREFIX=''  → 'HR.' 제거 (SCOTT 단독 스키마)
    """
    return sql.replace("HR.", HR)


def create_infrastructure(cursor):
    """마이그레이션 에이전트에 필요한 메타데이터 테이블 및 시퀀스를 생성합니다."""
    print("Dropping existing infrastructure tables...")
    tables = ['MAPPING_RULE_DETAIL', 'NEXT_MIG_INFO_DTL', 'NEXT_MIG_LOG', 'NEXT_MIG_INFO', 'MAP_DTL', 'MIGRATION_LOG', 'MAPPING_RULES']
    for table in tables:
        try:
            cursor.execute(f"DROP TABLE {table} CASCADE CONSTRAINTS")
            print(f"Dropped table {table}")
        except oracledb.DatabaseError:
            pass

    print("Creating infrastructure tables and sequences...")

    try:
        cursor.execute("""
            CREATE TABLE NEXT_MIG_INFO (
                MAP_ID NUMBER PRIMARY KEY,
                MAP_TYPE VARCHAR2(20),
                FR_TABLE VARCHAR2(200),
                TO_TABLE VARCHAR2(200),
                USE_YN CHAR(1) DEFAULT 'Y',
                TARGET_YN VARCHAR2(100),
                PRIORITY NUMBER,
                MIG_SQL CLOB,
                VERIFY_SQL CLOB,
                STATUS VARCHAR2(20),
                BATCH_CNT NUMBER DEFAULT 0,
                CORRECT_SQL CLOB,
                USER_EDITED CHAR(1) DEFAULT 'N',
                UPD_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                ELAPSED_SECONDS NUMBER DEFAULT 0,
                RETRY_COUNT NUMBER DEFAULT 0,
                CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("Created table NEXT_MIG_INFO")
    except oracledb.DatabaseError as e:
        if "ORA-00955" in str(e):
            print("Table NEXT_MIG_INFO already exists.")
        else:
            raise e

    try:
        cursor.execute("""
            CREATE TABLE NEXT_MIG_INFO_DTL (
                MAP_DTL_ID NUMBER PRIMARY KEY,
                MAP_ID NUMBER,
                FR_COL VARCHAR2(200),
                TO_COL VARCHAR2(200),
                CONSTRAINT FK_MAPPING_RULE FOREIGN KEY (MAP_ID)
                    REFERENCES NEXT_MIG_INFO(MAP_ID) ON DELETE CASCADE
            )
        """)
        print("Created table NEXT_MIG_INFO_DTL")
    except oracledb.DatabaseError as e:
        if "ORA-00955" in str(e):
            print("Table NEXT_MIG_INFO_DTL already exists.")
        else:
            raise e

    try:
        cursor.execute("""
            CREATE TABLE NEXT_MIG_LOG (
                LOG_ID NUMBER PRIMARY KEY,
                MAP_ID NUMBER,
                MIG_KIND VARCHAR2(20),
                LOG_TYPE VARCHAR2(20),
                LOG_LEVEL VARCHAR2(20),
                STEP_NAME VARCHAR2(50),
                STATUS VARCHAR2(20),
                MESSAGE VARCHAR2(4000),
                RETRY_COUNT NUMBER DEFAULT 0,
                CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("Created table NEXT_MIG_LOG")
    except oracledb.DatabaseError as e:
        if "ORA-00955" in str(e):
            print("Table NEXT_MIG_LOG already exists.")
        else:
            raise e


def reset_sequences(cursor):
    """시퀀스를 삭제 후 재생성하여 1번부터 시작하게 함"""
    seqs = ['MAPPING_RULES_SEQ', 'MAP_DTL_SEQ', 'MIGRATION_LOG_SEQ']
    for seq in seqs:
        try:
            cursor.execute(f"DROP SEQUENCE {seq}")
            print(f"Dropped sequence {seq}")
        except oracledb.DatabaseError as e:
            if "ORA-02289" not in str(e):
                print(f"Warning dropping sequence {seq}: {e}")

        try:
            cursor.execute(f"CREATE SEQUENCE {seq} START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE")
            print(f"Created sequence {seq} starting with 1")
        except oracledb.DatabaseError as e:
            if "ORA-00955" in str(e):
                print(f"Sequence {seq} already exists.")
            else:
                raise e


def setup_cases():
    print("Starting Setup for HR migration Environment...")
    try:
        conn = get_connection()
        cursor = conn.cursor()

        create_infrastructure(cursor)
        reset_sequences(cursor)

        print("Cleaning up existing data...")
        cursor.execute("DELETE FROM NEXT_MIG_LOG")
        cursor.execute("DELETE FROM NEXT_MIG_INFO_DTL")
        cursor.execute("DELETE FROM NEXT_MIG_INFO")

        target_tables = [
            'TGT_EMP',
            'TGT_DEPT',
            'TGT_EMP_JOB',
            'TGT_FAIL_ONCE',
            'TGT_FAIL_TWICE',
            'TGT_FAIL_ALWAYS',
            'TGT_BATCH_FAIL',
            'EMP_SAL_COMPLEX'
        ]
        for table in target_tables:
            try:
                cursor.execute(f"DROP TABLE {table}")
                print(f"Dropped target table {table}")
            except oracledb.DatabaseError:
                pass

        # ------------------------------------------------------------------
        # CASE 1: SIMPLE
        # HR.EMPLOYEES -> TGT_EMP
        # 사용자가 요청한 대로 SALARY 까지만 매핑
        # ------------------------------------------------------------------
        cursor.execute("SELECT MAPPING_RULES_SEQ.NEXTVAL FROM DUAL")
        map_id_1 = int(cursor.fetchone()[0])

        mig_sql_1 = """
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE TGT_EMP';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;
/
CREATE TABLE TGT_EMP (
    EMP_ID NUMBER(6) PRIMARY KEY,
    NAME VARCHAR2(50),
    EMAIL VARCHAR2(25),
    HIRE_DATE DATE,
    JOB_ID VARCHAR2(10),
    SALARY NUMBER(8,2)
);
/
INSERT INTO TGT_EMP (
    EMP_ID, NAME, EMAIL, HIRE_DATE, JOB_ID, SALARY
)
SELECT
    EMPLOYEE_ID,
    FIRST_NAME || ' ' || LAST_NAME,
    EMAIL,
    HIRE_DATE,
    JOB_ID,
    SALARY
FROM HR.EMPLOYEES
""".strip()

        verify_sql_1 = """
SELECT
    ABS(NVL(src_cnt.total_rows, 0) - NVL(tgt_cnt.total_rows, 0)) +
    ABS(NVL(src_emp.not_null_cnt, 0) - NVL(tgt_emp.not_null_cnt, 0)) +
    ABS(NVL(src_name.not_null_cnt, 0) - NVL(tgt_name.not_null_cnt, 0)) +
    ABS(NVL(src_email.not_null_cnt, 0) - NVL(tgt_email.not_null_cnt, 0)) +
    ABS(NVL(src_hire.not_null_cnt, 0) - NVL(tgt_hire.not_null_cnt, 0)) +
    ABS(NVL(src_job.not_null_cnt, 0) - NVL(tgt_job.not_null_cnt, 0)) +
    ABS(NVL(src_sal.not_null_cnt, 0) - NVL(tgt_sal.not_null_cnt, 0)) AS DIFF
FROM
    (SELECT COUNT(*) AS total_rows FROM HR.EMPLOYEES) src_cnt,
    (SELECT COUNT(*) AS total_rows FROM TGT_EMP) tgt_cnt,
    (SELECT COUNT(EMPLOYEE_ID) AS not_null_cnt FROM HR.EMPLOYEES) src_emp,
    (SELECT COUNT(EMP_ID) AS not_null_cnt FROM TGT_EMP) tgt_emp,
    (SELECT COUNT(FIRST_NAME || ' ' || LAST_NAME) AS not_null_cnt FROM HR.EMPLOYEES) src_name,
    (SELECT COUNT(NAME) AS not_null_cnt FROM TGT_EMP) tgt_name,
    (SELECT COUNT(EMAIL) AS not_null_cnt FROM HR.EMPLOYEES) src_email,
    (SELECT COUNT(EMAIL) AS not_null_cnt FROM TGT_EMP) tgt_email,
    (SELECT COUNT(HIRE_DATE) AS not_null_cnt FROM HR.EMPLOYEES) src_hire,
    (SELECT COUNT(HIRE_DATE) AS not_null_cnt FROM TGT_EMP) tgt_hire,
    (SELECT COUNT(JOB_ID) AS not_null_cnt FROM HR.EMPLOYEES) src_job,
    (SELECT COUNT(JOB_ID) AS not_null_cnt FROM TGT_EMP) tgt_job,
    (SELECT COUNT(SALARY) AS not_null_cnt FROM HR.EMPLOYEES) src_sal,
    (SELECT COUNT(SALARY) AS not_null_cnt FROM TGT_EMP) tgt_sal
""".strip()

        cursor.execute("""
            INSERT INTO NEXT_MIG_INFO (
                MAP_ID, MAP_TYPE, FR_TABLE, TO_TABLE, USE_YN, TARGET_YN,
                PRIORITY, MIG_SQL, VERIFY_SQL, STATUS
            )
            VALUES (
                :1, 'SIMPLE', :4, 'TGT_EMP', 'Y', 'Y', 1, :2, :3, ''
            )
        """, (map_id_1, _h(mig_sql_1), _h(verify_sql_1), _h('HR.EMPLOYEES')))

        emp_cols = [
            (1, 'EMPLOYEE_ID', 'EMP_ID'),
            (2, "FIRST_NAME || ' ' || LAST_NAME", 'NAME'),
            (3, 'EMAIL', 'EMAIL'),
            (4, 'HIRE_DATE', 'HIRE_DATE'),
            (5, 'JOB_ID', 'JOB_ID'),
            (6, 'SALARY', 'SALARY')
        ]
        for _, f, t in emp_cols:
            cursor.execute("""
                INSERT INTO NEXT_MIG_INFO_DTL (
                    MAP_DTL_ID, MAP_ID, FR_COL, TO_COL
                )
                VALUES (MAP_DTL_SEQ.NEXTVAL, :1, :2, :3)
            """, (map_id_1, f, t))
        print(f"Case 1 (HR.EMPLOYEES) added. MAP_ID: {map_id_1}")

        # ------------------------------------------------------------------
        # CASE 2: 기존 DEPT 유지
        # ------------------------------------------------------------------
        cursor.execute("SELECT MAPPING_RULES_SEQ.NEXTVAL FROM DUAL")
        map_id_2 = int(cursor.fetchone()[0])
        cursor.execute("""
            INSERT INTO NEXT_MIG_INFO (
                MAP_ID, MAP_TYPE, FR_TABLE, TO_TABLE, USE_YN, TARGET_YN, PRIORITY, STATUS
            )
            VALUES (:1, 'SIMPLE', 'DEPT', 'TGT_DEPT', 'Y', 'Y', 2, '')
        """, (map_id_2,))

        dept_cols = [
            (1, 'DEPTNO', 'DEPT_ID'),
            (2, 'DNAME', 'DEPT_NAME'),
            (3, 'LOC', 'LOCATION')
        ]
        for _, f, t in dept_cols:
            cursor.execute("""
                INSERT INTO NEXT_MIG_INFO_DTL (
                    MAP_DTL_ID, MAP_ID, FR_COL, TO_COL
                )
                VALUES (MAP_DTL_SEQ.NEXTVAL, :1, :2, :3)
            """, (map_id_2, f, t))
        print(f"Case 2 (DEPT) added. MAP_ID: {map_id_2}")

        # ------------------------------------------------------------------
        # CASE 3: COMPLEX
        # HR.EMPLOYEES + HR.JOBS JOIN
        # 직접 실행 가능한 MIG_SQL / VERIFY_SQL 넣어서 실패 가능성 줄임
        # ------------------------------------------------------------------
        cursor.execute("SELECT MAPPING_RULES_SEQ.NEXTVAL FROM DUAL")
        map_id_3 = int(cursor.fetchone()[0])

        mig_sql_3 = """
BEGIN
   EXECUTE IMMEDIATE 'DROP TABLE TGT_EMP_JOB';
EXCEPTION
   WHEN OTHERS THEN
      IF SQLCODE != -942 THEN
         RAISE;
      END IF;
END;
/
CREATE TABLE TGT_EMP_JOB (
    EMP_ID NUMBER(6),
    EMP_NAME VARCHAR2(50),
    JOB_TITLE VARCHAR2(35),
    SALARY NUMBER(8,2)
);
/
INSERT INTO TGT_EMP_JOB (
    EMP_ID, EMP_NAME, JOB_TITLE, SALARY
)
SELECT
    E.EMPLOYEE_ID,
    E.FIRST_NAME || ' ' || E.LAST_NAME,
    J.JOB_TITLE,
    E.SALARY
FROM HR.EMPLOYEES E
JOIN HR.JOBS J
    ON E.JOB_ID = J.JOB_ID
""".strip()

        verify_sql_3 = """
SELECT
    ABS(NVL(src_cnt.total_rows, 0) - NVL(tgt_cnt.total_rows, 0)) +
    ABS(NVL(src_emp.not_null_cnt, 0) - NVL(tgt_emp.not_null_cnt, 0)) +
    ABS(NVL(src_name.not_null_cnt, 0) - NVL(tgt_name.not_null_cnt, 0)) +
    ABS(NVL(src_job_title.not_null_cnt, 0) - NVL(tgt_job_title.not_null_cnt, 0)) +
    ABS(NVL(src_sal.not_null_cnt, 0) - NVL(tgt_sal.not_null_cnt, 0)) AS DIFF
FROM
    (
        SELECT COUNT(*) AS total_rows
        FROM HR.EMPLOYEES E
        JOIN HR.JOBS J ON E.JOB_ID = J.JOB_ID
    ) src_cnt,
    (
        SELECT COUNT(*) AS total_rows
        FROM TGT_EMP_JOB
    ) tgt_cnt,
    (
        SELECT COUNT(E.EMPLOYEE_ID) AS not_null_cnt
        FROM HR.EMPLOYEES E
        JOIN HR.JOBS J ON E.JOB_ID = J.JOB_ID
    ) src_emp,
    (
        SELECT COUNT(EMP_ID) AS not_null_cnt
        FROM TGT_EMP_JOB
    ) tgt_emp,
    (
        SELECT COUNT(E.FIRST_NAME || ' ' || E.LAST_NAME) AS not_null_cnt
        FROM HR.EMPLOYEES E
        JOIN HR.JOBS J ON E.JOB_ID = J.JOB_ID
    ) src_name,
    (
        SELECT COUNT(EMP_NAME) AS not_null_cnt
        FROM TGT_EMP_JOB
    ) tgt_name,
    (
        SELECT COUNT(J.JOB_TITLE) AS not_null_cnt
        FROM HR.EMPLOYEES E
        JOIN HR.JOBS J ON E.JOB_ID = J.JOB_ID
    ) src_job_title,
    (
        SELECT COUNT(JOB_TITLE) AS not_null_cnt
        FROM TGT_EMP_JOB
    ) tgt_job_title,
    (
        SELECT COUNT(E.SALARY) AS not_null_cnt
        FROM HR.EMPLOYEES E
        JOIN HR.JOBS J ON E.JOB_ID = J.JOB_ID
    ) src_sal,
    (
        SELECT COUNT(SALARY) AS not_null_cnt
        FROM TGT_EMP_JOB
    ) tgt_sal
""".strip()

        cursor.execute("""
            INSERT INTO NEXT_MIG_INFO (
                MAP_ID, MAP_TYPE, FR_TABLE, TO_TABLE, USE_YN, TARGET_YN,
                PRIORITY, MIG_SQL, VERIFY_SQL, STATUS
            )
            VALUES (
                :1,
                'COMPLEX',
                :4,
                'TGT_EMP_JOB',
                'Y', 'Y', 3, :2, :3, ''
            )
        """, (map_id_3, _h(mig_sql_3), _h(verify_sql_3),
              _h('HR.EMPLOYEES E JOIN HR.JOBS J ON E.JOB_ID = J.JOB_ID')))

        join_cols = [
            (1, 'E.EMPLOYEE_ID', 'EMP_ID'),
            (2, "E.FIRST_NAME || ' ' || E.LAST_NAME", 'EMP_NAME'),
            (3, 'J.JOB_TITLE', 'JOB_TITLE'),
            (4, 'E.SALARY', 'SALARY')
        ]
        for _, f, t in join_cols:
            cursor.execute("""
                INSERT INTO NEXT_MIG_INFO_DTL (
                    MAP_DTL_ID, MAP_ID, FR_COL, TO_COL
                )
                VALUES (MAP_DTL_SEQ.NEXTVAL, :1, :2, :3)
            """, (map_id_3, f, t))
        print(f"Case 3 (HR.EMPLOYEES-HR.JOBS JOIN) added. MAP_ID: {map_id_3}")

        # ------------------------------------------------------------------
        # 스트레스 테스트 시나리오 유지
        # ------------------------------------------------------------------
        for i, tbl_name in enumerate(['FAIL_ONCE_EMP', 'FAIL_TWICE_EMP', 'FAIL_ALWAYS_EMP', 'BATCH_FAIL_EMP'], 4):
            cursor.execute("SELECT MAPPING_RULES_SEQ.NEXTVAL FROM DUAL")
            mid = int(cursor.fetchone()[0])
            cursor.execute("""
                INSERT INTO NEXT_MIG_INFO (
                    MAP_ID, MAP_TYPE, FR_TABLE, TO_TABLE, USE_YN, TARGET_YN, PRIORITY, STATUS
                )
                VALUES (:1, 'SIMPLE', :2, :3, 'Y', 'Y', :4, '')
            """, (mid, tbl_name, f'TGT_CASE_{i}', i))
            for _, f, t in emp_cols[:3]:
                cursor.execute("""
                    INSERT INTO NEXT_MIG_INFO_DTL (
                        MAP_DTL_ID, MAP_ID, FR_COL, TO_COL
                    )
                    VALUES (MAP_DTL_SEQ.NEXTVAL, :1, :2, :3)
                """, (mid, f, t))

        # ------------------------------------------------------------------
        # CASE 8: 기존 예제 유지
        # ------------------------------------------------------------------
        cursor.execute("SELECT MAPPING_RULES_SEQ.NEXTVAL FROM DUAL")
        map_id_8 = int(cursor.fetchone()[0])
        cursor.execute("""
            INSERT INTO NEXT_MIG_INFO (
                MAP_ID, MAP_TYPE, FR_TABLE, TO_TABLE, USE_YN, TARGET_YN, PRIORITY, STATUS
            )
            VALUES (
                :1,
                'COMPLEX',
                'EMP E JOIN SALGRADE S ON E.SAL BETWEEN S.LOSAL AND S.HISAL',
                'EMP_SAL_COMPLEX',
                'Y', 'Y', 8, ''
            )
        """, (map_id_8,))

        real_complex_cols = [
            (1, 'E.EMPNO', 'EMP_ID'),
            (2, 'E.ENAME', 'EMP_NAME'),
            (3, 'E.SAL', 'SALARY'),
            (4, 'S.GRADE', 'SALARY_GRADE'),
            (5, "CASE WHEN E.JOB = 'PRESIDENT' THEN 'VIP' WHEN E.JOB = 'MANAGER' THEN 'EXEC' ELSE 'STAFF' END", 'JOB_CATEGORY'),
            (6, 'NVL(E.COMM, 0)', 'COMM_FIXED')
        ]
        for _, f, t in real_complex_cols:
            cursor.execute("""
                INSERT INTO NEXT_MIG_INFO_DTL (
                    MAP_DTL_ID, MAP_ID, FR_COL, TO_COL
                )
                VALUES (MAP_DTL_SEQ.NEXTVAL, :1, :2, :3)
            """, (map_id_8, f, t))
        print(f"Case 8 (EMP-SALGRADE Complex) added. MAP_ID: {map_id_8}")

        conn.commit()
        print("All infrastructures and test cases set successfully.")

    except Exception as e:
        print(f"Error during setup_cases: {e}")
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    setup_cases()
