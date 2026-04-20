import os
import json
from openai import OpenAI
from app.core.exceptions import LLMConnectionError, LLMAuthenticationError, LLMTokenLimitError
from app.core.logger import logger
from dotenv import load_dotenv

# .env 로드 (루트 경로 기준)
env_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    ".env"
)
load_dotenv(env_path)


def get_client():
    """OpenAI 호환 모델 클라이언트를 반환합니다."""
    api_key = os.getenv("OPEN_API_KEY")
    base_url = os.getenv("LLM_BASE_URL")

    if not api_key:
        error_msg = f"API Key(OPEN_API_KEY)가 설정되지 않았습니다. (Path: {env_path})"
        logger.error(f"[LLM] {error_msg}")
        raise LLMAuthenticationError(error_msg)

    # OpenAI 호환 클라이언트 초기화 (base_url이 있으면 사내 Gateway 등으로 연결)
    return OpenAI(
        api_key=api_key,
        base_url=base_url if base_url else None
    )


def _format_ddl_info(ddl_rows: list) -> str:
    """컬럼 메타데이터 튜플 리스트를 프롬프트용 텍스트로 변환합니다."""
    if not ddl_rows:
        return "  (조회된 컬럼 정보 없음)"
    lines = []
    for col_name, data_type, data_length, data_precision, data_scale, nullable in ddl_rows:
        if data_type == "NUMBER":
            if data_precision is not None and data_scale not in (None, 0):
                type_str = f"NUMBER({data_precision},{data_scale})"
            elif data_precision is not None:
                type_str = f"NUMBER({data_precision})"
            else:
                type_str = "NUMBER"
        elif data_type in ("VARCHAR2", "CHAR", "NVARCHAR2", "NCHAR") and data_length:
            type_str = f"{data_type}({data_length})"
        else:
            type_str = data_type
        null_str = "NULL" if nullable == "Y" else "NOT NULL"
        lines.append(f"  {col_name:<30} {type_str:<25} {null_str}")
    return "\n".join(lines)


def generate_sqls(NEXT_SQL_INFO, last_error=None, last_sql=None, source_ddl=None):
    """
    OpenAI 호환 API를 호출하여 Oracle 21c 마이그레이션 SQL들을 생성합니다.
    (DDL, Migration, Verification 분리)

    Args:
        NEXT_SQL_INFO: 매핑 규칙 객체
        last_error: 이전 실행 실패 에러 메시지 (재시도 시)
        last_sql: 이전 실행 실패 SQL (재시도 시)
        source_ddl: fetch_table_ddl()로 조회한 소스 테이블 컬럼 메타데이터 (읽기 전용 조회 결과)
    """
    client = get_client()
    model_name = os.getenv("LLM_MODEL") or "gpt-4o-mini"

    from_table = NEXT_SQL_INFO.fr_table
    to_table = NEXT_SQL_INFO.to_table

    # 컬럼 매핑 정보 정리
    details = NEXT_SQL_INFO.details
    mapping_info = "\n".join([f"  - {d.fr_col} -> {d.to_col}" for d in details])

    # 소스 테이블 DDL 정보 (실제 컬럼 타입/길이/제약조건)
    # source_ddl은 {table_name: [rows]} 형태의 dict
    ddl_info_block = ""
    if source_ddl and isinstance(source_ddl, dict):
        table_blocks = []
        for tbl_name, rows in source_ddl.items():
            formatted = _format_ddl_info(rows)
            table_blocks.append(
                f"    테이블: {tbl_name}\n"
                f"    {'컬럼명':<30} {'데이터타입':<25} {'NULL여부'}\n"
                f"    {'-'*70}\n"
                f"{formatted}"
            )
        ddl_info_block = f"""
    [소스 테이블 실제 DDL 정보] (ALL_TAB_COLUMNS 읽기 전용 조회 결과)
{chr(10).join(table_blocks)}

    ※ 위 DDL 정보를 타겟 테이블 생성 시 반드시 참고하여 정확한 타입을 사용하십시오.
"""

    # 프롬프트 구성 (Oracle 21c 전문가 페르소나 적용)
    prompt = f"""
    당신은 Oracle 데이터 마이그레이션 전문가입니다.
    제시된 매핑 규칙과 소스 테이블의 실제 DDL 정보를 기반으로
    (1) 타겟 테이블 생성 DDL, (2) 데이터 이관 DML, (3) 정합성 검증 SQL을 JSON 형식으로 각각 생성하십시오.

    [Oracle 버전 제약 — 반드시 준수]
    현재 환경은 Oracle 11.2 XE입니다. 아래 기능은 12c 이상 전용이므로 절대 사용하지 마십시오:
    - LATERAL JOIN 금지 → 대신 인라인 뷰(서브쿼리) 또는 WITH절(CTE) 사용
    - STANDARD_HASH() 금지 → 대신 ORA_HASH() 사용 (결과는 NUMBER, VARCHAR2 필요 시 TO_CHAR() 로 감쌀 것)
    - FETCH FIRST / OFFSET 금지 → 대신 ROWNUM 사용
    - LISTAGG ... ON OVERFLOW TRUNCATE 금지
{ddl_info_block}
    [매핑 규칙]
    - 소스 테이블: {from_table}
    - 타겟 테이블: {to_table}
    - 컬럼 매핑 정보:
{mapping_info}

    [필수 요구사항]
    1. ddl_sql (Schema Phase):
       - 타겟 테이블('{to_table}')을 생성하는 'CREATE TABLE' 문장만 포함하십시오.
       - 위 [소스 테이블 실제 DDL 정보]의 컬럼 타입/길이/NULL여부를 그대로 반영하십시오.
         (DDL 정보가 제공된 경우 타입을 임의로 추측하지 마십시오.)
       - 매핑 대상 컬럼만 타겟 테이블에 포함하십시오.
       - DROP 문장이나 TRUNCATE 문장은 포함하지 마십시오. (에이전트 로직에서 처리함)

    2. migration_sql (Migration Phase):
       - 데이터를 실제로 옮기는 'INSERT INTO ... SELECT ...' 문장만 포함하십시오.
       - **절대 주의**: 'CREATE TABLE', 'DROP TABLE', 'TRUNCATE' 등의 DDL 문장은 이 필드에 포함하지 마십시오. 오직 DML(INSERT)만 포함해야 합니다.
       - 반드시 위에서 정의한 타겟 테이블('{to_table}')과 소스 테이블('{from_table}')을 사용하십시오.
       - 소스 테이블명이 '{from_table}'이라면 이 이름 그대로 사용하십시오. 임의로 스키마 접두어를 붙이거나 제거하지 마십시오. (hallucination 주의)
       - DATE/TIMESTAMP 컬럼의 경우 소스 타입이 VARCHAR2이면 TO_DATE 변환을 적용하고, 이미 DATE/TIMESTAMP이면 변환 없이 그대로 사용하십시오.
       - FR_COL에 'c.' 등의 alias 접두어가 포함된 경우(예: c.SALARY_NUM, c.HIRE_DATE_ROUNDTRIP), 이는 동일 SELECT 내 다른 파생 컬럼을 참조하는 것입니다.
         이 경우 반드시 해당 파생 컬럼들을 내부 서브쿼리에서 먼저 계산하고 해당 alias(c 등)로 별칭을 붙인 뒤, 외부 SELECT에서 c.컬럼명으로 참조하십시오.
       - **[절대 금지] 매핑 규칙(DTL)에 없는 컬럼(예: FIRST_NAME, LAST_NAME, EMAIL 등 원본 테이블 컬럼)을 임의로 타겟 테이블에 추가하지 마십시오. (hallucination 주의)**
       - 이 단계는 테이블이 이미 생성된 상태에서 실행됩니다.

3. verification_sql (Verification Phase):
   - 소스/타겟 테이블의 데이터 정합성을 비교하는 단일 SELECT 문장을 작성하십시오.
   - 반드시 source side와 target side를 각각 집계한 후 최종 DIFF를 계산하십시오.
   - source side는 원본 테이블을 직접 집계하지 말고, migration_sql의 SELECT 결과와 동일한 논리의 내부 서브쿼리(src_base)를 먼저 생성한 뒤 집계하십시오.
   - target side는 타겟 테이블('{to_table}')에서 실제 타겟 컬럼명으로 집계하십시오.
   - CASE, CONCAT, CAST, TO_DATE, TO_CHAR, STANDARD_HASH, ROW_NUMBER, DENSE_RANK, COUNT OVER 등의 파생 컬럼은 반드시 src_base 내부에서 먼저 계산한 뒤 바깥에서 COUNT(별칭) 하십시오.
   - Oracle 제약상 window function을 COUNT(window_function(...)) 형태로 직접 사용하지 마십시오.
   - tgt alias에서는 소스 컬럼명을 사용하지 말고, 반드시 타겟 컬럼명만 사용하십시오.
   - **[절대 필수] JOIN ON 절에서도 타겟 테이블의 실제 컬럼명(TO_COL 기준)만 사용하십시오. 소스 컬럼명이나 TO_CHAR(TGT.소스컬럼) 형태는 절대 사용하지 마십시오.**
   - **[절대 필수] 최종 SELECT 결과는 반드시 'DIFF'라는 이름의 단일 컬럼 하나만 반환하십시오.**
   - SRC_COUNT, TGT_COUNT, DIFF_xxx 등 진단용 컬럼을 추가로 SELECT하지 마십시오. 오직 DIFF 하나만 출력해야 합니다.
   - DIFF는 모든 불일치 건수의 합계(ABS 포함)이며, 0이면 완벽한 일치로 간주됩니다.
   - 올바른 출력 예시: SELECT SUM(...) AS DIFF FROM ...
   - 잘못된 출력 예시: SELECT SRC_COUNT, TGT_COUNT, DIFF_COL1, DIFF_COL2, ..., DIFF FROM ...

    4. 공통:
       - DATE 컬럼 처리 시 TO_DATE 함수와 'YYYY-MM-DD HH24:MI:SS' 포맷을 사용하십시오.
       - 여러 SQL 문장이 포함될 경우 각 문장은 반드시 슬래시(/) 단독 라인으로 구분하십시오.

    응답은 반드시 'ddl_sql', 'migration_sql', 'verification_sql' 키를 가진 JSON 객체여야 합니다.
    """

    # [추가] 인간 전문가가 직접 수정한 정답 SQL이 있다면 프롬프트에 반영
    if NEXT_SQL_INFO.correct_sql:
        #logger.info(f"[LLM] map_id={NEXT_SQL_INFO.map_id} | 인간 전문가의 정답 SQL을 프롬프트에 반영합니다.")
        prompt += f"\n\n[인간 전문가가 검증한 정답 SQL 예시]\n{NEXT_SQL_INFO.correct_sql}\n"
        prompt += "- 위 예시의 패턴을 참고하여 ddl_sql, migration_sql, verification_sql로 나누어 생성하십시오.\n"

    if last_error:
        prompt += f"""
        
        [이전 실행 실패 피드백]
        - 실패한 SQL: {last_sql}
        - 발생한 에러: {last_error}
        - 작업: 위 에러를 분석하여 올바르게 수정한 쿼리들을 다시 생성하십시오.
        """

    try:
        logger.debug(f"[LLM_PROMPT] map_id={NEXT_SQL_INFO.map_id}\n{'='*60}\n{prompt}\n{'='*60}")
        response = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": "You are a helpful assistant that generates Oracle SQL in JSON format."},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"}
        )

        result = json.loads(response.choices[0].message.content)

        ddl_sql = result.get("ddl_sql", "")
        migration_sql = result.get("migration_sql", "")
        verification_sql = result.get("verification_sql", "")

        # (Post-processing) 리스트 형태일 경우 문자열로 병합
        def merge_list(val):
            if isinstance(val, list):
                return "\n/\n".join(val)
            return val

        logger.info(f"[LLM] SQL 생성 완료 (Model: {model_name})")
        return (
            merge_list(ddl_sql),
            merge_list(migration_sql),
            merge_list(verification_sql)
        )

    except Exception as e:
        logger.error(f"[LLM] API 호출 중 에러: {e}")
        # 예외 타입에 따른 세분화 처리는 필요 시 추가
        raise LLMConnectionError(f"LLM 연결 실패: {str(e)}")