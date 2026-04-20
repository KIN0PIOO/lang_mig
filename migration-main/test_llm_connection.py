import os
from app.agent.llm_client import get_client
from dotenv import load_dotenv

# .env 로드
load_dotenv()

def test_connection():
    print("--- OpenAI 호환 모델 연결 테스트 시작 ---")
    try:
        client = get_client()
        model_name = os.getenv("LLM_MODEL") or "gpt-4o-mini"
        
        response = client.chat.completions.create(
            model=model_name,
            messages=[{"role": "user", "content": "Hello, respond with {'status': 'ok'} in JSON."}],
            response_format={"type": "json_object"}
        )
        print(f"응답 결과: {response.choices[0].message.content}")
        print("연결 성공!")
    except Exception as e:
        print(f"연결 실패: {e}")

if __name__ == "__main__":
    test_connection()
