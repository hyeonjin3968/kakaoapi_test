import requests

# 카카오톡 API URL
KAKAO_API_URL = "https://kapi.kakao.com/v2/api/talk/memo/default/send"

# 카카오톡 액세스 토큰 (카카오 개발자 사이트에서 발급받은 액세스 토큰을 입력)
KAKAO_ACCESS_TOKEN = 'YOUR_KAKAO_ACCESS_TOKEN'  # 여기에 실제 액세스 토큰을 입력

# 메시지 내용
message_data = {
    "object_type": "text",  # 메시지 형식 (텍스트)
    "text": "자동화된 메시지 테스트입니다. 성공적으로 전송되었습니다!",  # 보낼 메시지
    "link": {
        "web_url": "https://www.kakao.com"  # 링크 (선택사항)
    },
    "button_title": "자세히 보기"  # 버튼 제목 (선택사항)
} 

# 헤더 설정 (액세스 토큰을 사용해 인증)
headers = {
    "Authorization": f"Bearer {KAKAO_ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

# 카카오톡 메시지 전송
response = requests.post(KAKAO_API_URL, headers=headers, json=message_data)

# 응답 출력
if response.status_code == 200:
    print("✅ 카카오톡 메시지가 성공적으로 전송되었습니다!")
else:
    print(f"❌ 메시지 전송 실패: {response.text}")
