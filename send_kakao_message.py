import pandas as pd
import time
import schedule
import requests

# CSV 파일 경로
csv_file = '/Users/vanessa/Desktop/HJ/keyword_counter/keyword_usage_count.csv'

# 카카오톡 API URL (카카오톡 채널로 메시지 전송)
KAKAO_API_URL = "https://kapi.kakao.com/v2/api/talk/memo/default/send"
access_token = 'LthwdrTfZKHywn-WPvlS21JNM3dahWpyAAAAAQo8IlIAAAGVAGVkt8c_xW4TVk05'  # 실제 받은 액세스 토큰

# CSV 데이터 읽기
def read_csv():
    df = pd.read_csv(csv_file)
    return df

# #인증 키워드 합산 결과 계산
def calculate_level(total_mentions):
    if total_mentions <= 10:
        return 1
    elif total_mentions <= 30:
        return 2
    elif total_mentions <= 50:
        return 3
    else:
        return 4

# 메시지 형식 만들기
def format_message(df):
    message = "📢 오늘의 인증 결과 📢\n\n"
    for index, row in df.iterrows():
        user_name = row['User'].split('/')[0]  # '/' 앞부분만 이름으로 추출
        total_mentions = row['Total']  # 총 언급 횟수
        level = calculate_level(total_mentions)  # 레벨 계산
        message += f"{user_name} - {total_mentions}회 📚 (레벨 {level})\n"
    return message

# 카카오톡 메시지 전송 함수
def send_kakao_message(message):
    message_data = {
        "object_type": "text",
        "text": message,
        "link": {
            "web_url": "https://www.kakao.com"
        },
        "button_title": "자세히 보기"
    }

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    response = requests.post(KAKAO_API_URL, headers=headers, json=message_data)

    if response.status_code == 200:
        print("✅ 카카오톡 메시지가 성공적으로 전송되었습니다!")
    else:
        print(f"❌ 메시지 전송 실패: {response.text}")

# 자동화된 작업을 매일 11:59 PM에 실행
def job():
    df = read_csv()
    message = format_message(df)
    send_kakao_message(message)

# 매일 11:59 PM에 실행되도록 스케줄 설정
schedule.every(1).minute.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)

schedule.every().day.at("23:59").do(job)

# 프로그램이 종료되지 않도록 계속 실행
while True:
    schedule.run_pending()
    time.sleep(1)
