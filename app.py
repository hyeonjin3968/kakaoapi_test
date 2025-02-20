import os
import requests
from flask import Flask, request, jsonify
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()
KAKAO_API_KEY = os.getenv("KAKAO_REST_API_KEY")

app = Flask(__name__)

# 특정 키워드에 대한 응답 데이터 (이미지 URL)
IMAGE_RESPONSE = {
    ".pw": "https://your-image-url.com/sample.jpg"
}

# 로그 저장 함수
def save_log(user_message):
    with open("logs.txt", "a") as log_file:
        log_file.write(f"{user_message}\n")

@app.route('/message', methods=['POST'])
def message():
    data = request.get_json()
    user_message = data.get('userRequest', {}).get('utterance', '')

    # 로그 저장
    save_log(user_message)

    response_text = "등록되지 않은 명령어입니다."
    response_image = None

    if user_message in IMAGE_RESPONSE:
        response_text = "📷 요청하신 이미지입니다."
        response_image = IMAGE_RESPONSE[user_message]

    return jsonify({
        "version": "2.0",
        "template": {
            "outputs": [
                {"simpleText": {"text": response_text}}
            ] + (
                [{"simpleImage": {"imageUrl": response_image, "altText": "요청한 이미지"}}] if response_image else []
            )
        }
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
