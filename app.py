from flask import Flask, request, redirect
import requests

# Flask 앱 객체 정의
app = Flask(__name__)

# 카카오 로그인 인증 코드 받기
@app.route('/callback')
def callback():
    code = request.args.get('code')  # URL에서 'code' 파라미터를 가져옵니다.
    
    # 'code' 값이 제대로 전달되었는지 확인하기 위해 출력
    if code:
        print("Received Code:", code)  # 인증 코드 출력
        
        # 카카오 API로 토큰 발급 요청
        token_url = "https://kauth.kakao.com/oauth/token"
        data = {
            "grant_type": "authorization_code",
            "client_id": "d83eb81ff6dbf61d59032ea7426b5498",  # 카카오 개발자 사이트에서 받은 REST API 키
            "redirect_uri": "http://localhost:8000/callback",
            "code": code
        }
        
        response = requests.post(token_url, data=data)
        token_info = response.json()
        access_token = token_info.get('access_token')
        
        return f"Access Token: {access_token}"  # 액세스 토큰 출력
        
    else:
        return "Authorization failed, no code received."

# 서버 실행
if __name__ == '__main__':
    app.run(debug=True, port=8000)
