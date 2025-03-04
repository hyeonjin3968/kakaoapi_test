import pandas as pd
from collections import defaultdict
from datetime import datetime

def load_csv(file_path):
    """CSV 파일을 불러와 데이터프레임으로 변환"""
    print(f"Loading CSV file: {file_path}")
    df = pd.read_csv(file_path, encoding="utf-8")
    print(f"CSV file loaded successfully with {len(df)} rows.")
    return df

def convert_kakao_date(date_str):
    """KakaoTalk 날짜 형식을 변환하는 함수 (한국어 AM/PM 처리 추가)"""
    try:
        date_str = date_str.replace("오전", "AM").replace("오후", "PM")  # 한글 오전/오후 -> 영어
        return datetime.strptime(date_str, "%Y-%m-%d %p %I:%M:%S").date()
    except ValueError:
        print(f"Warning: Failed to convert date {date_str}")
        return None

def preprocess_data(df):
    """날짜 변환 및 필터링을 적용하여 데이터 정리"""
    print("Preprocessing data: Converting date format...")
    df["Date"] = df["Date"].astype(str).apply(convert_kakao_date)
    df["Date"] = df["Date"].ffill()  # 날짜가 없는 경우 위의 값으로 채우기
    return df

def filter_multiple_keywords(df, keywords):
    """여러 개의 키워드를 필터링하고 오픈채팅봇 메시지는 제외"""
    print(f"Filtering messages with keywords {keywords}...")
    keyword_pattern = '|'.join(keywords)
    df_filtered = df[
        (df["Message"].str.contains(keyword_pattern, na=False, case=False))
        & (df["User"] != "오픈채팅봇")
    ]
    print(f"Found {len(df_filtered)} messages containing keywords {keywords}.")
    return df_filtered

def count_keyword_mentions(df):
    """사용자별 하루에 한 번만 키워드 사용을 카운트하고 날짜별로 행을 추가"""
    print("Counting unique users mentioning the keywords per day...")
    keyword_count = defaultdict(set)
    for _, row in df.iterrows():
        date, user = row["Date"], row["User"]
        if date and user:
            keyword_count[user].add(date)

    all_dates = sorted(df["Date"].dropna().unique())
    users = sorted(set(df["User"].dropna().unique()))

    df_result = pd.DataFrame(index=users, columns=all_dates, data=0)

    for user, dates in keyword_count.items():
        for date in dates:
            df_result.at[user, date] = 1

    df_result.reset_index(inplace=True)
    df_result.rename(columns={"index": "User"}, inplace=True)

    # 사용자별 총합 추가
    df_result["Total"] = df_result.drop(columns=["User"]).sum(axis=1)

    return df_result

if __name__ == "__main__":
    file_path = "chatdata_0228.csv"  # CSV 파일 경로
    keywords = input("분석할 키워드를 쉼표로 구분하여 입력하세요: ").split(',')
    keywords = [k.strip() for k in keywords]

    print(f"Starting analysis for keywords: {keywords}")
    df = load_csv(file_path)
    df = preprocess_data(df)
    df_filtered = filter_multiple_keywords(df, keywords)

    if not df_filtered.empty:
        start_date = df_filtered["Date"].min()
        end_date = datetime.now().date()

        # "오늘 날짜 이하"만 포함 → 오늘까지
        df_filtered = df_filtered[df_filtered["Date"] <= end_date]

        print(f"인증 카운트 기간: {start_date} ~ {end_date} (오늘까지 포함)")

        # 집계
        df_summary = count_keyword_mentions(df_filtered)

        # 숫자 랭크(Rank_num) 부여 (Total 많은 순 → Rank_num 작음)
        df_summary["Rank_num"] = df_summary["Total"].rank(method="dense", ascending=False).astype(int)
        
        # Rank_num 기준 오름차순 정렬 (1위가 맨 위)
        df_summary = df_summary.sort_values(by="Rank_num", ascending=True)

        # '/사이' 전까지만 User 표시 (옵션)
        df_summary["User"] = df_summary["User"].astype(str).apply(lambda u: u.split('/')[0])

        # 숫자 랭크를 텍스트로
        df_summary["Rank"] = df_summary["Rank_num"].astype(str)
        df_summary.loc[df_summary["Rank"] == "1", "Rank"] = "1위 👑👑👑"
        df_summary.loc[df_summary["Rank"] == "2", "Rank"] = "2위 👑👑"
        df_summary.loc[df_summary["Rank"] == "3", "Rank"] = "3위 👑"
        df_summary.loc[df_summary["Rank"] == "4", "Rank"] = "4위"
        df_summary.loc[df_summary["Rank"] == "5", "Rank"] = "5위"

        # 5위 이후는 "-"
        df_summary.loc[~df_summary["Rank"].isin(
            ["1위 👑👑👑", "2위 👑👑", "3위 👑", "4위", "5위"]
        ), "Rank"] = "-"

        # 보기 편하게 Rank 열을 맨 앞으로 이동
        df_summary.insert(0, "Rank", df_summary.pop("Rank"))

        # 필요 없으면 Rank_num 제거
        df_summary.drop(columns=["Rank_num"], inplace=True)

        # 결과 출력 (상위 10명 예시)
        print("\n🔹 인증 카운트 정렬 결과 (상위 10명)")
        print(df_summary.head(10))

        # CSV 저장
        df_summary.to_csv("keyword_usage_count.csv", index=False, encoding="utf-8")
        print("✅ 결과가 'keyword_usage_count.csv'에 저장되었습니다.")
    else:
        print("키워드가 포함된 메시지가 없습니다.")
        exit()
