import pandas as pd
from collections import defaultdict
from datetime import datetime, date, timedelta

def load_csv(file_path):
    """CSV 파일을 불러와 데이터프레임으로 변환"""
    print(f"Loading CSV file: {file_path}")
    df = pd.read_csv(file_path, encoding="utf-8")
    print(f"CSV file loaded successfully with {len(df)} rows.")
    return df

def convert_kakao_date(date_str):
    """KakaoTalk 날짜 형식을 변환하는 함수 (한글 오전/오후 -> 영어 AM/PM)"""
    try:
        date_str = date_str.replace("오전", "AM").replace("오후", "PM")
        return datetime.strptime(date_str, "%Y-%m-%d %p %I:%M:%S").date()
    except ValueError:
        # 변환 실패 시 None 반환
        print(f"Warning: Failed to convert date {date_str}")
        return None

def preprocess_data(df):
    """날짜 전처리: 문자열 -> datetime.date, 결측치 채우기"""
    print("Preprocessing data: Converting date format...")
    df["Date"] = df["Date"].astype(str).apply(convert_kakao_date)
    # 날짜가 없는 행이 있을 경우 위 값으로 채우기 (카카오톡 추출 시 흔히 발생)
    df["Date"] = df["Date"].ffill()
    return df

def filter_multiple_keywords(df, keywords):
    """키워드 여러 개를 OR 조건으로 필터링, '오픈채팅봇' 메시지 제외"""
    print(f"Filtering messages with keywords {keywords}...")
    keyword_pattern = '|'.join(keywords)
    df_filtered = df[
        (df["Message"].str.contains(keyword_pattern, na=False, case=False))
        & (df["User"] != "오픈채팅봇")
    ]
    print(f"Found {len(df_filtered)} messages containing keywords {keywords}.")
    return df_filtered

# ─────────────────────────────────────────────────────────────
# (A) 일자별 집계 함수
# ─────────────────────────────────────────────────────────────
def count_keyword_mentions_daily(df):
    """
    (User, Date)가 중복된 행은 1일 1회로 계산. 
    -> pivot: 행=User, 열=날짜, 값=사용(1회 이상이면 1).
    -> 값은 하루 동안 키워드를 몇 번 썼는지 count로도 가능하지만,
       여기서는 중복 제거해 '1'씩만 카운트한다고 가정.
    """
    print("Counting daily usage for each user...")
    df_unique = df.drop_duplicates(subset=["User", "Date"])
    grouped = df_unique.groupby(["User", "Date"])["Message"].count()
    df_pivot = grouped.unstack(fill_value=0)

    # 사용자별 합계
    df_pivot["Total"] = df_pivot.sum(axis=1)
    df_pivot.reset_index(inplace=True)

    new_cols = []
    for c in df_pivot.columns:
        if isinstance(c, date):
            # date -> string
            c_str = c.strftime("%Y-%m-%d")
            new_cols.append(c_str)
        else:
            # 'User', 'Total'
            new_cols.append(c)
    df_pivot.columns = new_cols
    return df_pivot
# ─────────────────────────────────────────────────────────────
# (B) '목요일 기준' 주차 계산 함수 (2025년 전용)
# ─────────────────────────────────────────────────────────────
def get_week_of_month_2025(date_obj):
    """ 
    2025년에만 적용. 
    '해당 달의 첫 목요일'을 포함하는 월~일을 1주차로 본 뒤,
    다음 달 1주차 시작 전날까지가 이 달의 주차.
    """
    if not date_obj or date_obj.year != 2025:
        return None

    y, m = date_obj.year, date_obj.month
    
    # 1) '이 달'의 첫 번째 목요일 찾기
    first_thursday = None
    for d in range(1, 8):
        tmp = date(y, m, d)
        if tmp.weekday() == 3:  # 목요일 => weekday=3
            first_thursday = tmp
            break
    if not first_thursday:
        return None

    # 1주차 시작(월요일) = 첫 목요일 - 3일
    month_week1_start = first_thursday - timedelta(days=3)

    # 2) '다음 달' 첫 목요일 찾기
    if m == 12:
        next_month_year, next_month = y + 1, 1
    else:
        next_month_year, next_month = y, m + 1

    first_thursday_next = None
    for d in range(1, 8):
        tmp = date(next_month_year, next_month, d)
        if tmp.weekday() == 3:
            first_thursday_next = tmp
            break 
    if not first_thursday_next: 
        return None

    next_month_week1_start = first_thursday_next - timedelta(days=3)
    
    # 이 달의 주차 범위: [month_week1_start, next_month_week1_start - 1]
    range_end = next_month_week1_start - timedelta(days=1)

    if date_obj < month_week1_start or date_obj > range_end:
        return None  # 범위 밖이면 None (이전 달 또는 다음 달로 처리 가능)

    # 주차 번호
    offset_days = (date_obj - month_week1_start).days
    week_num = offset_days // 7 + 1

    return f"{m}월 {week_num}주차"

# ─────────────────────────────────────────────────────────────
# (B) 목요일 기준 주차별 키워드 집계
# ─────────────────────────────────────────────────────────────
def count_keyword_mentions_by_thursday_weeks_2025(df):
    """
    (User, Date)가 중복된 행은 1일 1회로 처리.
    get_week_of_month_2025() -> '몇 월 몇 주차' 구해서 pivot.
    """
    print("Counting weekly usage (Thursday-based) for each user in 2025...")
    df_unique = df.drop_duplicates(subset=["User", "Date"])

    # 주차 컬럼 생성
    df_unique["WeekGroup"] = df_unique["Date"].apply(get_week_of_month_2025)
    # 2025년이 아닌 날짜, 혹은 계산 불가한 날짜는 제외
    df_unique = df_unique[df_unique["WeekGroup"].notna()]

    # (User, WeekGroup)별 개수 -> 하루 1회로 처리하므로 count()
    grouped = df_unique.groupby(["User", "WeekGroup"])["Message"].count()
    df_pivot = grouped.unstack(fill_value=0)

    # 사용자별 합계
    df_pivot["Total"] = df_pivot.sum(axis=1)

    df_pivot.reset_index(inplace=True)
    return df_pivot

# ─────────────────────────────────────────────────────────────
# 메인 실행부
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # 1) 파일 경로 & 키워드 입력 받기
    file_path = "chatdata_0305.csv"  # CSV 파일 경로
    keywords = input("분석할 키워드를 쉼표로 구분하여 입력하세요: ").split(',')
    keywords = [k.strip() for k in keywords]

    print(f"\nStarting analysis for keywords: {keywords}")
    df = load_csv(file_path)
    df = preprocess_data(df)
    df_filtered = filter_multiple_keywords(df, keywords)

    if df_filtered.empty:
        print("키워드가 포함된 메시지가 없습니다.")
        exit()

    # 2) 오늘 날짜 이전까지만 포함
    end_date = datetime.now().date()
    df_filtered = df_filtered[df_filtered["Date"] <= end_date]

    start_date = df_filtered["Date"].min()
    print(f"\n인증 카운트 기간: {start_date} ~ {end_date} (오늘까지 포함)")

    # ─────────────────────────────────────────────────────────────
    # (A) 일자별 CSV
    # ─────────────────────────────────────────────────────────────
    df_daily = count_keyword_mentions_daily(df_filtered)
    df_daily.sort_values(by="Total", ascending=False, inplace=True)  # 전체 합계 순으로 정렬(선택)
    df_daily.reset_index(drop=True, inplace=True)

    daily_csv_file = "keyword_usage_count_daily.csv"
    df_daily.to_csv(daily_csv_file, index=False, encoding="utf-8")
    print(f"✅ 일자별 집계 완료: '{daily_csv_file}' 생성")

    # ─────────────────────────────────────────────────────────────
    # (B) 목요일 기준 주차별 CSV
    # ─────────────────────────────────────────────────────────────
    df_weekly = count_keyword_mentions_by_thursday_weeks_2025(df_filtered)
    # 순위(Rank_num) 부여: Total 많은 순 → Rank_num 작음
    df_weekly["Rank_num"] = df_weekly["Total"].rank(method="dense", ascending=False).astype(int)
    df_weekly.sort_values(by="Rank_num", ascending=True, inplace=True)

    # '/사이' 전까지만 User 표시 (선택적으로)
    df_weekly["User"] = df_weekly["User"].astype(str).apply(lambda x: x.split('/')[0])

    # 숫자 랭크를 텍스트로
    df_weekly["Rank"] = df_weekly["Rank_num"].astype(str)
    df_weekly.loc[df_weekly["Rank"] == "1", "Rank"] = "1위 👑👑👑"
    df_weekly.loc[df_weekly["Rank"] == "2", "Rank"] = "2위 👑👑"
    df_weekly.loc[df_weekly["Rank"] == "3", "Rank"] = "3위 👑"
    df_weekly.loc[df_weekly["Rank"] == "4", "Rank"] = "4위"
    df_weekly.loc[df_weekly["Rank"] == "5", "Rank"] = "5위"
    df_weekly.loc[~df_weekly["Rank"].isin(
        ["1위 👑👑👑", "2위 👑👑", "3위 👑", "4위", "5위"]
    ), "Rank"] = "-"

    # Rank 열을 맨 앞으로 이동 & Rank_num 제거
    df_weekly.insert(0, "Rank", df_weekly.pop("Rank"))
    df_weekly.drop(columns=["Rank_num"], inplace=True)

    weekly_csv_file = "keyword_usage_count_weekly.csv"
    df_weekly.to_csv(weekly_csv_file, index=False, encoding="utf-8")
    print(f"✅ 주차별 집계 완료: '{weekly_csv_file}' 생성")

    # ─────────────────────────────────────────────────────────────
    # 3) "이 달의 AI 왕은?" 콘솔 출력
    # ─────────────────────────────────────────────────────────────
    #   - 사용자에게 원하는 달을 물어보고,
    #   - df_daily에서 해당 달의 칼럼들만 합산 → 최대값 찾기
    #   - 콘솔에만 보여주기
    # ─────────────────────────────────────────────────────────────
    while True:
     try:
        desired_month = input("\n원하는 달(예: 2) 또는 종료하려면 'q' 입력: ")
        if desired_month.lower() == 'q':
            print("종료합니다.")
            break

        desired_month = int(desired_month)
        if not (1 <= desired_month <= 12):
            print("월은 1부터 12 사이의 정수를 입력하세요.")
            continue

        date_cols = []
        for col in df_daily.columns:
            # 'User' / 'Total' 등의 특수 컬럼은 제외
            if col in ["User", "Total"]:
                continue

            # 문자열 컬럼을 date 형태로 변환 후, 월이 같은지 확인
            try:
                col_date = datetime.strptime(col, "%Y-%m-%d").date()
                if col_date.month == desired_month:
                    date_cols.append(col)
            except ValueError:
                pass

        if not date_cols:
            print(f"{desired_month}월에 해당하는 날짜 컬럼이 없습니다.")
            continue

        # 해당 달의 컬럼만 합산
        df_temp = df_daily[["User"] + date_cols].copy()
        df_temp["MonthSum"] = df_temp[date_cols].sum(axis=1)

        # MonthSum 기준 내림차순 정렬
        df_temp_sorted = df_temp.sort_values("MonthSum", ascending=False).reset_index(drop=True)

        # 상위 3명을 뽑아서 출력
        print(f"\n🔥이 달의 인증 왕🔥")
        print(f"{desired_month}월에 AI를 가장 많이 활용한 사람은?\n")

        if len(df_temp_sorted) >= 1:
            row1 = df_temp_sorted.iloc[0]
            name1 = row1["User"]
            count1 = int(row1["MonthSum"])
            print(f"  1위 [@{name1}]님, 총 {count1}회 인증! 축하합니다!🎉")

        if len(df_temp_sorted) >= 2:
            row2 = df_temp_sorted.iloc[1]
            name2 = row2["User"]
            count2 = int(row2["MonthSum"])
            print(f"  2위 [@{name2}]님, 총 {count2}회 인증!")

        if len(df_temp_sorted) >= 3:
            row3 = df_temp_sorted.iloc[2]
            name3 = row3["User"]
            count3 = int(row3["MonthSum"])
            print(f"  3위 [@{name3}]님, 총 {count3}회 인증!")

        print("--------------------------") 

     except ValueError:
        print("정수(월) 또는 'q'만 입력 가능합니다. 다시 시도해주세요.")
