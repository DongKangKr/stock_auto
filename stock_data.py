"""
KRX 우선주/보통주 데이터 수집 및 KPRI 지수 계산 모듈

- 우선주-보통주 페어 자동 매칭
- 기준일(2020-01-02) 대비 상대가치 지수 계산
- KPRI-전체 / KPRI-Top20 (시총 기준) 지수
- 기여도 Top 5 / Worst 5 분석
"""

import logging
import pickle
import re
import time
from datetime import datetime, timedelta
from pathlib import Path

import FinanceDataReader as fdr
import pandas as pd
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

PREFERRED_PATTERN = re.compile(r"(2우B|1우B|3우B|우B|2우|1우|3우|우)$")
BASE_DATE = "2020-01-02"
CACHE_FILE = Path(__file__).parent / "history_cache.pkl"
DIVIDEND_CACHE_FILE = Path(__file__).parent / "dividend_cache.pkl"
TOP_N = 20

NAVER_UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


def _fetch_dividend_yield(code):
    """Naver Finance 투자정보 테이블에서 배당수익률 정확히 파싱
    - 배당 없는 종목(N/A)은 None 반환
    - 동일업종 PER 등 다른 값이 잘못 매칭되지 않도록 per_table 구조를 따름
    """
    try:
        url = f"https://finance.naver.com/item/main.naver?code={code}"
        r = requests.get(url, headers=NAVER_UA, timeout=10)
        if not r.ok:
            return None
        soup = BeautifulSoup(r.text, "html.parser")
        for table in soup.find_all("table", class_="per_table"):
            for row in table.find_all("tr"):
                th = row.find("th")
                if not th or "배당수익률" not in th.get_text():
                    continue
                td = row.find("td")
                if not td:
                    return None
                em = td.find("em")
                if em:
                    val = em.get_text(strip=True).replace(",", "")
                    try:
                        return float(val)
                    except ValueError:
                        return None
                return None
    except Exception:
        pass
    return None


def fetch_dividend_yields(codes, use_cache=True):
    """여러 종목의 배당수익률 일괄 조회 (캐시 지원, 하루 단위)"""
    today_str = datetime.now().strftime("%Y-%m-%d")

    cache = {}
    if use_cache and DIVIDEND_CACHE_FILE.exists():
        try:
            with open(DIVIDEND_CACHE_FILE, "rb") as f:
                cache = pickle.load(f)
        except Exception:
            pass

    result = {}
    to_fetch = []
    for code in codes:
        cached = cache.get(code)
        if cached and cached.get("date") == today_str:
            result[code] = cached.get("yield")
        else:
            to_fetch.append(code)

    logger.info(f"배당수익률 조회: 캐시 {len(result)}개, 신규 조회 {len(to_fetch)}개")

    for i, code in enumerate(to_fetch):
        y = _fetch_dividend_yield(code)
        result[code] = y
        cache[code] = {"date": today_str, "yield": y}
        time.sleep(0.1)
        if (i + 1) % 20 == 0:
            logger.info(f"배당수익률 진행: {i + 1}/{len(to_fetch)}")

    try:
        with open(DIVIDEND_CACHE_FILE, "wb") as f:
            pickle.dump(cache, f)
    except Exception as e:
        logger.warning(f"배당수익률 캐시 저장 실패: {e}")

    return result


# ════════════════════════════════════════════════════════
# 페어 수집
# ════════════════════════════════════════════════════════

def fetch_pairs():
    """현재 KRX 상장 우선주-보통주 페어 조회"""
    logger.info("=== KRX 종목 리스트 조회 ===")
    df = fdr.StockListing("KRX")
    logger.info(f"전체 종목 수: {len(df)}")

    all_stocks = {}
    preferred_list = []

    for _, row in df.iterrows():
        code = row["Code"]
        name = row["Name"]
        if not name or not code:
            continue

        all_stocks[code] = {
            "name": name,
            "close": int(row["Close"]) if row["Close"] else 0,
            "volume": int(row["Volume"]) if row["Volume"] else 0,
            "marcap": int(row["Marcap"]) if row["Marcap"] else 0,
            "shares": int(row["Stocks"]) if row["Stocks"] else 0,
            "market": "KOSPI" if row.get("MarketId") == "STK" else "KOSDAQ",
        }

        match = PREFERRED_PATTERN.search(name)
        if match:
            suffix = match.group(1)
            base_name = name[: -len(suffix)]
            preferred_list.append({
                "code": code,
                "name": name,
                "base_name": base_name,
                "suffix": suffix,
            })

    common_by_name = {
        info["name"]: code
        for code, info in all_stocks.items()
        if not PREFERRED_PATTERN.search(info["name"])
    }

    pairs = []
    for pref in preferred_list:
        if pref["base_name"] not in common_by_name:
            continue

        cc = common_by_name[pref["base_name"]]
        ci = all_stocks[cc]
        pi = all_stocks[pref["code"]]

        if ci["close"] <= 0 or pi["close"] <= 0:
            continue

        ratio = pi["close"] / ci["close"]
        total_shares = ci["shares"] + pi["shares"]
        shares_ratio = pi["shares"] / total_shares if total_shares > 0 else 0

        pairs.append({
            "common_code": cc,
            "preferred_code": pref["code"],
            "common_name": pref["base_name"],
            "preferred_name": pref["name"],
            "suffix": pref["suffix"],
            "market": ci["market"],
            "common_price": ci["close"],
            "preferred_price": pi["close"],
            "ratio": round(ratio, 4),
            "divergence_rate": round((1 - ratio) * 100, 2),
            "divergence_amount": ci["close"] - pi["close"],
            "common_volume": ci["volume"],
            "preferred_volume": pi["volume"],
            "common_marcap": ci["marcap"],
            "preferred_marcap": pi["marcap"],
            "common_shares": ci["shares"],
            "preferred_shares": pi["shares"],
            "shares_ratio": round(shares_ratio, 4),
            "is_reversed": ratio > 1,
        })

    logger.info(f"페어 매칭 완료: {len(pairs)}개")
    return pairs


# ════════════════════════════════════════════════════════
# 과거 데이터 수집 (캐시 지원)
# ════════════════════════════════════════════════════════

def fetch_historical_data(pairs, start_date=BASE_DATE, use_cache=True):
    """
    각 페어의 과거 일간 종가 수집
    캐시 파일이 있으면 증분 업데이트 (마지막 날짜 이후만 재조회)
    """
    logger.info(f"=== 과거 데이터 수집 ({start_date} ~ 현재, {len(pairs)}개 페어) ===")

    end_date = datetime.now().strftime("%Y-%m-%d")

    # 캐시 로드
    cache = {}
    if use_cache and CACHE_FILE.exists():
        try:
            with open(CACHE_FILE, "rb") as f:
                cache = pickle.load(f)
            logger.info(f"캐시 로드: {len(cache)}개 종목")
        except Exception as e:
            logger.warning(f"캐시 로드 실패: {e}")

    history = {}
    for i, pair in enumerate(pairs):
        cc = pair["common_code"]
        pc = pair["preferred_code"]
        name = pair["common_name"]

        # 캐시된 데이터가 있고 오늘자면 재사용
        cached = cache.get(pc)
        if cached and cached.get("last_date") == end_date:
            history[pc] = cached
            continue

        # 증분 업데이트: 캐시의 마지막 날짜부터 재조회
        fetch_start = start_date
        if cached and cached.get("last_date"):
            last = datetime.strptime(cached["last_date"], "%Y-%m-%d")
            fetch_start = (last - timedelta(days=5)).strftime("%Y-%m-%d")

        try:
            df_common = fdr.DataReader(cc, fetch_start, end_date)
            time.sleep(0.08)
            df_pref = fdr.DataReader(pc, fetch_start, end_date)
            time.sleep(0.08)

            if df_common.empty or df_pref.empty:
                continue

            # 두 종목 모두 데이터가 있는 날짜만
            merged = pd.DataFrame({
                "common": df_common["Close"],
                "pref": df_pref["Close"],
            }).dropna()
            merged = merged[(merged["common"] > 0) & (merged["pref"] > 0)]

            if len(merged) < 2:
                continue

            merged["ratio"] = merged["pref"] / merged["common"]

            new_data = {
                "common_name": pair["common_name"],
                "preferred_name": pair["preferred_name"],
                "dates": [d.strftime("%Y-%m-%d") for d in merged.index],
                "common_close": merged["common"].astype(int).tolist(),
                "pref_close": merged["pref"].astype(int).tolist(),
                "ratio": merged["ratio"].round(4).tolist(),
                "marcap": pair["preferred_marcap"],
                "last_date": end_date,
            }

            # 캐시와 병합
            if cached:
                old_dates = set(cached["dates"])
                # 겹치지 않는 새 데이터만 추가
                for j, d in enumerate(new_data["dates"]):
                    if d not in old_dates:
                        cached["dates"].append(d)
                        cached["common_close"].append(new_data["common_close"][j])
                        cached["pref_close"].append(new_data["pref_close"][j])
                        cached["ratio"].append(new_data["ratio"][j])
                cached["marcap"] = new_data["marcap"]
                cached["last_date"] = end_date
                history[pc] = cached
            else:
                history[pc] = new_data

        except Exception as e:
            logger.warning(f"[{name}] 히스토리 실패: {e}")
            if cached:
                history[pc] = cached  # 실패시 캐시 유지

        if (i + 1) % 20 == 0:
            logger.info(f"히스토리 진행: {i + 1}/{len(pairs)} (수집 {len(history)}개)")

    # 캐시 저장
    try:
        with open(CACHE_FILE, "wb") as f:
            pickle.dump(history, f)
        logger.info(f"캐시 저장: {len(history)}개 종목")
    except Exception as e:
        logger.warning(f"캐시 저장 실패: {e}")

    logger.info(f"히스토리 수집 완료: {len(history)}개")
    return history


# ════════════════════════════════════════════════════════
# KPRI 지수 계산
# ════════════════════════════════════════════════════════

def calculate_kpri_index(history, selected_codes=None, base_date=BASE_DATE):
    """
    KPRI 상대가치 지수 계산

    오늘 지수 = (오늘 평균 비율 / 기준일 평균 비율) × 100
    """
    if selected_codes is None:
        selected_codes = list(history.keys())

    # 날짜별 비율 DataFrame 구성
    all_dates = set()
    for pc in selected_codes:
        if pc in history:
            all_dates.update(history[pc]["dates"])

    if not all_dates:
        return None

    all_dates = sorted(all_dates)
    ratio_df = pd.DataFrame(index=all_dates, columns=selected_codes, dtype=float)

    for pc in selected_codes:
        if pc not in history:
            continue
        h = history[pc]
        s = pd.Series(h["ratio"], index=h["dates"])
        ratio_df.loc[s.index, pc] = s.values

    # 각 날짜의 평균 비율 (결측치 제외)
    avg_ratio = ratio_df.mean(axis=1)
    avg_ratio = avg_ratio.dropna()

    if avg_ratio.empty:
        return None

    # 기준일 찾기 (2020-01-02 이후 첫 유효 날짜)
    valid_dates = [d for d in avg_ratio.index if d >= base_date]
    if not valid_dates:
        return None

    base_d = valid_dates[0]
    base_ratio = avg_ratio[base_d]

    # 지수 값 = (오늘 / 기준일) × 100
    index_values = (avg_ratio.loc[valid_dates] / base_ratio * 100).round(2)

    dates = list(index_values.index)
    current = index_values.iloc[-1]

    def value_days_ago(days):
        target = datetime.strptime(dates[-1], "%Y-%m-%d") - timedelta(days=days)
        target_str = target.strftime("%Y-%m-%d")
        valid = [d for d in dates if d <= target_str]
        if not valid:
            return None
        return float(index_values[valid[-1]])

    change_today = 0.0
    if len(index_values) >= 2:
        change_today = round(float(current - index_values.iloc[-2]), 2)

    return {
        "dates": dates,
        "values": [float(v) for v in index_values.tolist()],
        "current": round(float(current), 2),
        "current_avg_ratio": round(float(avg_ratio.iloc[-1]), 4),
        "base_date": base_d,
        "base_avg_ratio": round(float(base_ratio), 4),
        "change_today": change_today,
        "value_3m_ago": round(value_days_ago(90), 2) if value_days_ago(90) else None,
        "value_6m_ago": round(value_days_ago(180), 2) if value_days_ago(180) else None,
        "value_12m_ago": round(value_days_ago(365), 2) if value_days_ago(365) else None,
        "stock_count": len([pc for pc in selected_codes if pc in history]),
    }


# ════════════════════════════════════════════════════════
# 기여도 분석
# ════════════════════════════════════════════════════════

def calculate_contributions(history, selected_codes=None, marcap_weighted=False, period_days=90):
    """
    각 종목의 지수 변화 기여도 계산

    기본: 3개월(90일) 전 대비 변화량 기준
    Contribution = 비중 × (오늘 비율 - N개월전 비율)
    """
    if selected_codes is None:
        selected_codes = list(history.keys())

    valid_codes = [pc for pc in selected_codes if pc in history and len(history[pc]["ratio"]) >= 2]
    if not valid_codes:
        return []

    # 비중 계산
    if marcap_weighted:
        total_mcap = sum(history[pc]["marcap"] for pc in valid_codes)
        weights = {pc: history[pc]["marcap"] / total_mcap if total_mcap > 0 else 0 for pc in valid_codes}
    else:
        n = len(valid_codes)
        weights = {pc: 1 / n for pc in valid_codes}

    contributions = []
    for pc in valid_codes:
        h = history[pc]
        ratio_today = h["ratio"][-1]
        ratio_past = _get_ratio_days_ago(h, period_days)
        if ratio_past is None:
            ratio_past = h["ratio"][0]
        change = ratio_today - ratio_past
        weight = weights[pc]
        contribution = weight * change

        contributions.append({
            "preferred_code": pc,
            "common_name": h["common_name"],
            "preferred_name": h["preferred_name"],
            "ratio_today": round(ratio_today, 4),
            "ratio_past": round(ratio_past, 4),
            "change": round(change, 4),
            "weight": round(weight, 4),
            "weight_pct": round(weight * 100, 2),
            "contribution": round(contribution, 5),
            "marcap": h["marcap"],
        })

    contributions.sort(key=lambda x: x["contribution"], reverse=True)
    return contributions


# ════════════════════════════════════════════════════════
# 과거 비율 조회 (12/6/3개월전)
# ════════════════════════════════════════════════════════

def _get_ratio_days_ago(hist, days):
    """N일 전 비율 조회 (가장 가까운 과거 거래일)"""
    if not hist or not hist.get("dates"):
        return None
    target = datetime.strptime(hist["dates"][-1], "%Y-%m-%d") - timedelta(days=days)
    target_str = target.strftime("%Y-%m-%d")
    for i in range(len(hist["dates"]) - 1, -1, -1):
        if hist["dates"][i] <= target_str:
            return hist["ratio"][i]
    return None


# ════════════════════════════════════════════════════════
# 메인 진입점
# ════════════════════════════════════════════════════════

def fetch_all():
    """전체 데이터 수집 파이프라인"""
    # 1. 페어 수집
    pairs = fetch_pairs()

    # 2. 과거 데이터 수집 (캐시 사용)
    history = fetch_historical_data(pairs, start_date=BASE_DATE)

    # 3. 과거 데이터가 있는 페어만 필터
    valid_pairs = [p for p in pairs if p["preferred_code"] in history]
    logger.info(f"유효 페어: {len(valid_pairs)}개")

    # 4. 시총 기준 Top 20
    valid_pairs_sorted = sorted(valid_pairs, key=lambda x: x["preferred_marcap"], reverse=True)
    top20_pairs = valid_pairs_sorted[:TOP_N]
    top20_codes = [p["preferred_code"] for p in top20_pairs]

    # 5. 각 페어에 12/6/3개월전 비율 + 배당수익률 추가
    pref_codes = [p["preferred_code"] for p in valid_pairs]
    dividend_yields = fetch_dividend_yields(pref_codes)

    for p in valid_pairs:
        h = history[p["preferred_code"]]
        p["ratio_3m_ago"] = _get_ratio_days_ago(h, 90)
        p["ratio_6m_ago"] = _get_ratio_days_ago(h, 180)
        p["ratio_12m_ago"] = _get_ratio_days_ago(h, 365)
        p["dividend_yield"] = dividend_yields.get(p["preferred_code"])

    # 6. 지수 계산
    kpri_all = calculate_kpri_index(history)
    kpri_top20 = calculate_kpri_index(history, selected_codes=top20_codes)

    # 7. 기여도 분석
    contributions_all = calculate_contributions(history)
    contributions_top20 = calculate_contributions(history, selected_codes=top20_codes)

    top5_all = contributions_all[:5]
    worst5_all = contributions_all[-5:][::-1]
    top5_top20 = contributions_top20[:5]
    worst5_top20 = contributions_top20[-5:][::-1]

    logger.info(
        f"KPRI-전체: {kpri_all['current']} (전일비 {kpri_all['change_today']:+.2f}), "
        f"KPRI-Top20: {kpri_top20['current']} (전일비 {kpri_top20['change_today']:+.2f})"
    )

    # 시가총액(우선주) 내림차순 정렬
    valid_pairs.sort(key=lambda x: x["preferred_marcap"], reverse=True)
    normal = [p for p in valid_pairs if not p["is_reversed"]]
    reversed_pairs = [p for p in valid_pairs if p["is_reversed"]]

    return {
        "date": datetime.now().strftime("%Y%m%d"),
        "pairs": valid_pairs,
        "normal": normal,
        "reversed": reversed_pairs,
        "history": history,
        "kpri_all": kpri_all,
        "kpri_top20": kpri_top20,
        "top5_all": top5_all,
        "worst5_all": worst5_all,
        "top5_top20": top5_top20,
        "worst5_top20": worst5_top20,
        "top20_codes": top20_codes,
        "top20_pairs": top20_pairs,
    }
