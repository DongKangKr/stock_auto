"""
Notion API 연동 모듈 - 3페이지 구조

페이지 구조:
  메인 페이지 (랜딩)
  ├─ 우선주 전체 (sub-page): 전체 종목 DB
  │   └─ 각 row (자동): 종목별 정보 (차트 + 상세)
  └─ KPRI 인덱스 (sub-page): KPRI-전체 / KPRI-Top20 분석
"""

import json
import logging
import time
from pathlib import Path
from urllib.parse import quote

import requests

logger = logging.getLogger(__name__)

NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"
CONFIG_FILE = Path(__file__).parent / "db_config.json"
RATE_LIMIT_DELAY = 0.35

# Notion DB는 컬럼을 알파벳(가나다) 순으로 정렬하므로,
# 원하는 순서를 강제하려면 컬럼명에 정렬 가능한 접두사를 붙여야 함
# 원숫자(①②③...) 사용 → 한글보다 작은 유니코드값으로 정렬됨
COL = {
    "title": "종목명",
    "pref_price": "①우선주 종가",
    "common_price": "②보통주 종가",
    "ratio": "③보통주 대비 비율",
    "ratio_3m": "④3개월전 비율",
    "marcap": "⑤시가총액(억)",
    "div_yield": "⑥우선주 배당수익률(%)",
    "shares_ratio": "⑦발행주식수 비율(%)",
    "market": "⑧시장",
    "status": "⑨상태",
    "ratio_change": "⑩비율 변화(3M)",
    "common_name": "⑪회사명",
    "common_code": "⑫보통주코드",
    "pref_code": "⑬우선주코드",
    "update_date": "⑭업데이트일",
}


# ════════════════════════════════════════════════════════
# Helper functions
# ════════════════════════════════════════════════════════

def _rt(content, bold=False, color="default", code=False):
    obj = {"type": "text", "text": {"content": str(content)}}
    ann = {}
    if bold:
        ann["bold"] = True
    if color != "default":
        ann["color"] = color
    if code:
        ann["code"] = True
    if ann:
        obj["annotations"] = ann
    return obj


def _callout(text_parts, emoji, color="default"):
    return {
        "type": "callout",
        "callout": {
            "rich_text": text_parts if isinstance(text_parts, list) else [_rt(text_parts)],
            "icon": {"type": "emoji", "emoji": emoji},
            "color": color,
        },
    }


def _sample_data(dates, *data_series, max_points=25):
    n = len(dates)
    if n <= max_points:
        return (dates, *data_series)
    step = max(1, n // max_points)
    sampled_dates = [dates[i] for i in range(0, n, step)]
    sampled_series = tuple([series[i] for i in range(0, n, step)] for series in data_series)
    if dates[-1] != sampled_dates[-1]:
        sampled_dates.append(dates[-1])
        for j, series in enumerate(data_series):
            sampled_series[j].append(series[-1])
    return (sampled_dates, *sampled_series)


def _chart_url(chart_config, w=800, h=380):
    config_json = json.dumps(chart_config, ensure_ascii=False, separators=(",", ":"))
    return f"https://quickchart.io/chart?c={quote(config_json)}&w={w}&h={h}&bkg=white"


def _fmt_num(v, digits=2):
    if v is None:
        return "-"
    try:
        return f"{float(v):.{digits}f}"
    except (ValueError, TypeError):
        return "-"


def _fmt_change(v):
    if v is None:
        return "-"
    try:
        return f"{float(v):+.2f}"
    except (ValueError, TypeError):
        return "-"


def _fmt_marcap(v):
    if not v:
        return "-"
    if v >= 1_0000_0000_0000:
        return f"{v / 1_0000_0000_0000:.2f}조"
    if v >= 1_0000_0000:
        return f"{v / 1_0000_0000:.0f}억"
    return f"{v:,}"


# ════════════════════════════════════════════════════════
# Main class
# ════════════════════════════════════════════════════════

class NotionUpdater:
    def __init__(self, token: str, main_page_id: str):
        self.token = token
        self.main_page_id = main_page_id
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Notion-Version": NOTION_VERSION,
        }
        self.list_page_id = None
        self.index_page_id = None
        self.db_id = None
        self._load_config()

    # ─── Config ────────────────────────────────────────────

    def _load_config(self):
        if not CONFIG_FILE.exists():
            return
        try:
            with open(CONFIG_FILE, "r") as f:
                cfg = json.load(f)
                self.list_page_id = cfg.get("list_page_id")
                self.index_page_id = cfg.get("index_page_id")
                self.db_id = cfg.get("database_id")
        except Exception as e:
            logger.warning(f"config 로드 실패: {e}")

    def _save_config(self):
        with open(CONFIG_FILE, "w") as f:
            json.dump({
                "list_page_id": self.list_page_id,
                "index_page_id": self.index_page_id,
                "database_id": self.db_id,
            }, f, indent=2, ensure_ascii=False)

    # ─── API ───────────────────────────────────────────────

    def _request(self, method, url, **kwargs):
        time.sleep(RATE_LIMIT_DELAY)
        res = requests.request(method, url, headers=self.headers, timeout=30, **kwargs)
        if res.status_code == 429:
            retry_after = int(res.headers.get("Retry-After", 2))
            logger.warning(f"Rate limited, {retry_after}s...")
            time.sleep(retry_after)
            res = requests.request(method, url, headers=self.headers, timeout=30, **kwargs)
        if not res.ok:
            logger.error(f"Notion API error: {res.status_code} {res.text[:300]}")
        res.raise_for_status()
        return res.json()

    def _append_blocks(self, parent_id, blocks):
        return self._request("PATCH", f"{NOTION_API}/blocks/{parent_id}/children", json={"children": blocks})

    def _exists(self, obj_type, obj_id):
        """pages 또는 databases가 존재하는지 확인"""
        if not obj_id:
            return False
        try:
            data = self._request("GET", f"{NOTION_API}/{obj_type}/{obj_id}")
            return not data.get("archived", False)
        except Exception:
            return False

    def _clear_page_blocks(self, page_id, keep_types=None):
        """페이지의 블록 삭제 (keep_types에 있는 타입은 유지)"""
        keep_types = keep_types or []
        block_ids = []
        has_more, cursor = True, None
        while has_more:
            url = f"{NOTION_API}/blocks/{page_id}/children?page_size=100"
            if cursor:
                url += f"&start_cursor={cursor}"
            data = self._request("GET", url)
            for b in data.get("results", []):
                if b["type"] not in keep_types:
                    block_ids.append(b["id"])
            has_more = data.get("has_more", False)
            cursor = data.get("next_cursor")

        for bid in block_ids:
            try:
                self._request("DELETE", f"{NOTION_API}/blocks/{bid}")
            except Exception:
                pass
        return len(block_ids)

    # ─── Sub-page management ─────────────────────────────

    def _create_sub_page(self, title, emoji):
        data = self._request("POST", f"{NOTION_API}/pages", json={
            "parent": {"page_id": self.main_page_id},
            "icon": {"type": "emoji", "emoji": emoji},
            "properties": {
                "title": {"title": [{"text": {"content": title}}]}
            },
        })
        return data["id"]

    def _find_existing_sub_pages(self):
        """메인 페이지의 child_page 블록들을 제목으로 매핑"""
        pages_by_title = {}  # title -> [(block_id, page_id), ...]
        has_more, cursor = True, None
        while has_more:
            url = f"{NOTION_API}/blocks/{self.main_page_id}/children?page_size=100"
            if cursor:
                url += f"&start_cursor={cursor}"
            data = self._request("GET", url)
            for b in data.get("results", []):
                if b["type"] == "child_page":
                    title = b["child_page"].get("title", "")
                    pages_by_title.setdefault(title, []).append(b["id"])
            has_more = data.get("has_more", False)
            cursor = data.get("next_cursor")
        return pages_by_title

    def _cleanup_duplicate_sub_pages(self):
        """동일 제목 서브페이지 중복 제거 - 설정에 저장된 ID만 유지"""
        by_title = self._find_existing_sub_pages()
        for title, target_attr in [("우선주 전체", "list_page_id"), ("KPRI 인덱스", "index_page_id")]:
            ids = by_title.get(title, [])
            if len(ids) <= 1:
                continue
            stored = getattr(self, target_attr)
            to_delete = [pid for pid in ids if pid != stored]
            for pid in to_delete:
                try:
                    self._request("PATCH", f"{NOTION_API}/pages/{pid}", json={"archived": True})
                    logger.info(f"중복 서브페이지 아카이브: {title} ({pid[:8]})")
                except Exception as e:
                    logger.warning(f"중복 페이지 삭제 실패: {e}")

    def _ensure_sub_pages(self):
        if not self._exists("pages", self.list_page_id):
            logger.info("우선주 전체 서브페이지 생성")
            self.list_page_id = self._create_sub_page("우선주 전체", "📋")
            self._save_config()
        if not self._exists("pages", self.index_page_id):
            logger.info("KPRI 인덱스 서브페이지 생성")
            self.index_page_id = self._create_sub_page("KPRI 인덱스", "📈")
            self._save_config()
        # 중복 서브페이지 정리
        self._cleanup_duplicate_sub_pages()

    # ─── Database ──────────────────────────────────────────

    def _create_database(self):
        """우선주 전체 sub-page 안에 인라인 DB 생성
        Notion이 컬럼을 가나다 순으로 정렬하기 때문에,
        원숫자(①~⑭) 접두사로 원하는 순서를 강제함
        """
        payload = {
            "parent": {"page_id": self.list_page_id},
            "is_inline": True,
            "title": [{"type": "text", "text": {"content": "우선주 전체"}}],
            "properties": {
                COL["title"]: {"title": {}},
                COL["pref_price"]: {"number": {"format": "number_with_commas"}},
                COL["common_price"]: {"number": {"format": "number_with_commas"}},
                COL["ratio"]: {"number": {"format": "number"}},
                COL["ratio_3m"]: {"number": {"format": "number"}},
                COL["marcap"]: {"number": {"format": "number_with_commas"}},
                COL["div_yield"]: {"number": {"format": "number"}},
                COL["shares_ratio"]: {"number": {"format": "number"}},
                COL["market"]: {"select": {"options": [
                    {"name": "KOSPI", "color": "blue"},
                    {"name": "KOSDAQ", "color": "green"},
                ]}},
                COL["status"]: {"select": {"options": [
                    {"name": "정상", "color": "green"},
                    {"name": "역전", "color": "red"},
                ]}},
                COL["ratio_change"]: {"number": {"format": "number"}},
                COL["common_name"]: {"rich_text": {}},
                COL["common_code"]: {"rich_text": {}},
                COL["pref_code"]: {"rich_text": {}},
                COL["update_date"]: {"date": {}},
            },
        }
        data = self._request("POST", f"{NOTION_API}/databases", json=payload)
        return data["id"]

    def _clear_existing_databases_on_list_page(self):
        """우선주 전체 sub-page에 있는 모든 child_database 블록 삭제"""
        removed = 0
        has_more, cursor = True, None
        while has_more:
            url = f"{NOTION_API}/blocks/{self.list_page_id}/children?page_size=100"
            if cursor:
                url += f"&start_cursor={cursor}"
            data = self._request("GET", url)
            for b in data.get("results", []):
                if b["type"] == "child_database":
                    try:
                        self._request("DELETE", f"{NOTION_API}/blocks/{b['id']}")
                        removed += 1
                    except Exception as e:
                        logger.warning(f"DB 블록 삭제 실패: {e}")
            has_more = data.get("has_more", False)
            cursor = data.get("next_cursor")
        if removed:
            logger.info(f"기존 DB 블록 {removed}개 삭제")

    def _ensure_database(self):
        if self._exists("databases", self.db_id):
            return self.db_id
        logger.info("우선주 전체 DB 생성 - 기존 DB 블록 정리")
        self._clear_existing_databases_on_list_page()
        self.db_id = self._create_database()
        self._save_config()
        return self.db_id

    def _get_existing_db_pages(self):
        """기존 DB 페이지 조회 (우선주코드 -> page_id 매핑)"""
        pages = {}
        has_more, cursor = True, None
        while has_more:
            payload = {"page_size": 100}
            if cursor:
                payload["start_cursor"] = cursor
            data = self._request("POST", f"{NOTION_API}/databases/{self.db_id}/query", json=payload)
            for page in data.get("results", []):
                props = page["properties"]
                pref_code_prop = props.get(COL["pref_code"], {})
                rich_text = pref_code_prop.get("rich_text", [])
                if rich_text:
                    code = rich_text[0]["text"]["content"]
                    pages[code] = page["id"]
            has_more = data.get("has_more", False)
            cursor = data.get("next_cursor")
        return pages

    def _build_row_properties(self, row, date_str):
        fd = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        ratio_3m = row.get("ratio_3m_ago")
        ratio_change = round(row["ratio"] - ratio_3m, 4) if ratio_3m else None
        div_yield = row.get("dividend_yield")

        return {
            COL["title"]: {"title": [{"text": {"content": row["preferred_name"]}}]},
            COL["pref_price"]: {"number": row["preferred_price"]},
            COL["common_price"]: {"number": row["common_price"]},
            COL["ratio"]: {"number": round(row["ratio"], 4)},
            COL["ratio_3m"]: {"number": round(ratio_3m, 4) if ratio_3m else None},
            COL["marcap"]: {"number": int(row["preferred_marcap"] / 1_0000_0000) if row["preferred_marcap"] else 0},
            COL["div_yield"]: {"number": div_yield if div_yield is not None else None},
            COL["shares_ratio"]: {"number": round(row["shares_ratio"] * 100, 2)},
            COL["market"]: {"select": {"name": row["market"]}},
            COL["status"]: {"select": {"name": "역전" if row.get("is_reversed") else "정상"}},
            COL["ratio_change"]: {"number": ratio_change},
            COL["common_name"]: {"rich_text": [{"text": {"content": row["common_name"]}}]},
            COL["common_code"]: {"rich_text": [{"text": {"content": row["common_code"]}}]},
            COL["pref_code"]: {"rich_text": [{"text": {"content": row["preferred_code"]}}]},
            COL["update_date"]: {"date": {"start": fd}},
        }

    def _create_row(self, properties):
        data = self._request("POST", f"{NOTION_API}/pages", json={
            "parent": {"database_id": self.db_id},
            "properties": properties,
        })
        return data["id"]

    def _update_row(self, page_id, properties):
        self._request("PATCH", f"{NOTION_API}/pages/{page_id}", json={"properties": properties})

    def _archive_row(self, page_id):
        self._request("PATCH", f"{NOTION_API}/pages/{page_id}", json={"archived": True})

    # ─── Chart URL Builders ────────────────────────────────

    def _kpri_chart_url(self, title, dates, values, color="#2563eb"):
        s_dates, s_values = _sample_data(dates, values, max_points=40)
        config = {
            "type": "line",
            "data": {
                "labels": s_dates,
                "datasets": [{
                    "label": title,
                    "data": s_values,
                    "borderColor": color,
                    "fill": True,
                    "pointRadius": 0,
                    "borderWidth": 2,
                }],
            },
            "options": {
                "title": {"display": True, "text": f"{title} (기준일=100)"},
                "legend": {"display": False},
            },
        }
        return _chart_url(config, 900, 360)

    def _price_chart_url(self, name, dates, common, pref):
        s_dates, s_common, s_pref = _sample_data(dates, common, pref, max_points=25)
        config = {
            "type": "line",
            "data": {
                "labels": s_dates,
                "datasets": [
                    {"label": "보통주", "data": s_common, "borderColor": "#3498db", "fill": False, "pointRadius": 0},
                    {"label": "우선주", "data": s_pref, "borderColor": "#e74c3c", "fill": False, "pointRadius": 0},
                ],
            },
            "options": {"title": {"display": True, "text": f"{name} 가격(1년)"}},
        }
        return _chart_url(config, 700, 340)

    def _ratio_chart_url(self, name, dates, ratios):
        s_dates, s_ratios = _sample_data(dates, ratios, max_points=25)
        config = {
            "type": "line",
            "data": {
                "labels": s_dates,
                "datasets": [{
                    "label": "비율",
                    "data": s_ratios,
                    "borderColor": "#e67e22",
                    "fill": True,
                    "pointRadius": 0,
                }],
            },
            "options": {"title": {"display": True, "text": f"{name} 보통주대비 비율(1년)"}},
        }
        return _chart_url(config, 700, 320)

    # ─── Main Page (Landing) ──────────────────────────────

    def _set_main_title(self):
        self._request("PATCH", f"{NOTION_API}/pages/{self.main_page_id}", json={
            "icon": {"type": "emoji", "emoji": "📈"},
            "properties": {
                "title": {"title": [{"text": {"content": "국내 상장사 보통주 vs 우선주 대시보드"}}]}
            },
        })

    def _update_main_page(self, data):
        """메인 페이지 랜딩 업데이트 - 요약 + 서브페이지 네비게이션"""
        self._clear_page_blocks(self.main_page_id, keep_types=["child_page"])

        fd = data["date"]
        formatted = f"{fd[:4]}-{fd[4:6]}-{fd[6:8]}"
        kpri_all = data["kpri_all"]
        kpri_top20 = data["kpri_top20"]

        blocks = []

        # 환영 + 목적 설명
        blocks.append(_callout([
            _rt("국내 상장사 보통주 vs 우선주 대시보드\n\n", bold=True),
            _rt("한국거래소(KRX)에 상장된 우선주 보유 회사들의 보통주/우선주 가격 관계를 "
                "추적하고, 우선주 상대가치 지수(KPRI)를 산출하여 시장 전반의 우선주 할인/프리미엄 "
                "변화를 한눈에 확인할 수 있습니다.\n\n"),
            _rt(f"📅 업데이트: {formatted}  |  📊 종목수: {len(data['pairs'])}개  |  "
                f"📐 기준일: 2020.01.02 = 100"),
        ], "📈", "blue_background"))

        # KPRI 배지 (2열) - 설명 포함
        def kpri_badge(title, kpri, emoji, desc):
            change = kpri["change_today"]
            change_color = "red" if change < 0 else "green"
            return {
                "type": "column",
                "column": {"children": [
                    _callout([
                        _rt(f"{title}\n", bold=True),
                        _rt(f"{kpri['current']:.2f}", bold=True, color="blue"),
                        _rt(f"  전일비 {_fmt_change(change)}\n", bold=True, color=change_color),
                        _rt(desc, color="gray"),
                    ], emoji, "gray_background"),
                ]},
            }

        blocks.append({
            "type": "column_list",
            "column_list": {"children": [
                kpri_badge("KPRI-전체", kpri_all, "📊", f"전체 {kpri_all['stock_count']}개 우선주 기준"),
                kpri_badge("KPRI-Top20", kpri_top20, "🏆", "우선주 시총 상위 20개 기준"),
            ]},
        })

        blocks.append({"type": "divider", "divider": {}})

        # 핵심 용어 설명
        blocks.append({"type": "heading_2", "heading_2": {"rich_text": [_rt("📖 핵심 용어")]}})

        blocks.append(_callout([
            _rt("보통주 대비 비율 (Ratio)\n", bold=True, color="blue"),
            _rt("= 우선주 종가 ÷ 보통주 종가\n"),
            _rt("1에 가까울수록 우선주가 보통주와 비슷한 가격이며, "),
            _rt("낮을수록 우선주가 저평가(할인) 상태입니다. "),
            _rt("1보다 크면 '역전' 상태로 우선주가 보통주보다 비싸게 거래되는 특수 상황입니다."),
        ], "1️⃣", "default"))

        blocks.append(_callout([
            _rt("KPRI 상대가치 지수\n", bold=True, color="blue"),
            _rt("= (오늘 평균 비율 ÷ 기준일 평균 비율) × 100\n"),
            _rt("기준일 2020-01-02을 100으로 고정하고, 시장 전체 평균 비율이 기준일 대비 어떻게 "),
            _rt("변했는지를 지수화한 값입니다. 100보다 낮으면 우선주 할인이 확대된 상태, "),
            _rt("높으면 축소된 상태입니다."),
        ], "2️⃣", "default"))

        blocks.append(_callout([
            _rt("KPRI-Top20\n", bold=True, color="blue"),
            _rt("우선주 시가총액 상위 20개 종목만으로 계산한 지수입니다. "),
            _rt("소형 종목의 이상치(극단적 역전 등)를 배제하여 시장의 중심 흐름을 더 안정적으로 "),
            _rt("반영합니다."),
        ], "3️⃣", "default"))

        blocks.append({"type": "divider", "divider": {}})

        # 페이지 구성 안내
        blocks.append({"type": "heading_2", "heading_2": {"rich_text": [_rt("📂 페이지 구성")]}})
        blocks.append({
            "type": "paragraph",
            "paragraph": {"rich_text": [
                _rt("아래 2개 서브페이지로 구성되어 있습니다. 각 카드를 클릭하면 이동합니다.", color="gray"),
            ]},
        })

        blocks.append(_callout([
            _rt("📋 우선주 전체\n", bold=True),
            _rt("한국거래소 상장 우선주 보유 회사 전체 리스트입니다. 우선주명, 보통주/우선주 종가, "),
            _rt("보통주 대비 비율, 3개월전 비율, 시가총액, 발행주식수 비율 등을 테이블로 확인할 수 "),
            _rt("있으며, 종목(행)을 클릭하면 해당 회사의 1년 가격 추이 차트, 비율 변동, 12/6/3개월전 "),
            _rt("비교 등 상세 정보를 볼 수 있습니다."),
        ], "1️⃣", "default"))

        blocks.append(_callout([
            _rt("📈 KPRI 인덱스\n", bold=True),
            _rt("KPRI-전체 / KPRI-Top20 상대가치 지수의 현재값, 시점별 비교(12/6/3개월전), "),
            _rt("기준일부터의 시계열 차트, 그리고 3개월 대비 기여도 Top 5 / Worst 5 분석을 "),
            _rt("제공합니다. 시장 전반의 우선주 할인 흐름을 한눈에 파악할 수 있습니다."),
        ], "2️⃣", "default"))

        self._append_blocks(self.main_page_id, blocks)

    # ─── List Page (우선주 전체) ──────────────────────────

    def _build_list_page_explanation(self):
        """우선주 전체 페이지 설명 블록"""
        blocks = []

        blocks.append({"type": "heading_2", "heading_2": {"rich_text": [_rt("📖 컬럼 설명")]}})

        blocks.append(_callout([
            _rt("이 테이블은 한국거래소에 상장된 ", color="gray"),
            _rt("우선주를 보유한 전체 상장사", bold=True),
            _rt("를 나열한 것입니다. 종목(행)을 클릭하면 해당 회사의 ", color="gray"),
            _rt("가격/비율 차트와 과거 비교 데이터", bold=True),
            _rt("를 확인할 수 있습니다.", color="gray"),
        ], "💡", "blue_background"))

        columns = [
            ("종목명", "해당 회사가 발행한 우선주의 종목명 (예: BYC우, 삼성전자우). 클릭하면 상세 페이지로 이동"),
            ("우선주 종가", "오늘 우선주 종가 (원)"),
            ("보통주 종가", "오늘 보통주 종가 (원)"),
            ("보통주 대비 비율", "우선주 종가 ÷ 보통주 종가. 1에 가까울수록 우선주 프리미엄, 낮을수록 할인 상태"),
            ("3개월전 비율", "약 90일 전(거래일 기준)의 보통주 대비 비율"),
            ("비율 변화(3M)", "현재 비율 - 3개월전 비율. 양수면 우선주 할인이 축소(비율 상승), 음수면 확대"),
            ("시가총액(억)", "우선주 시가총액 (억원 단위)"),
            ("우선주 배당수익률(%)", "최근 12개월 배당금 ÷ 우선주 종가 × 100. 네이버 파이낸스 기준"),
            ("발행주식수 비율(%)", "우선주 발행주식수 ÷ (보통주 + 우선주) 전체 발행주식수"),
            ("시장", "KOSPI 또는 KOSDAQ"),
            ("상태", "🟢 정상: 보통주 > 우선주 (일반적)  /  🔴 역전: 우선주 > 보통주 (특수 상황)"),
            ("회사명 / 보통주코드 / 우선주코드", "참고용 식별 정보"),
            ("업데이트일", "데이터 조회 날짜"),
        ]

        for name, desc in columns:
            blocks.append({
                "type": "bulleted_list_item",
                "bulleted_list_item": {"rich_text": [
                    _rt(f"{name}: ", bold=True, color="blue"),
                    _rt(desc),
                ]},
            })

        blocks.append({"type": "divider", "divider": {}})

        blocks.append({"type": "heading_3", "heading_3": {"rich_text": [_rt("🔍 사용 팁")]}})

        tips = [
            "괴리율이 큰 종목 찾기: '보통주 대비 비율' 컬럼을 오름차순 정렬 → 가장 할인된 우선주가 위로",
            "최근 변동 확인: '비율 변화(3M)' 컬럼으로 최근 3개월간 프리미엄이 확대/축소된 종목 확인",
            "시장별 필터: '시장' 컬럼으로 KOSPI / KOSDAQ 필터링",
            "역전 종목 찾기: '상태' 컬럼에서 '역전' 필터로 우선주가 보통주보다 비싼 특수 종목 확인",
            "종목 클릭 → 상세 페이지: 1년 가격 추이, 비율 변동, 12/6/3개월전 비교, 최근 거래일 데이터 확인",
        ]
        for tip in tips:
            blocks.append({
                "type": "bulleted_list_item",
                "bulleted_list_item": {"rich_text": [_rt(tip)]},
            })

        return blocks

    def _update_list_page(self, data):
        """우선주 전체 페이지 - DB rows 업데이트 + 설명 블록"""
        date_str = data["date"]
        pairs = data["pairs"]
        history = data["history"]

        # 기존 비-DB 블록 삭제 (설명 등), DB는 유지
        self._clear_page_blocks(self.list_page_id, keep_types=["child_database"])

        # 기존 DB 페이지 조회
        existing = self._get_existing_db_pages()
        current_codes = set()

        created = 0
        updated = 0
        chart_added = 0

        # Notion DB 기본 뷰는 생성시간 역순으로 표시하므로,
        # 시총 큰 종목이 상단에 보이려면 시총 작은 것부터 삽입해야 함
        # data["pairs"]는 시총 내림차순이므로 reversed로 순회
        insertion_order = list(reversed(pairs))

        for i, row in enumerate(insertion_order):
            pref_code = row["preferred_code"]
            current_codes.add(pref_code)
            props = self._build_row_properties(row, date_str)

            if pref_code in existing:
                page_id = existing[pref_code]
                self._update_row(page_id, props)
                updated += 1
            else:
                page_id = self._create_row(props)
                created += 1

            # 종목 상세 페이지 업데이트 (기존 블록 clear + 차트 재생성)
            hist = history.get(pref_code)
            if hist and len(hist.get("dates", [])) >= 5:
                try:
                    self._clear_page_blocks(page_id)
                    blocks = self._build_stock_detail_blocks(row, hist)
                    self._append_blocks(page_id, blocks)
                    chart_added += 1
                except Exception as e:
                    logger.warning(f"[{row['common_name']}] 상세 페이지 실패: {e}")

            if (i + 1) % 20 == 0:
                logger.info(f"List 진행: {i + 1}/{len(pairs)} (차트: {chart_added})")

        # 더 이상 없는 종목 아카이브
        archived = 0
        for code, page_id in existing.items():
            if code not in current_codes:
                try:
                    self._archive_row(page_id)
                    archived += 1
                except Exception:
                    pass

        logger.info(f"DB 업데이트: 생성 {created}, 수정 {updated}, 아카이브 {archived}, 차트 {chart_added}")

        # DB 아래에 설명 블록 추가
        try:
            self._append_blocks(self.list_page_id, self._build_list_page_explanation())
            logger.info("우선주 전체 페이지 설명 추가 완료")
        except Exception as e:
            logger.warning(f"설명 블록 추가 실패: {e}")

        return {"created": created, "updated": updated, "archived": archived, "chart_added": chart_added}

    # ─── Stock Detail Page ─────────────────────────────────

    def _build_stock_detail_blocks(self, row, hist):
        """종목별 정보 페이지 내부 콘텐츠"""
        blocks = []
        name = row["common_name"]
        pref_name = row["preferred_name"]

        status = "🔴 역전" if row.get("is_reversed") else "🟢 정상"

        # 요약 callout
        blocks.append(_callout([
            _rt(f"{pref_name} ({name})\n\n", bold=True),
            _rt(f"우선주 종가: {row['preferred_price']:,}원  |  보통주 종가: {row['common_price']:,}원\n"),
            _rt("보통주 대비 비율: "),
            _rt(f"{row['ratio']:.4f}", bold=True, color="blue"),
            _rt(f"  |  상태: {status}\n"),
            _rt(f"시가총액(우): {_fmt_marcap(row['preferred_marcap'])}  |  "),
            _rt(f"발행주식수 비율: {row['shares_ratio'] * 100:.1f}%"),
        ], "📋", "blue_background"))

        if not hist or len(hist.get("dates", [])) < 5:
            blocks.append(_callout("과거 데이터를 불러올 수 없습니다.", "⚠️", "yellow_background"))
            return blocks

        # 비율 비교 테이블 섹션
        blocks.append({"type": "heading_3", "heading_3": {"rich_text": [_rt("📋 비율 비교 (보통주 대비)")]}})
        blocks.append(_callout([
            _rt("기준 시점별 '보통주 대비 비율'을 비교합니다. 값이 낮을수록 우선주가 보통주 대비 저평가 상태입니다. "),
            _rt("시간이 지나며 비율이 하락하면 할인이 확대된 것이고, 상승하면 할인이 축소된 것입니다.", color="gray"),
        ], "💡", "default"))
        blocks.append({
            "type": "table",
            "table": {
                "table_width": 5,
                "has_column_header": True,
                "has_row_header": False,
                "children": [
                    {"type": "table_row", "table_row": {"cells": [
                        [_rt("시점", bold=True)],
                        [_rt("12개월전", bold=True)],
                        [_rt("6개월전", bold=True)],
                        [_rt("3개월전", bold=True)],
                        [_rt("현재", bold=True)],
                    ]}},
                    {"type": "table_row", "table_row": {"cells": [
                        [_rt("비율")],
                        [_rt(_fmt_num(row.get("ratio_12m_ago"), 4))],
                        [_rt(_fmt_num(row.get("ratio_6m_ago"), 4))],
                        [_rt(_fmt_num(row.get("ratio_3m_ago"), 4))],
                        [_rt(_fmt_num(row.get("ratio"), 4), bold=True, color="blue")],
                    ]}},
                ],
            },
        })

        # 1년 데이터만 (252 거래일)
        recent_dates = hist["dates"][-252:]
        recent_common = hist["common_close"][-252:]
        recent_pref = hist["pref_close"][-252:]
        recent_ratio = hist["ratio"][-252:]

        # 가격 차트 섹션
        blocks.append({"type": "heading_3", "heading_3": {"rich_text": [_rt("📈 보통주 vs 우선주 가격 (1년)")]}})
        blocks.append(_callout([
            _rt("최근 1년간 "),
            _rt("보통주(파란색)", bold=True, color="blue"),
            _rt("와 "),
            _rt("우선주(빨간색)", bold=True, color="red"),
            _rt("의 일간 종가 추이입니다. 두 선의 간격이 벌어질수록 괴리가 커진 것이며, "),
            _rt("방향성이 일치하는지(함께 상승/하락)를 통해 종목 특성을 확인할 수 있습니다.", color="gray"),
        ], "💡", "default"))
        blocks.append({
            "type": "image",
            "image": {"type": "external", "external": {"url": self._price_chart_url(name, recent_dates, recent_common, recent_pref)}},
        })

        # 비율 차트 섹션
        blocks.append({"type": "heading_3", "heading_3": {"rich_text": [_rt("📊 보통주 대비 비율 추이 (1년)")]}})
        blocks.append(_callout([
            _rt("최근 1년간 "),
            _rt("보통주 대비 비율", bold=True, color="orange"),
            _rt(" (우선주 종가 ÷ 보통주 종가)의 변동입니다. "),
            _rt("상승 추세면 할인이 축소(우선주 상대 강세), 하락 추세면 할인이 확대된 상태입니다. "),
            _rt("급격한 변동 구간은 배당락, 대규모 거래, 이벤트 등을 반영할 수 있습니다.", color="gray"),
        ], "💡", "default"))
        blocks.append({
            "type": "image",
            "image": {"type": "external", "external": {"url": self._ratio_chart_url(name, recent_dates, recent_ratio)}},
        })

        # 최근 10거래일 테이블 섹션
        blocks.append({"type": "heading_3", "heading_3": {"rich_text": [_rt("📋 최근 10거래일")]}})
        blocks.append(_callout([
            _rt("가장 최근 10거래일의 종가와 보통주 대비 비율 원시 데이터입니다. 비율이 1.0을 초과하면 붉게 표시됩니다(역전 구간).", color="gray"),
        ], "💡", "default"))
        table_rows = [{
            "type": "table_row",
            "table_row": {"cells": [
                [_rt("날짜", bold=True)],
                [_rt("보통주", bold=True)],
                [_rt("우선주", bold=True)],
                [_rt("비율", bold=True)],
            ]},
        }]
        recent_count = min(10, len(hist["dates"]))
        for j in range(1, recent_count + 1):
            idx = -j
            r = hist["ratio"][idx]
            color = "red" if r > 1 else "default"
            table_rows.append({
                "type": "table_row",
                "table_row": {"cells": [
                    [_rt(hist["dates"][idx])],
                    [_rt(f"{hist['common_close'][idx]:,}")],
                    [_rt(f"{hist['pref_close'][idx]:,}")],
                    [_rt(_fmt_num(r, 4), bold=True, color=color)],
                ]},
            })
        blocks.append({
            "type": "table",
            "table": {
                "table_width": 4,
                "has_column_header": True,
                "has_row_header": False,
                "children": table_rows,
            },
        })

        return blocks

    # ─── Index Page (KPRI 인덱스) ─────────────────────────

    def _update_index_page(self, data):
        """KPRI 인덱스 페이지 - 전체 clear 후 재구성"""
        self._clear_page_blocks(self.index_page_id)

        kpri_all = data["kpri_all"]
        kpri_top20 = data["kpri_top20"]
        fd = data["date"]
        formatted = f"{fd[:4]}-{fd[4:6]}-{fd[6:8]}"

        blocks = []

        # 지수 개념 설명
        blocks.append(_callout([
            _rt("KPRI 우선주 상대가치 지수란?\n\n", bold=True),
            _rt("KPRI(Korea Preferred stock Relative Index)는 시장 전체의 우선주가 보통주 대비 "),
            _rt("어느 정도로 평가받고 있는지를 하나의 숫자로 나타낸 지수입니다. "),
            _rt("기준일 2020.01.02를 100으로 고정하여, 오늘의 숫자가 낮으면 기준일 대비 "),
            _rt("우선주 할인이 확대(저평가 심화)된 것이고, 높으면 할인이 축소(상대 강세)된 것입니다.\n\n"),
            _rt("계산 공식\n", bold=True, color="blue"),
            _rt("① 각 종목의 보통주 대비 비율 = 우선주 종가 ÷ 보통주 종가\n"),
            _rt("② 전체 대상 종목의 비율을 단순 평균\n"),
            _rt("③ 지수 = (오늘 평균 비율 ÷ 기준일 평균 비율) × 100"),
        ], "📐", "gray_background"))

        # KPRI 배지 (2열)
        def kpri_badge(title, kpri, emoji, desc):
            change = kpri["change_today"]
            change_color = "red" if change < 0 else "green"
            return {
                "type": "column",
                "column": {"children": [
                    _callout([
                        _rt(f"{title}\n", bold=True),
                        _rt(f"{kpri['current']:.2f}\n", bold=True, color="blue"),
                        _rt("전일비 ", color="gray"),
                        _rt(f"{_fmt_change(change)}", bold=True, color=change_color),
                        _rt(f"  |  종목수 {kpri['stock_count']}\n", color="gray"),
                        _rt(desc, color="gray"),
                    ], emoji, "blue_background"),
                ]},
            }

        blocks.append({
            "type": "column_list",
            "column_list": {"children": [
                kpri_badge("KPRI-전체", kpri_all, "📊",
                           "우선주 보유 상장사 전체를 대상으로 산출"),
                kpri_badge("KPRI-Top20 (시총)", kpri_top20, "🏆",
                           "우선주 시가총액 상위 20개 종목 대상"),
            ]},
        })

        # 두 지수 차이 설명
        blocks.append(_callout([
            _rt("전체 vs Top20 차이\n", bold=True),
            _rt("전체 지수는 소형·이상치 종목의 영향을 받을 수 있습니다. "),
            _rt("Top20 지수는 시가총액 상위 20개만 사용하므로 주요 대형 우선주의 중심 흐름을 반영하며 더 안정적입니다.", color="gray"),
        ], "💡", "default"))

        blocks.append({"type": "divider", "divider": {}})

        # 지수 비교 테이블
        blocks.append({"type": "heading_2", "heading_2": {"rich_text": [_rt("📊 시점별 지수 비교")]}})
        blocks.append(_callout([
            _rt("지수가 12개월 / 6개월 / 3개월 전 대비 어떻게 변했는지 한눈에 확인할 수 있습니다. "),
            _rt("시점이 최근으로 올수록 숫자가 낮아지면 그 구간에 우선주 할인이 확대된 것입니다.", color="gray"),
        ], "💡", "default"))

        def row_for(name, kpri):
            change_color = "red" if kpri["change_today"] < 0 else "green"
            return {
                "type": "table_row",
                "table_row": {"cells": [
                    [_rt(name, bold=True)],
                    [_rt(_fmt_num(kpri["value_12m_ago"]))],
                    [_rt(_fmt_num(kpri["value_6m_ago"]))],
                    [_rt(_fmt_num(kpri["value_3m_ago"]))],
                    [_rt(_fmt_num(kpri["current"]), bold=True, color="blue")],
                    [_rt(_fmt_change(kpri["change_today"]), bold=True, color=change_color)],
                ]},
            }

        blocks.append({
            "type": "table",
            "table": {
                "table_width": 6,
                "has_column_header": True,
                "has_row_header": False,
                "children": [
                    {"type": "table_row", "table_row": {"cells": [
                        [_rt("지수", bold=True)],
                        [_rt("12개월전", bold=True)],
                        [_rt("6개월전", bold=True)],
                        [_rt("3개월전", bold=True)],
                        [_rt("현재", bold=True)],
                        [_rt("전일비", bold=True)],
                    ]}},
                    row_for("KPRI-전체", kpri_all),
                    row_for("KPRI-Top20", kpri_top20),
                ],
            },
        })

        # KPRI 시계열 차트
        blocks.append({"type": "heading_2", "heading_2": {"rich_text": [_rt("📉 지수 시계열 차트")]}})
        blocks.append(_callout([
            _rt("기준일(2020.01.02 = 100)부터 오늘까지의 지수 추이입니다. "),
            _rt("선이 100 아래면 기준일 대비 우선주 할인이 확대된 상태, 100 위면 축소된 상태입니다. "),
            _rt("장기 추세와 단기 급등락을 구분해 볼 수 있습니다.", color="gray"),
        ], "💡", "default"))

        blocks.append({"type": "heading_3", "heading_3": {"rich_text": [_rt("KPRI-전체")]}})
        blocks.append({
            "type": "image",
            "image": {"type": "external", "external": {"url": self._kpri_chart_url("KPRI-전체", kpri_all["dates"], kpri_all["values"], "#2563eb")}},
        })

        blocks.append({"type": "heading_3", "heading_3": {"rich_text": [_rt("KPRI-Top20")]}})
        blocks.append({
            "type": "image",
            "image": {"type": "external", "external": {"url": self._kpri_chart_url("KPRI-Top20", kpri_top20["dates"], kpri_top20["values"], "#dc2626")}},
        })

        # Top20 구성 종목 리스트 (시가총액 내림차순)
        blocks.append({"type": "divider", "divider": {}})
        blocks.append({"type": "heading_2", "heading_2": {"rich_text": [_rt("🏆 KPRI-Top20 구성 종목")]}})
        blocks.append(_callout([
            _rt("KPRI-Top20 지수 계산에 포함된 ", color="gray"),
            _rt("우선주 시가총액 상위 20개 종목", bold=True),
            _rt("입니다. 시가총액 내림차순으로 정렬됩니다.", color="gray"),
        ], "💡", "default"))

        top20_pairs = data.get("top20_pairs", [])
        top20_rows = [{
            "type": "table_row",
            "table_row": {"cells": [
                [_rt("#", bold=True)],
                [_rt("종목명", bold=True)],
                [_rt("우선주 종가", bold=True)],
                [_rt("보통주 종가", bold=True)],
                [_rt("비율", bold=True)],
                [_rt("시가총액(억)", bold=True)],
            ]},
        }]
        for i, p in enumerate(top20_pairs, 1):
            mcap_ok = int(p["preferred_marcap"] / 1_0000_0000) if p["preferred_marcap"] else 0
            ratio_color = "red" if p.get("is_reversed") else "blue"
            top20_rows.append({
                "type": "table_row",
                "table_row": {"cells": [
                    [_rt(str(i), bold=True)],
                    [_rt(p["preferred_name"], bold=True)],
                    [_rt(f"{p['preferred_price']:,}")],
                    [_rt(f"{p['common_price']:,}")],
                    [_rt(_fmt_num(p["ratio"], 4), bold=True, color=ratio_color)],
                    [_rt(f"{mcap_ok:,}")],
                ]},
            })
        blocks.append({
            "type": "table",
            "table": {
                "table_width": 6,
                "has_column_header": True,
                "has_row_header": False,
                "children": top20_rows,
            },
        })

        # 상단 블록 먼저 추가
        self._append_blocks(self.index_page_id, blocks)

        # 기여도 섹션 (2열)
        self._append_blocks(self.index_page_id, [
            {"type": "divider", "divider": {}},
            {"type": "heading_2", "heading_2": {"rich_text": [_rt("🎯 기여도 분석 (3개월 대비)")]}},
            _callout([
                _rt("기여도란?\n", bold=True),
                _rt("3개월 전 대비 지수 변화에 각 종목이 얼마나 기여했는지를 나타냅니다. "),
                _rt("계산: 기여도 = 종목 비중 × (오늘 비율 − 3개월전 비율). "),
                _rt("값이 큰 (+) 종목은 지수를 밀어올린 주역, 큰 (−) 종목은 지수를 끌어내린 주역입니다. "),
                _rt("Top 5는 가장 긍정적으로 기여한 종목, Worst 5는 가장 부정적으로 기여한 종목입니다.", color="gray"),
            ], "💡", "default"),
        ])

        def contrib_column(title, emoji, items, color):
            children = [
                {"type": "heading_3", "heading_3": {"rich_text": [_rt(f"{emoji} {title}")]}},
            ]
            rows = [{
                "type": "table_row",
                "table_row": {"cells": [
                    [_rt("종목", bold=True)],
                    [_rt("3개월전", bold=True)],
                    [_rt("현재", bold=True)],
                    [_rt("변화", bold=True)],
                    [_rt("기여도", bold=True)],
                ]},
            }]
            for item in items:
                change_color = "red" if item["change"] < 0 else "green"
                rows.append({
                    "type": "table_row",
                    "table_row": {"cells": [
                        [_rt(item["preferred_name"], bold=True)],
                        [_rt(_fmt_num(item.get("ratio_past"), 4))],
                        [_rt(_fmt_num(item["ratio_today"], 4))],
                        [_rt(_fmt_change(item["change"]), color=change_color)],
                        [_rt(_fmt_change(item["contribution"] * 100), bold=True, color=color)],
                    ]},
                })
            children.append({
                "type": "table",
                "table": {
                    "table_width": 5,
                    "has_column_header": True,
                    "has_row_header": False,
                    "children": rows,
                },
            })
            return {"type": "column", "column": {"children": children}}

        # 4개 섹션을 세로로 나열 (1열 × 4행)
        def contrib_section(title, emoji, items, color):
            col = contrib_column(title, emoji, items, color)
            return col["column"]["children"]

        all_blocks = []
        all_blocks.extend(contrib_section("KPRI-전체 기여도 Top 5", "🏆", data["top5_all"], "green"))
        all_blocks.extend(contrib_section("KPRI-전체 기여도 Worst 5", "💔", data["worst5_all"], "red"))
        all_blocks.extend(contrib_section("KPRI-Top20 기여도 Top 5", "🥇", data["top5_top20"], "green"))
        all_blocks.extend(contrib_section("KPRI-Top20 기여도 Worst 5", "❌", data["worst5_top20"], "red"))

        self._append_blocks(self.index_page_id, all_blocks)

        logger.info("KPRI 인덱스 페이지 업데이트 완료")

    # ─── Entry Point ───────────────────────────────────────

    def update_data(self, data):
        """전체 대시보드 업데이트"""
        logger.info("=== Notion 업데이트 시작 ===")

        # 1. 메인 페이지 제목/아이콘
        self._set_main_title()

        # 2. 서브 페이지 확보 (없으면 생성)
        self._ensure_sub_pages()

        # 3. DB 확보 (없으면 생성)
        self._ensure_database()

        # 4. 메인 페이지 콘텐츠 업데이트
        self._update_main_page(data)
        logger.info("메인 페이지 업데이트 완료")

        # 5. 인덱스 페이지 업데이트
        self._update_index_page(data)

        # 6. 우선주 전체 페이지 (DB rows + 종목별 상세) 업데이트
        result = self._update_list_page(data)

        logger.info(f"전체 업데이트 완료: {result}")
        return result
