# 📈 KPRI 우선주 대시보드

한국거래소(KRX)에 상장된 우선주 보유 회사들의 보통주/우선주 가격 관계를 추적하고, **KPRI 상대가치 지수**를 산출하여 Notion 대시보드로 매일 자동 업데이트합니다.

> 🏦 KPRI = **K**orea **P**referred stock **R**elative **I**ndex (우선주 상대가치 지수)

---

## 📌 이 프로젝트는 무엇인가요?

한국 주식시장에서 **보통주**와 **우선주**는 같은 회사가 발행하지만 가격이 다릅니다.
- 일반적으로 우선주가 보통주보다 **싸게(할인) 거래**됩니다
- 이 할인율이 얼마나 큰지, 시간에 따라 어떻게 변하는지를 **수치화·시각화**하는 것이 이 프로젝트의 목적입니다

**매일 장 마감 후** 자동으로:
1. KRX에서 전 종목 종가 수집
2. 우선주 107개와 해당 보통주 페어 매칭
3. 각 종목의 "보통주 대비 비율" 계산
4. KPRI 지수 산출 (기준일 2020.01.02 = 100)
5. Notion 대시보드 업데이트

---

## 🎯 주요 기능

### 1. 우선주 전체 목록
- 상장 우선주 **107종목** 전체 리스트
- 컬럼: 종목명, 우선주/보통주 종가, 보통주 대비 비율, 3개월전 비율, 시가총액, 배당수익률, 발행주식수 비율 등
- Notion 데이터베이스로 검색/필터/정렬 가능

### 2. 종목별 상세 페이지
각 종목 행을 클릭하면 자동 생성되는 상세 페이지에:
- 📋 요약 (현재가, 비율, 시가총액 등)
- 📈 1년 가격 추이 차트 (보통주 vs 우선주)
- 📊 1년 비율 추이 차트
- 📋 비율 비교표 (12/6/3개월전 vs 현재)
- 📋 최근 10거래일 종가 테이블

### 3. KPRI 인덱스 페이지
- **KPRI-전체**: 107개 우선주 전체 기준 지수
- **KPRI-Top20**: 우선주 시가총액 상위 20개 기준 지수
- 시점별 비교 (12/6/3개월전 / 현재)
- 2020.01.02부터의 시계열 차트
- 기여도 Top 5 / Worst 5 (3개월 대비)
- KPRI-Top20 구성 종목 리스트

---

## 🧮 KPRI 지수 계산 공식

```
① 각 종목의 보통주 대비 비율 = 우선주 종가 ÷ 보통주 종가
② 전체 종목의 비율을 단순 평균
③ 오늘 지수 = (오늘 평균 비율 ÷ 기준일 평균 비율) × 100
```

- **기준일**: 2020-01-02 = 100
- **해석**: 숫자가 100보다 낮으면 기준일 대비 우선주 할인이 확대된 상태, 높으면 축소된 상태

### 예시
```
기준일(2020.01.02) 평균 비율: 0.75
오늘 평균 비율: 0.64
KPRI = (0.64 / 0.75) × 100 = 85.33

→ 기준일보다 약 14.67% 만큼 우선주 할인이 확대됨
```

---

## 📂 파일 구조

```
stock_auto/
├── .github/
│   └── workflows/
│       └── daily_update.yml      # GitHub Actions 자동화 설정
├── main.py                        # 메인 진입점 (전체 흐름 제어)
├── stock_data.py                  # KRX 데이터 수집 + KPRI 계산
├── notion_updater.py              # Notion API 연동 (3페이지 업데이트)
├── requirements.txt               # Python 라이브러리 목록
├── .gitignore                     # Git 제외 파일
└── README.md                      # 이 문서
```

### 자동 생성되는 파일 (gitignore)
```
history_cache.pkl      # 2020-01-02부터 일간 종가 캐시
dividend_cache.pkl     # 네이버 배당수익률 캐시 (일 단위)
db_config.json         # Notion 서브 페이지/DB ID 저장
update.log             # 실행 로그
```

---

## 🔄 코드 흐름 (어떻게 작동하나요?)

### 전체 실행 순서

```
main.py 실행
   ↓
[1] load_env()               # 환경변수 로드
   ↓
[2] stock_data.fetch_all()   # 데이터 수집 + 지수 계산
   ↓
[3] NotionUpdater.update_data()  # Notion 업데이트
```

### `stock_data.fetch_all()` 내부 흐름

```python
fetch_pairs()                # KRX 전 종목 조회 → 우선주 페어 매칭 (107개)
fetch_historical_data()      # 각 페어의 2020-01-02 ~ 현재 일간 종가 조회 (캐시)
fetch_dividend_yields()      # Naver Finance에서 배당수익률 크롤링 (캐시)
_get_ratio_days_ago()        # 각 종목의 3/6/12개월전 비율 계산
calculate_kpri_index()       # KPRI-전체, KPRI-Top20 지수 계산
calculate_contributions()    # 3개월 대비 기여도 Top5/Worst5 계산
```

### `NotionUpdater.update_data()` 내부 흐름

```python
_set_main_title()              # 메인 페이지 제목/아이콘 설정
_ensure_sub_pages()            # '우선주 전체' + 'KPRI 인덱스' 서브 페이지 확보
_cleanup_duplicate_sub_pages() # 중복 서브 페이지 자동 정리
_ensure_database()             # 우선주 전체 DB 확보 (없으면 생성)
_update_main_page(data)        # 메인 랜딩 페이지 업데이트
_update_index_page(data)       # KPRI 인덱스 페이지 업데이트
_update_list_page(data)        # 우선주 전체 DB 업데이트 + 107개 상세 페이지 생성
```

---

## 🧩 핵심 모듈 설명

### `main.py` (39줄)
가장 단순한 파일. 환경변수 로드 후 `stock_data.fetch_all()` → `NotionUpdater.update_data()` 순서로 호출합니다.

### `stock_data.py`
KRX 데이터를 가져오고 KPRI 지수를 계산합니다.

| 함수 | 역할 |
|---|---|
| `fetch_pairs()` | FinanceDataReader로 전 종목 리스트 조회, 이름 끝의 '우/우B/1우/2우' 등을 감지해 우선주-보통주 페어 매칭 |
| `fetch_historical_data()` | 각 종목의 일간 종가 수집 (2020-01-02 ~ 오늘). 캐시 사용으로 증분 업데이트 |
| `fetch_dividend_yields()` | 네이버 파이낸스(`finance.naver.com/item/main.naver?code=XXX`)에서 배당수익률 HTML 파싱. BeautifulSoup 사용 |
| `calculate_kpri_index()` | 기준일 대비 상대가치 지수 계산. 전체 107종목 / 시총 Top20 두 가지 산출 |
| `calculate_contributions()` | 3개월 대비 각 종목의 지수 변화 기여도 계산 (Top5, Worst5) |

### `notion_updater.py`
Notion API를 호출해 3페이지 구조를 만들고 업데이트합니다.

| 섹션 | 역할 |
|---|---|
| `_request()` | Notion API 공통 호출 (rate limit 자동 대기, 429 재시도) |
| `_ensure_sub_pages()` | '우선주 전체', 'KPRI 인덱스' 서브 페이지가 없으면 생성, 있으면 재사용 |
| `_create_database()` | Notion 인라인 DB 생성 (컬럼 14개, 원숫자 ①~⑭ 접두사로 순서 강제) |
| `_build_stock_detail_blocks()` | 각 종목의 상세 페이지 내용 (차트 + 테이블) 생성 |
| `_kpri_chart_url()` | QuickChart.io URL 생성으로 시계열 차트 임베딩 |

### Notion API의 특이점

#### 1. 컬럼 순서 문제
Notion DB는 컬럼을 **가나다 순으로 자동 정렬**합니다. 이를 강제로 원하는 순서로 배치하기 위해 **원숫자(①②③...)** 를 컬럼명 앞에 붙여 사용합니다.

```python
COL = {
    "title": "종목명",         # title은 항상 첫번째
    "pref_price": "①우선주 종가",
    "common_price": "②보통주 종가",
    ...
}
```

#### 2. 시총 내림차순 정렬
Notion DB 기본 뷰는 **생성 시간 역순(최신 먼저)**으로 표시됩니다. 시총 큰 종목을 상단에 보이게 하려면 **시총 작은 것부터 삽입**해야 합니다.

```python
# data["pairs"]는 시총 내림차순이므로 reversed로 순회
insertion_order = list(reversed(pairs))
```

#### 3. 차트 임베딩
Notion API는 이미지 URL을 2000자 이하로 제한합니다. QuickChart.io로 차트를 생성할 때 데이터를 20포인트로 샘플링해 URL 길이를 맞춥니다.

---

## 🚀 GitHub Actions 자동화 세팅

### 1. 저장소 준비
이 코드를 자신의 GitHub 저장소에 Push

### 2. Notion Integration 발급
1. https://www.notion.so/my-integrations 접속
2. "New integration" → 워크스페이스 선택
3. "Internal Integration Token" 복사 (`ntn_...` 또는 `secret_...`)

### 3. Notion 페이지 준비
1. 대시보드를 넣을 빈 페이지 생성
2. 페이지 우측 상단 `…` → **연결 추가** → 위에서 만든 Integration 선택
3. 페이지 URL 끝의 32자 ID 복사 (예: `342a44484ead806ea3f5f44f5c1a94a8`)

### 4. GitHub Secrets 등록
저장소 → `Settings` → `Secrets and variables` → `Actions` → `New repository secret`

| Name | Value |
|---|---|
| `NOTION_TOKEN` | Notion Integration Token |
| `NOTION_PAGE_ID` | 페이지 ID (32자) |

### 5. Workflow 실행
- **자동**: 매 평일 KST 17:00 (UTC 08:00) 자동 실행
- **수동**: `Actions` 탭 → "Daily KPRI Dashboard Update" → `Run workflow`

### 스케줄 (.github/workflows/daily_update.yml)
```yaml
on:
  schedule:
    - cron: '0 8 * * 1-5'   # UTC 08:00 = KST 17:00 (평일만)
  workflow_dispatch:          # 수동 실행 허용
```

### 캐시 동작
첫 실행은 ~50분, 두 번째부터는 ~10분 (캐시 덕분)
```yaml
- name: Restore data cache
  uses: actions/cache/restore@v4
  with:
    path: |
      history_cache.pkl
      dividend_cache.pkl
      db_config.json
    restore-keys: kpri-data-
```

---

## 💻 로컬 실행

### 1. 의존성 설치
```bash
pip install -r requirements.txt
```

### 2. 환경변수 설정
상위 디렉토리에 `.env` 파일 생성:
```
NOTION_TOKEN=ntn_xxxxxxxxxxxxxxxxxxxxxxxx
NOTION_PAGE_ID=342a44484ead806ea3f5f44f5c1a94a8
```

또는 터미널에서 직접:
```bash
export NOTION_TOKEN=ntn_xxx
export NOTION_PAGE_ID=xxx
```

### 3. 실행
```bash
python main.py
```

---

## 📊 데이터 소스

| 데이터 | 소스 | 라이브러리 |
|---|---|---|
| 전 종목 리스트 + 종가 + 시총 | FinanceDataReader (KRX 스크래핑) | `finance-datareader` |
| 과거 일간 종가 | FinanceDataReader → Naver Finance | `finance-datareader` |
| 배당수익률 | Naver Finance HTML 파싱 | `beautifulsoup4` |
| Notion 페이지/DB | Notion API | `requests` |
| 차트 이미지 | QuickChart.io (Chart.js 렌더링) | URL 파라미터 |

---

## 🔧 트러블슈팅

### Q. `.env 파일을 찾을 수 없습니다` 경고
- 로컬: `.env` 파일 생성 필요
- GitHub Actions: 정상 동작 (Secrets이 환경변수로 자동 주입됨)

### Q. `Notion API error: 404 Could not find page`
- Integration이 해당 페이지에 연결되지 않음
- Notion 페이지에서 `…` → 연결 추가 → Integration 선택

### Q. Notion에 DB가 2개 나옴
- 자동 정리 로직(`_clear_existing_databases_on_list_page`)이 포함되어 있음
- 여전히 문제 있으면 `db_config.json`에서 `database_id`를 `null`로 수정 후 재실행

### Q. 시총 순서가 반대
- Notion DB의 기본 뷰 정렬을 확인
- 데이터 삽입 순서는 시총 오름차순 (작은 것부터)이며, Notion의 "생성 시간 내림차순" 기본 뷰에서 큰 시총이 상단에 표시되어야 함

### Q. 첫 실행이 너무 느림 (50분)
- 정상입니다. 107종목 × 상세 페이지 생성 + 캐시 없음
- 두 번째 실행부터는 10분 이내로 완료

---

## 🎨 Notion 페이지 구조

```
📈 국내 상장사 보통주 vs 우선주 대시보드 (메인)
├── 📋 우선주 전체 (서브 페이지)
│   └── 📊 우선주 전체 DB (107행)
│       ├── 삼성전자우 (클릭 → 상세 페이지)
│       ├── 현대차2우B (클릭 → 상세 페이지)
│       └── ...
└── 📈 KPRI 인덱스 (서브 페이지)
    ├── KPRI-전체 / KPRI-Top20 배지
    ├── 시점별 비교 테이블
    ├── 시계열 차트 (전체 + Top20)
    ├── KPRI-Top20 구성 종목 리스트
    └── 기여도 분석 (Top5/Worst5 × 전체/Top20)
```

---

## 📝 라이선스

개인 프로젝트 / 내부 사용 목적
