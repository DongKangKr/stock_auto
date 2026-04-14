# KPRI 우선주 대시보드 자동 업데이트

한국거래소 상장 우선주 보유 회사의 보통주/우선주 가격 관계를 매일 Notion 대시보드로 자동 업데이트합니다.

## 구성

- **우선주 전체** 페이지: 107개 우선주 종목 테이블 (종가, 비율, 배당수익률, 시총 등)
- **종목별 정보** 페이지: 클릭 시 1년 가격 차트 + 비율 추이 + 과거 비교
- **KPRI 인덱스** 페이지: 상대가치 지수 (전체/Top20) + 시계열 차트 + 기여도 Top5/Worst5

## GitHub Actions 세팅

### 1. GitHub 저장소 생성
이 폴더를 새 GitHub 저장소로 푸시합니다.

### 2. Secrets 등록
저장소 `Settings` → `Secrets and variables` → `Actions` → `New repository secret`

| 이름 | 값 |
|---|---|
| `NOTION_TOKEN` | Notion Internal Integration Token (`ntn_...`) |
| `NOTION_PAGE_ID` | 대시보드를 넣을 Notion 페이지 ID (URL 끝 32자) |

### 3. Workflow 확인
`.github/workflows/daily_update.yml`

- 매 평일 **KST 17:00** (UTC 08:00) 자동 실행
- `Actions` 탭에서 **수동 실행** 가능 (`Run workflow`)

### 4. 최초 실행
Actions 탭에서 "Daily KPRI Dashboard Update" → "Run workflow"로 첫 실행 확인

## 로컬 실행

```bash
pip install -r requirements.txt
python main.py
```

환경변수 필요: `NOTION_TOKEN`, `NOTION_PAGE_ID` (또는 부모 디렉토리 `.env`)

## 파일 구성

| 파일 | 역할 |
|---|---|
| `main.py` | 메인 스크립트 (데이터 수집 → Notion 업데이트) |
| `stock_data.py` | KRX 데이터 수집, KPRI 지수 계산, 기여도 분석 |
| `notion_updater.py` | Notion API 3페이지 구조 생성/업데이트 |
| `requirements.txt` | Python 의존성 |
| `history_cache.pkl` | 일간 종가 캐시 (자동 관리) |
| `dividend_cache.pkl` | 배당수익률 캐시 (자동 관리) |
| `db_config.json` | Notion 서브페이지/DB ID 저장 |

## 데이터 소스

- **KRX 종목 리스트, OHLCV**: FinanceDataReader
- **배당수익률**: Naver Finance (HTML 파싱)
