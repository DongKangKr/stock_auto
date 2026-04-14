"""
우선주 KPRI 대시보드 - 메인 스크립트
- KRX 종가 수집 + 2020.01.02 ~ 현재 히스토리 (캐시 사용)
- KPRI-전체 / KPRI-Top20 상대가치 지수 계산
- 기여도 Top 5 / Worst 5 분석
- Notion 대시보드 업데이트
"""

import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv

from stock_data import fetch_all
from notion_updater import NotionUpdater

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(Path(__file__).parent / "update.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


def load_env():
    for p in [Path(__file__).parent.parent / ".env", Path(__file__).parent / ".env"]:
        if p.exists():
            load_dotenv(p)
            logger.info(f".env 로드: {p}")
            return
    logger.warning(".env 파일을 찾을 수 없습니다")


def main():
    load_env()

    notion_token = os.environ.get("NOTION_TOKEN")
    notion_page_id = os.environ.get("NOTION_PAGE_ID")

    if not notion_token or not notion_page_id:
        logger.error("NOTION_TOKEN, NOTION_PAGE_ID 환경변수가 필요합니다")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("KPRI 대시보드 업데이트 시작")
    logger.info("=" * 60)

    # 1. 데이터 수집 + 지수 계산
    try:
        data = fetch_all()
    except Exception as e:
        logger.error(f"데이터 수집 실패: {e}", exc_info=True)
        sys.exit(1)

    kpri_all = data["kpri_all"]
    kpri_top20 = data["kpri_top20"]
    logger.info(
        f"KPRI-전체: {kpri_all['current']} (전일비 {kpri_all['change_today']:+.2f}), "
        f"KPRI-Top20: {kpri_top20['current']} (전일비 {kpri_top20['change_today']:+.2f})"
    )

    # 2. Notion 업데이트
    try:
        notion = NotionUpdater(notion_token, notion_page_id)
        result = notion.update_data(data)
        logger.info(f"Notion 업데이트 완료: {result}")
    except Exception as e:
        logger.error(f"Notion 업데이트 실패: {e}", exc_info=True)
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("전체 프로세스 완료")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
