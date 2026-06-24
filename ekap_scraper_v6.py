import asyncio
import json
import logging
import re
import sys
from pathlib import Path
import pandas as pd
from tqdm import tqdm

# Допустим, библиотека ihale_client установлена или лежит в папке
REPO_DIR = Path(__file__).parent / "ihale-mcp"
sys.path.insert(0, str(REPO_DIR))

try:
    from ihale_client import EKAPClient
except ImportError:
    print("❌ ihale_client не найден! Установите ihale-mcp:")
    print("   pip install git+https://github.com/saidsurucu/ihale-mcp")
    print("   или клонируйте репозиторий рядом со скриптом")
    sys.exit(1)

# ── КОНФИГУРАЦИЯ ──────────────────────────────────────────────────────────────
OUTPUT_DIR = Path(__file__).parent / "ekap_output"
OUTPUT_DIR.mkdir(exist_ok=True)

CONCURRENCY = 5  # Параллельных запросов
SEM = asyncio.Semaphore(CONCURRENCY)

KEYWORDS = ["bilişim", "yazılım", "bilgisayar", "donanım"]  # IT-тематика
YEAR = 2025

MAX_PAGES = None  # None = без лимита. Для теста поставьте 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(OUTPUT_DIR / "scraper.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


# ── ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ───────────────────────────────────────────────────
def _num(s):
    """Парсит турецкий числовой формат (1.234.567,89) в float."""
    try:
        return float(str(s).replace(".", "").replace(",", "."))
    except Exception:
        return None


# ── ПАРСИНГ ОБЪЯВЛЕНИЯ О РЕЗУЛЬТАТАХ (тип 4) ──────────────────────────────────
def parse_result_ilan(announcements):
    """Извлекает данные из объявления о результатах (Sonuç İlanı, тип "4")."""
    out = {
        "estimated_cost": None,
        "contract_value": None,
        "winner": None,
        "winner_vkn": None,
        "num_bidders": None,
    }
    for ann in announcements:
        if ann.get("type", {}).get("code") != "4":
            continue

        md = ann.get("markdown_content", "")

        m_est = re.search(r"Yaklaşık Maliyeti.*?([\d.,]+)\s*TRY", md)
        if m_est:
            out["estimated_cost"] = _num(m_est.group(1))

        m_con = re.search(r"Bedeli.*?([\d.,]+)\s*TRY", md)
        if m_con:
            out["contract_value"] = _num(m_con.group(1))

        m_win = re.search(r"Yüklenici.*?\|\s*:\s*\|\s*([^\|\n]+)", md)
        if m_win:
            out["winner"] = m_win.group(1).strip()

        m_vkn = re.search(r"VKN\s*/\s*TCKN.*?\|\s*:\s*\|\s*(\d+)", md)
        if m_vkn:
            out["winner_vkn"] = m_vkn.group(1).strip()

        m_bid = re.search(
            r"Teklif\s+(?:Veren\s+)?(?:İstekli\s+)?Sayısı.*?(\d+)", md, re.IGNORECASE
        )
        if m_bid:
            out["num_bidders"] = int(m_bid.group(1))

        break

    return out


# ── СБОРКА ЗАПИСИ ─────────────────────────────────────────────────────────────
def build_record(item, details, announcements):
    """Собирает строку датасета из трёх источников."""
    td = (details or {}).get("tender_details", {})
    basic = td.get("basic_info", {})
    authority = td.get("authority", {})
    ann_list = (announcements or {}).get("announcements", [])

    parsed = parse_result_ilan(ann_list)

    bid_ratio = None
    if (
        parsed["contract_value"]
        and parsed["estimated_cost"]
        and parsed["estimated_cost"] > 0
    ):
        bid_ratio = parsed["contract_value"] / parsed["estimated_cost"]

    return {
        "tender_id": item.get("id"),
        "ikn": item.get("ikn"),
        "announcement_date": item.get("tender_datetime"),
        "procurement_type": basic.get("type_description"),
        "procedure_type": basic.get("method_description"),
        "authority": authority.get("name"),
        "region": authority.get("province"),
        "num_bidders": parsed["num_bidders"],
        "is_e_tender": basic.get("is_electronic"),
        "estimated_cost": parsed["estimated_cost"],
        "contract_value": parsed["contract_value"],
        "bid_ratio": bid_ratio,
        "winner_name": parsed["winner"],
        "winner_vkn": parsed["winner_vkn"],
    }


# ── ЗАПРОСЫ С ПОВТОРНЫМИ ПОПЫТКАМИ ────────────────────────────────────────────
async def fetch_with_retry(client, tender_id):
    """Получает детали и объявления с повторными попытками."""
    async with SEM:
        for attempt in range(3):
            try:
                d_task = client.get_tender_details(tender_id=tender_id)
                a_task = client.get_tender_announcements(tender_id=tender_id)
                details, announcements = await asyncio.gather(d_task, a_task)
                return details, announcements
            except Exception as e:
                log.warning(f"Retry {attempt + 1}/3 for {tender_id}: {e}")
                await asyncio.sleep(2 * (attempt + 1))
        log.error(f"All retries failed for {tender_id}")
        return None, None


# ── ПАГИНАЦИЯ ─────────────────────────────────────────────────────────────────
async def collect_all_ids(client, keyword, year):
    """Собирает все ID за год через пагинацию."""
    all_items = []
    page_size = 100
    current_page = 0
    
    while True:
        log.info(f"Fetching page {current_page} for keyword '{keyword}'...")
        offset = current_page * page_size
        
        result = await client.search_tenders(
            search_text=keyword,
            tender_date_start=f"{year}-01-01",
            tender_date_end=f"{year}-12-31",
            limit=page_size,
            offset=offset,
        )

        tenders = result.get("tenders", [])
        if not tenders:
            log.info(f"No more results for '{keyword}' at page {current_page}.")
            break

        all_items.extend(tenders)
        log.info(f"  Got {len(tenders)} tenders (total so far: {len(all_items)})")

        if len(tenders) < page_size:
            break

        current_page += 1
        await asyncio.sleep(1)

        if MAX_PAGES and current_page >= MAX_PAGES:
            log.warning(f"Reached MAX_PAGES={MAX_PAGES} limit for '{keyword}'.")
            break

    return all_items


# ── MAIN
