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
from ihale_client import EKAPClient

# ── КОНФИГУРАЦИЯ ──────────────────────────────────────────────────────────────
OUTPUT_DIR = Path(__file__).parent / "ekap_output"
OUTPUT_DIR.mkdir(exist_ok=True)

CONCURRENCY = 5
SEM = asyncio.Semaphore(CONCURRENCY)
KEYWORDS = ["bilişim", "yazılım"]
YEAR = 2025

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(OUTPUT_DIR / "scraper.log"),
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
    """
    Извлекает данные из объявления о результатах (Sonuç İlanı, тип "4").
    Поля winner_* и num_bidders будут None для незавершённых тендеров.
    """
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

        # Приблизительная стоимость
        m_est = re.search(r"Yaklaşık Maliyeti.*?([\d.,]+)\s*TRY", md)
        if m_est:
            out["estimated_cost"] = _num(m_est.group(1))

        # Стоимость контракта
        m_con = re.search(r"Bedeli.*?([\d.,]+)\s*TRY", md)
        if m_con:
            out["contract_value"] = _num(m_con.group(1))

        # Победитель (Yüklenici)
        m_win = re.search(r"Yüklenici.*?\|\s*:\s*\|\s*([^\|\n]+)", md)
        if m_win:
            out["winner"] = m_win.group(1).strip()

        # VKN/TCKN победителя
        m_vkn = re.search(r"VKN\s*/\s*TCKN.*?\|\s*:\s*\|\s*(\d+)", md)
        if m_vkn:
            out["winner_vkn"] = m_vkn.group(1).strip()

        # Количество участников
        m_bid = re.search(
            r"Teklif\s+(?:Veren\s+)?(?:İstekli\s+)?Sayısı.*?(\d+)", md, re.IGNORECASE
        )
        if m_bid:
            out["num_bidders"] = int(m_bid.group(1))

        break  # Берём только первое объявление типа 4

    return out


# ── СБОРКА ЗАПИСИ ─────────────────────────────────────────────────────────────
def build_record(item, details, announcements):
    """
    Собирает строку датасета из трёх источников:
      - item: результат поиска (search_tenders)
      - details: get_tender_details -> tender_details
      - announcements: get_tender_announcements -> announcements

    Реальная структура tender_details (проверено по API):
      tender_details.basic_info.type_description   — тип закупки
      tender_details.basic_info.method_description — процедура
      tender_details.basic_info.is_electronic      — e-тендер
      tender_details.authority.name                — орган
      tender_details.authority.province            — регион
    """
    td = (details or {}).get("tender_details", {})
    basic = td.get("basic_info", {})
    authority = td.get("authority", {})
    ann_list = (announcements or {}).get("announcements", [])

    parsed = parse_result_ilan(ann_list)

    # Bid Ratio = contract_value / estimated_cost
    bid_ratio = None
    if (
        parsed["contract_value"]
        and parsed["estimated_cost"]
        and parsed["estimated_cost"] > 0
    ):
        bid_ratio = parsed["contract_value"] / parsed["estimated_cost"]

    return {
        "tender_id":        item.get("id"),
        "ikn":              item.get("ikn"),
        # tender_datetime приходит из search-результата напрямую
        "announcement_date": item.get("tender_datetime"),
        # Тип закупки и процедура — в basic_info (не в type/procedure верхнего уровня)
        "procurement_type": basic.get("type_description"),
        "procedure_type":   basic.get("method_description"),
        # Орган и регион — в authority (не в administration/place_of_delivery)
        "authority":        authority.get("name"),
        "region":           authority.get("province"),
        # Количество участников — только из Sonuç İlanı
        "num_bidders":      parsed["num_bidders"],
        # e-тендер — в basic_info (не is_e_tender верхнего уровня)
        "is_e_tender":      basic.get("is_electronic"),
        "estimated_cost":   parsed["estimated_cost"],
        "contract_value":   parsed["contract_value"],
        "bid_ratio":        bid_ratio,
        "winner_name":      parsed["winner"],
        "winner_vkn":       parsed["winner_vkn"],
    }


# ── ЗАПРОСЫ С ПОВТОРНЫМИ ПОПЫТКАМИ ────────────────────────────────────────────
async def fetch_with_retry(client, tender_id):
    async with SEM:
        for attempt in range(3):
            try:
                d_task = client.get_tender_details(tender_id=tender_id)
                a_task = client.get_tender_announcements(tender_id=tender_id)
                return await asyncio.gather(d_task, a_task)
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
    MAX_PAGES = 5  # Убрать в продакшене

    while True:
        log.info(f"Fetching page {current_page} for keyword '{keyword}'...")
        result = await client.search_tenders(
            search_text=keyword,
            tender_date_start=f"{year}-01-01",
            tender_date_end=f"{year}-12-31",
            limit=page_size,
            # offset=current_page * page_size  # раскомментировать если API поддерживает
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

        if current_page >= MAX_PAGES:
            log.warning(f"Reached MAX_PAGES={MAX_PAGES} limit for '{keyword}'.")
            break

    return all_items


# ── MAIN ──────────────────────────────────────────────────────────────────────
async def main():
    client = EKAPClient()
    all_tender_items = []

    # 1. Собираем ID по всем ключевым словам
    for kw in KEYWORDS:
        items = await collect_all_ids(client, kw, YEAR)
        all_tender_items.extend(items)

    # Дедупликация по ID
    unique_map = {i["id"]: i for i in all_tender_items}
    unique_items = list(unique_map.values())
    log.info(f"Total unique tenders found: {len(unique_items)}")

    # 2. Параллельно получаем детали и объявления
    tasks = [fetch_with_retry(client, item["id"]) for item in unique_items]

    results = []
    for f in tqdm(
        asyncio.as_completed(tasks), total=len(tasks), desc="Parsing details"
    ):
        results.append(await f)

    # 3. Сборка датасета
    # Примечание: as_completed не гарантирует порядок, поэтому сопоставляем
    # через отдельный словарь (см. улучшенный вариант ниже).
    # Простой вариант — если порядок важен, используйте asyncio.gather вместо as_completed.
    records = []
    for item, (details, announcements) in zip(unique_items, results):
        if details is not None:
            rec = build_record(item, details, announcements)
            records.append(rec)
        else:
            log.warning(f"Skipped tender {item.get('id')} — no details fetched.")

    df = pd.DataFrame(records)

    # Статистика заполненности
    log.info("── Column fill rates ──────────────────────────")
    for col in df.columns:
        filled = df[col].notna().sum()
        log.info(f"  {col:20s}: {filled}/{len(df)} ({filled/len(df)*100:.1f}%)")
    log.info("───────────────────────────────────────────────")

    # Сохранение полного датасета
    filename = f"tenders_turkey_it_{YEAR}.csv"
    df.to_csv(OUTPUT_DIR / filename, index=False, encoding="utf-8-sig")
    log.info(f"Saved {len(df)} records to {filename}")

    # Дополнительно: только завершённые тендеры (есть победитель)
    df_completed = df[df["winner_name"].notna()].copy()
    if not df_completed.empty:
        filename_c = f"tenders_turkey_it_{YEAR}_completed.csv"
        df_completed.to_csv(OUTPUT_DIR / filename_c, index=False, encoding="utf-8-sig")
        log.info(f"Saved {len(df_completed)} completed tenders to {filename_c}")


if __name__ == "__main__":
    asyncio.run(main())