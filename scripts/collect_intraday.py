"""
Thu thập dữ liệu giao dịch trong phiên (intraday) cho VN30 + top cổ phiếu.

Nguồn: KBS Quote.intraday() - tick-by-tick matching data
Fallback: vnstock_pipeline run_intraday_task (nếu có)

Output:
    data/intraday/
    ├── VCB.csv     - Dữ liệu khớp lệnh trong phiên VCB
    ├── FPT.csv     - Dữ liệu khớp lệnh trong phiên FPT
    └── ... (VN30 + top 50)

Cách chạy:
    python scripts/collect_intraday.py                  # VN30 stocks
    python scripts/collect_intraday.py --top-n 100      # Top 100 mã
    python scripts/collect_intraday.py --symbols VCB FPT # Chỉ mã cụ thể
"""

import sys
import logging
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
from utils import init_rate_limiter, get_limiter, is_file_fresh

# ============================================================
# CẤU HÌNH
# ============================================================

DATA_DIR = PROJECT_ROOT / "data" / "intraday"
MAX_WORKERS = 5
MAX_PAGES = 20  # Mỗi mã tối đa 20 pages x 100 records = 2000 ticks

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("intraday")

_api_lock = threading.Lock()


def _rate_limited_call(func):
    """Call func with global rate limiter (thread-safe)."""
    with _api_lock:
        get_limiter().wait()
    return func()


# ============================================================
# LẤY DANH SÁCH MÃ
# ============================================================

def get_symbols(top_n: int = 50, specific: list = None) -> list:
    """Lấy danh sách mã: VN30 + top N vốn hóa."""
    if specific:
        return specific

    from vnstock.common.client import Vnstock

    client = Vnstock(source="VCI", show_log=False)
    stock = client.stock(symbol="ACB", source="VCI")

    symbols = set()

    # VN30
    try:
        vn30 = stock.listing.symbols_by_group(group="VN30", show_log=False)
        if vn30 is not None and "symbol" in vn30.columns:
            symbols.update(vn30["symbol"].tolist())
            logger.info(f"  VN30: {len(vn30)} mã")
    except Exception as e:
        logger.warning(f"  VN30 lỗi: {e}")

    # Top N vốn hóa
    try:
        all_syms = stock.listing.symbols_by_exchange(show_log=False)
        extra = [s for s in all_syms["symbol"].tolist() if s not in symbols]
        symbols.update(extra[:max(0, top_n - len(symbols))])
    except Exception as e:
        logger.warning(f"  Listing lỗi: {e}")

    result = sorted(symbols)
    logger.info(f"  Tổng: {len(result)} mã")
    return result


# ============================================================
# FETCH INTRADAY
# ============================================================

def fetch_intraday_one(symbol: str) -> pd.DataFrame:
    """Fetch intraday data cho 1 mã từ KBS (tất cả pages)."""
    from vnstock.common.client import Vnstock

    try:
        client = Vnstock(source="KBS", show_log=False)
        stock = client.stock(symbol=symbol, source="KBS")

        all_pages = []
        for page in range(1, MAX_PAGES + 1):
            df = _rate_limited_call(
                lambda p=page: stock.quote.intraday(
                    page_size=100, page=p, get_all=True, show_log=False
                )
            )
            if df is None or df.empty:
                break
            all_pages.append(df)
            if len(df) < 100:
                break

        if not all_pages:
            return pd.DataFrame()

        combined = pd.concat(all_pages, ignore_index=True)
        combined["symbol"] = symbol
        combined = combined.drop_duplicates(subset=["time", "price", "volume"], keep="first")
        combined = combined.sort_values("time").reset_index(drop=True)
        return combined

    except Exception as e:
        logger.debug(f"  {symbol}: intraday lỗi - {e}")
        return pd.DataFrame()


# ============================================================
# MAIN COLLECTOR
# ============================================================

def collect_intraday(symbols: list):
    """Thu thập intraday cho danh sách mã (song song)."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    today = datetime.now().strftime("%Y-%m-%d")
    total = len(symbols)
    success = 0
    skipped = 0
    errors = 0
    completed = 0

    # Check which files need update
    need_update = []
    for sym in symbols:
        csv_path = DATA_DIR / f"{sym}.csv"
        if is_file_fresh(csv_path, max_age_hours=6):
            skipped += 1
        else:
            need_update.append(sym)

    if skipped > 0:
        logger.info(f"  Bỏ qua {skipped} mã đã có dữ liệu hôm nay")

    if not need_update:
        logger.info("  Tất cả đã có, không cần cập nhật.")
        return True

    logger.info(f"  Cần cập nhật: {len(need_update)} mã ({MAX_WORKERS} threads)")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_intraday_one, sym): sym for sym in need_update}

        for future in as_completed(futures):
            completed += 1
            symbol = futures[future]

            if completed % 10 == 0 or completed == len(need_update):
                logger.info(f"  [{completed}/{len(need_update)}] (OK: {success}, lỗi: {errors})")

            try:
                df = future.result()
                if df is not None and not df.empty:
                    csv_path = DATA_DIR / f"{symbol}.csv"

                    # Append to existing data
                    if csv_path.exists():
                        try:
                            existing = pd.read_csv(csv_path)
                            df = pd.concat([existing, df], ignore_index=True)
                            df = df.drop_duplicates(
                                subset=["time", "price", "volume"], keep="last"
                            )
                            df = df.sort_values("time").reset_index(drop=True)
                        except Exception:
                            pass

                    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
                    success += 1
                else:
                    errors += 1
            except Exception:
                errors += 1

    logger.info(f"\nKết quả: {success}/{len(need_update)} mã, {errors} lỗi, {skipped} bỏ qua")
    return success > 0


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Thu thập dữ liệu giao dịch trong phiên (intraday).",
    )
    parser.add_argument("--top-n", type=int, default=50,
                        help="Số mã (VN30 + top vốn hóa, mặc định: 50)")
    parser.add_argument("--symbols", nargs="+", default=None,
                        help="Chỉ lấy mã cụ thể")
    args = parser.parse_args()

    init_rate_limiter()

    logger.info("=" * 60)
    logger.info("THU THẬP DỮ LIỆU INTRADAY")
    logger.info(f"Nguồn: KBS Quote.intraday()")
    logger.info(f"Output: {DATA_DIR}")
    logger.info("=" * 60)

    # 1. Lấy danh sách mã
    logger.info("\n[1/2] XÁC ĐỊNH DANH SÁCH MÃ")
    symbols = get_symbols(top_n=args.top_n, specific=args.symbols)

    if not symbols:
        logger.error("Không lấy được danh sách mã.")
        return

    # 2. Fetch intraday
    logger.info(f"\n[2/2] FETCH INTRADAY ({len(symbols)} mã)")
    collect_intraday(symbols)

    logger.info("\n" + "=" * 60)
    logger.info("HOÀN TẤT!")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
