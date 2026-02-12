"""
Thu thập dữ liệu công ty bổ sung: Reports, Ownership, Capital History, Affiliate.

Nguồn:
  - VCI Company: reports() - báo cáo phân tích CTCK
  - KBS Company: ownership() - cơ cấu cổ đông chi tiết
  - KBS Company: capital_history() - lịch sử tăng vốn điều lệ
  - KBS Company: affiliate() - công ty liên kết

Output:
    data/company_reports.csv     - Báo cáo phân tích từ CTCK (top 50 mã)
    data/company_ownership.csv   - Cơ cấu sở hữu chi tiết (top 50 mã)
    data/capital_history.csv     - Lịch sử thay đổi vốn điều lệ (top 50 mã)
    data/company_affiliate.csv   - Công ty liên kết (top 50 mã)

Cách chạy:
    python scripts/collect_company_extra.py                 # Tất cả (top 50)
    python scripts/collect_company_extra.py --top-n 100     # Top 100 mã
    python scripts/collect_company_extra.py --only reports ownership
"""

import sys
import logging
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
from utils import init_rate_limiter, get_limiter, is_file_fresh

# ============================================================
# CẤU HÌNH
# ============================================================

DATA_DIR = PROJECT_ROOT / "data"
MAX_WORKERS = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("company_extra")

_api_lock = threading.Lock()

COLLECT_TYPES = ["reports", "ownership", "capital_history", "affiliate"]


def _rate_limited_call(func):
    """Call func with global rate limiter (thread-safe)."""
    with _api_lock:
        get_limiter().wait()
    return func()


# ============================================================
# GENERIC CONCURRENT FETCHER
# ============================================================

def _get_symbols(source: str, top_n: int) -> list:
    """Get top N symbols from listing."""
    from vnstock.common.client import Vnstock
    client = Vnstock(source=source, show_log=False)
    stock = client.stock(symbol="ACB", source=source)
    symbols_df = stock.listing.symbols_by_exchange(show_log=False)
    return symbols_df["symbol"].tolist()[:top_n]


def _collect_concurrent(label, fetch_func, symbols, csv_path, add_symbol_col=True):
    """Generic concurrent fetcher for company data."""
    if is_file_fresh(csv_path):
        logger.info(f"  {csv_path.name} đã có hôm nay, bỏ qua {label}.")
        return True

    top_n = len(symbols)
    logger.info(f"  Đang lấy {label} cho {top_n} mã ({MAX_WORKERS} threads)...")

    all_data = []
    success = 0
    errors = 0
    completed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_func, sym): sym for sym in symbols}

        for future in as_completed(futures):
            completed += 1
            symbol = futures[future]
            if completed % 20 == 0 or completed == top_n:
                logger.info(f"    [{completed}/{top_n}] (OK: {success}, lỗi: {errors})")
            try:
                df = future.result()
                if df is not None and not df.empty:
                    if add_symbol_col and "symbol" not in df.columns:
                        df["symbol"] = symbol
                    all_data.append(df)
                success += 1
            except Exception:
                errors += 1

    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        combined.to_csv(csv_path, index=False, encoding="utf-8-sig")
        logger.info(f"    {label}: {len(combined)} rows → {csv_path.name}")
    else:
        logger.warning(f"    {label}: không có dữ liệu")

    logger.info(f"    Kết quả: {success}/{top_n} mã, {errors} lỗi")
    return success > 0


# ============================================================
# 1. COMPANY REPORTS (VCI)
# ============================================================

def collect_reports(top_n: int = 50):
    """Thu thập báo cáo phân tích từ CTCK (VCI source)."""
    symbols = _get_symbols("VCI", top_n)

    def fetch(symbol):
        from vnstock.common.client import Vnstock
        client = Vnstock(source="VCI", show_log=False)
        stock = client.stock(symbol=symbol, source="VCI")
        return _rate_limited_call(lambda: stock.company.reports())

    return _collect_concurrent(
        "Reports (VCI)", fetch, symbols, DATA_DIR / "company_reports.csv"
    )


# ============================================================
# 2. OWNERSHIP (KBS)
# ============================================================

def collect_ownership(top_n: int = 50):
    """Thu thập cơ cấu sở hữu chi tiết (KBS source)."""
    symbols = _get_symbols("KBS", top_n)

    def fetch(symbol):
        from vnstock.explorer.kbs.company import Company
        comp = Company(symbol, show_log=False)
        return _rate_limited_call(lambda: comp.ownership())

    return _collect_concurrent(
        "Ownership (KBS)", fetch, symbols, DATA_DIR / "company_ownership.csv"
    )


# ============================================================
# 3. CAPITAL HISTORY (KBS)
# ============================================================

def collect_capital_history(top_n: int = 50):
    """Thu thập lịch sử thay đổi vốn điều lệ (KBS source)."""
    symbols = _get_symbols("KBS", top_n)

    def fetch(symbol):
        from vnstock.explorer.kbs.company import Company
        comp = Company(symbol, show_log=False)
        return _rate_limited_call(lambda: comp.capital_history())

    return _collect_concurrent(
        "Capital History (KBS)", fetch, symbols, DATA_DIR / "capital_history.csv"
    )


# ============================================================
# 4. AFFILIATE (KBS)
# ============================================================

def collect_affiliate(top_n: int = 50):
    """Thu thập thông tin công ty liên kết (KBS source)."""
    symbols = _get_symbols("KBS", top_n)

    def fetch(symbol):
        from vnstock.explorer.kbs.company import Company
        comp = Company(symbol, show_log=False)
        return _rate_limited_call(lambda: comp.affiliate())

    return _collect_concurrent(
        "Affiliate (KBS)", fetch, symbols, DATA_DIR / "company_affiliate.csv"
    )


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Thu thập dữ liệu công ty bổ sung: Reports, Ownership, Capital History, Affiliate.",
    )
    parser.add_argument("--top-n", type=int, default=50,
                        help="Số mã (mặc định: 50)")
    parser.add_argument("--only", nargs="+", default=None,
                        choices=COLLECT_TYPES,
                        help="Chỉ lấy loại cụ thể")
    args = parser.parse_args()

    init_rate_limiter()
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    targets = args.only or COLLECT_TYPES

    logger.info("=" * 60)
    logger.info("THU THẬP DỮ LIỆU CÔNG TY BỔ SUNG")
    logger.info(f"Loại: {targets}")
    logger.info(f"Top: {args.top_n} mã")
    logger.info(f"Output: {DATA_DIR}")
    logger.info("=" * 60)

    step = 1
    total = len(targets)

    collectors = {
        "reports": collect_reports,
        "ownership": collect_ownership,
        "capital_history": collect_capital_history,
        "affiliate": collect_affiliate,
    }

    for t in targets:
        logger.info(f"\n[{step}/{total}] {t.upper()}")
        collector = collectors.get(t)
        if collector:
            collector(top_n=args.top_n)
        step += 1

    logger.info("\n" + "=" * 60)
    logger.info("HOÀN TẤT!")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
