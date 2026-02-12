"""
Thu thập dữ liệu tài chính bổ sung: VCI Finance (note, ratio chi tiết) + MAS Finance.

Nguồn:
  - VCI Finance: note() - thuyết minh BCTC
  - VCI Finance: ratio(flatten_columns=True) - chỉ số tài chính chi tiết
  - vnstock_data Finance(MAS): annual_plan - kế hoạch kinh doanh năm
  - vnstock_data Finance(MAS): income_statement, balance_sheet, cash_flow, ratio

Output:
    data/financials_extra/
    ├── notes/          - Thuyết minh BCTC (VCI, top 50 mã)
    │   ├── VCB.csv
    │   └── ...
    ├── ratios_detail/  - Chỉ số tài chính chi tiết flatten (VCI, top 50 mã)
    │   ├── VCB.csv
    │   └── ...
    └── mas/            - BCTC từ Mirae Asset (MAS, top 50 mã)
        ├── VCB_annual_plan.csv
        ├── VCB_income.csv
        └── ...

Cách chạy:
    python scripts/collect_finance_extra.py                    # Tất cả
    python scripts/collect_finance_extra.py --top-n 100        # Top 100 mã
    python scripts/collect_finance_extra.py --only notes ratios_detail
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

DATA_DIR = PROJECT_ROOT / "data" / "financials_extra"
MAX_WORKERS = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("finance_extra")

_api_lock = threading.Lock()

COLLECT_TYPES = ["notes", "ratios_detail", "mas_annual_plan", "mas_statements"]


def _rate_limited_call(func):
    with _api_lock:
        get_limiter().wait()
    return func()


def _get_symbols(source: str, top_n: int) -> list:
    from vnstock.common.client import Vnstock
    client = Vnstock(source=source, show_log=False)
    stock = client.stock(symbol="ACB", source=source)
    symbols_df = stock.listing.symbols_by_exchange(show_log=False)
    return symbols_df["symbol"].tolist()[:top_n]


# ============================================================
# 1. VCI FINANCE NOTES (thuyết minh BCTC)
# ============================================================

def collect_notes(top_n: int = 50):
    """Thu thập thuyết minh BCTC từ VCI Finance.note()."""
    notes_dir = DATA_DIR / "notes"
    notes_dir.mkdir(parents=True, exist_ok=True)

    symbols = _get_symbols("VCI", top_n)
    success = 0
    errors = 0
    completed = 0

    logger.info(f"  Đang lấy Finance notes cho {len(symbols)} mã...")

    def fetch(symbol):
        csv_path = notes_dir / f"{symbol}.csv"
        if is_file_fresh(csv_path):
            return "skipped"

        from vnstock.common.client import Vnstock
        client = Vnstock(source="VCI", show_log=False)
        stock = client.stock(symbol=symbol, source="VCI")

        for period in ["year", "quarter"]:
            try:
                df = _rate_limited_call(
                    lambda p=period: stock.finance.note(period=p, lang="vi")
                )
                if df is not None and not df.empty:
                    df["period"] = period
                    df["symbol"] = symbol
                    return df
            except Exception:
                continue
        return pd.DataFrame()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch, sym): sym for sym in symbols}

        for future in as_completed(futures):
            completed += 1
            symbol = futures[future]
            if completed % 20 == 0 or completed == len(symbols):
                logger.info(f"    [{completed}/{len(symbols)}] (OK: {success}, lỗi: {errors})")
            try:
                result = future.result()
                if isinstance(result, str) and result == "skipped":
                    success += 1
                elif isinstance(result, pd.DataFrame) and not result.empty:
                    csv_path = notes_dir / f"{symbol}.csv"
                    result.to_csv(csv_path, index=False, encoding="utf-8-sig")
                    success += 1
                else:
                    errors += 1
            except Exception:
                errors += 1

    logger.info(f"    Notes: {success}/{len(symbols)} mã, {errors} lỗi")
    return success > 0


# ============================================================
# 2. VCI FINANCE RATIOS DETAIL (flatten columns)
# ============================================================

def collect_ratios_detail(top_n: int = 50):
    """Thu thập chỉ số tài chính chi tiết (flatten columns) từ VCI."""
    ratios_dir = DATA_DIR / "ratios_detail"
    ratios_dir.mkdir(parents=True, exist_ok=True)

    symbols = _get_symbols("VCI", top_n)
    success = 0
    errors = 0
    completed = 0

    logger.info(f"  Đang lấy Finance ratios (flatten) cho {len(symbols)} mã...")

    def fetch(symbol):
        csv_path = ratios_dir / f"{symbol}.csv"
        if is_file_fresh(csv_path):
            return "skipped", symbol

        from vnstock.common.client import Vnstock
        client = Vnstock(source="VCI", show_log=False)
        stock = client.stock(symbol=symbol, source="VCI")

        all_dfs = []
        for period in ["year", "quarter"]:
            try:
                df = _rate_limited_call(
                    lambda p=period: stock.finance.ratio(
                        period=p, lang="en",
                        flatten_columns=True, separator="_"
                    )
                )
                if df is not None and not df.empty:
                    df["period"] = period
                    all_dfs.append(df)
            except Exception:
                continue

        if all_dfs:
            combined = pd.concat(all_dfs, ignore_index=True)
            combined["symbol"] = symbol
            return combined, symbol
        return pd.DataFrame(), symbol

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch, sym): sym for sym in symbols}

        for future in as_completed(futures):
            completed += 1
            if completed % 20 == 0 or completed == len(symbols):
                logger.info(f"    [{completed}/{len(symbols)}] (OK: {success}, lỗi: {errors})")
            try:
                result, symbol = future.result()
                if isinstance(result, str) and result == "skipped":
                    success += 1
                elif isinstance(result, pd.DataFrame) and not result.empty:
                    csv_path = ratios_dir / f"{symbol}.csv"
                    result.to_csv(csv_path, index=False, encoding="utf-8-sig")
                    success += 1
                else:
                    errors += 1
            except Exception:
                errors += 1

    logger.info(f"    Ratios detail: {success}/{len(symbols)} mã, {errors} lỗi")
    return success > 0


# ============================================================
# 3. MAS ANNUAL PLAN (vnstock_data)
# ============================================================

def collect_mas_annual_plan(top_n: int = 50):
    """Thu thập kế hoạch kinh doanh năm từ MAS (vnstock_data)."""
    mas_dir = DATA_DIR / "mas"
    mas_dir.mkdir(parents=True, exist_ok=True)

    check_path = mas_dir / "_annual_plan_done.csv"
    if is_file_fresh(check_path):
        logger.info("  MAS annual_plan đã có hôm nay, bỏ qua.")
        return True

    try:
        from vnstock_data import Finance as MASFinance
    except ImportError:
        logger.warning("  vnstock_data.Finance không khả dụng, bỏ qua MAS")
        return False

    symbols = _get_symbols("VCI", top_n)
    success = 0
    errors = 0
    completed = 0

    logger.info(f"  Đang lấy MAS annual_plan cho {len(symbols)} mã...")

    def fetch(symbol):
        try:
            fin = MASFinance(symbol=symbol, source="MAS")
            return _rate_limited_call(lambda: fin.annual_plan())
        except Exception:
            return pd.DataFrame()

    all_data = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch, sym): sym for sym in symbols}

        for future in as_completed(futures):
            completed += 1
            symbol = futures[future]
            if completed % 20 == 0 or completed == len(symbols):
                logger.info(f"    [{completed}/{len(symbols)}] (OK: {success}, lỗi: {errors})")
            try:
                df = future.result()
                if df is not None and not df.empty:
                    df["symbol"] = symbol
                    all_data.append(df)
                    success += 1
                else:
                    errors += 1
            except Exception:
                errors += 1

    if all_data:
        combined = pd.concat(all_data, ignore_index=True)
        combined.to_csv(mas_dir / "annual_plan.csv", index=False, encoding="utf-8-sig")
        # Mark as done
        pd.DataFrame({"done": [True]}).to_csv(check_path, index=False)
        logger.info(f"    MAS annual_plan: {len(combined)} rows")

    logger.info(f"    Kết quả: {success}/{len(symbols)} mã, {errors} lỗi")
    return success > 0


# ============================================================
# 4. MAS STATEMENTS (vnstock_data)
# ============================================================

def collect_mas_statements(top_n: int = 50):
    """Thu thập BCTC từ Mirae Asset (MAS) qua vnstock_data."""
    mas_dir = DATA_DIR / "mas"
    mas_dir.mkdir(parents=True, exist_ok=True)

    check_path = mas_dir / "_statements_done.csv"
    if is_file_fresh(check_path):
        logger.info("  MAS statements đã có hôm nay, bỏ qua.")
        return True

    try:
        from vnstock_data import Finance as MASFinance
    except ImportError:
        logger.warning("  vnstock_data.Finance không khả dụng, bỏ qua MAS statements")
        return False

    symbols = _get_symbols("VCI", top_n)
    statements = ["income_statement", "balance_sheet", "cash_flow", "ratio"]

    for stmt in statements:
        logger.info(f"  MAS {stmt}...")
        all_data = []
        success = 0
        errors = 0

        def fetch(symbol, s=stmt):
            try:
                fin = MASFinance(symbol=symbol, source="MAS")
                method = getattr(fin, s, None)
                if method is None:
                    return pd.DataFrame()
                return _rate_limited_call(lambda: method(period="year"))
            except Exception:
                return pd.DataFrame()

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(fetch, sym): sym for sym in symbols}

            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    df = future.result()
                    if df is not None and not df.empty:
                        df["symbol"] = symbol
                        all_data.append(df)
                        success += 1
                    else:
                        errors += 1
                except Exception:
                    errors += 1

        if all_data:
            combined = pd.concat(all_data, ignore_index=True)
            combined.to_csv(mas_dir / f"{stmt}.csv", index=False, encoding="utf-8-sig")
            logger.info(f"    {stmt}: {len(combined)} rows ({success} mã)")

    pd.DataFrame({"done": [True]}).to_csv(check_path, index=False)
    return True


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Thu thập dữ liệu tài chính bổ sung (VCI notes/ratios + MAS).",
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
    logger.info("THU THẬP DỮ LIỆU TÀI CHÍNH BỔ SUNG")
    logger.info(f"Loại: {targets}")
    logger.info(f"Top: {args.top_n} mã")
    logger.info(f"Output: {DATA_DIR}")
    logger.info("=" * 60)

    step = 1
    total = len(targets)

    collectors = {
        "notes": collect_notes,
        "ratios_detail": collect_ratios_detail,
        "mas_annual_plan": collect_mas_annual_plan,
        "mas_statements": collect_mas_statements,
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
