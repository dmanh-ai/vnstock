"""
Thu thập dữ liệu giao dịch chi tiết: foreign trade, prop trade, trading stats.

Nguồn:
  - vnstock_data Trading(source='cafef'): foreign_trade, prop_trade, order_stats
  - vnstock Trading(VCI): trading_stats (thống kê giao dịch theo mã)
  - Fallback: KBS Trading price_board (thông tin cơ bản)

Output:
    data/trading/
    ├── foreign_trade.csv     - Giao dịch NDTNN theo mã (mua/bán ròng)
    ├── prop_trade.csv        - Giao dịch tự doanh theo mã
    ├── order_stats.csv       - Thống kê lệnh mua/bán (CafeF)
    ├── trading_stats.csv     - Thống kê giao dịch (VCI)
    └── matched_prices.csv    - Thống kê khớp lệnh theo bước giá (KBS)

Cách chạy:
    python scripts/collect_trading_detail.py                 # Tất cả (top 50)
    python scripts/collect_trading_detail.py --top-n 100     # Top 100 mã
    python scripts/collect_trading_detail.py --only foreign_trade prop_trade
"""

import sys
import logging
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
from utils import init_rate_limiter, get_limiter, is_file_fresh

# ============================================================
# CẤU HÌNH
# ============================================================

DATA_DIR = PROJECT_ROOT / "data" / "trading"
MAX_WORKERS = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("trading_detail")

_api_lock = threading.Lock()

COLLECT_TYPES = [
    "foreign_trade", "prop_trade", "order_stats",
    "trading_stats", "matched_prices",
]


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


def _collect_concurrent(label, fetch_func, symbols, csv_path, add_symbol_col=True):
    """Generic concurrent fetcher."""
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
# 1. FOREIGN TRADE (vnstock_data CafeF → fallback VCI)
# ============================================================

def collect_foreign_trade(top_n: int = 50):
    """Giao dịch NDTNN theo mã: mua/bán ròng hàng ngày."""
    symbols = _get_symbols("VCI", top_n)
    end = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")

    # Try vnstock_data CafeF first
    def fetch_cafef(symbol):
        try:
            from vnstock_data import Trading
            t = Trading(symbol=symbol, source="cafef")
            return _rate_limited_call(lambda: t.foreign_trade(start=start, end=end))
        except (ImportError, AttributeError, Exception):
            return pd.DataFrame()

    # Fallback: VCI trading (nếu CafeF không có)
    def fetch_vci(symbol):
        try:
            from vnstock.common.client import Vnstock
            client = Vnstock(source="VCI", show_log=False)
            stock = client.stock(symbol=symbol, source="VCI")
            return _rate_limited_call(lambda: stock.company.trading_stats())
        except Exception:
            return pd.DataFrame()

    # Test CafeF first
    use_cafef = False
    try:
        from vnstock_data import Trading
        test = Trading(symbol="VCB", source="cafef")
        test_df = test.foreign_trade(start=start, end=end)
        if test_df is not None and not test_df.empty:
            use_cafef = True
            logger.info("  Sử dụng vnstock_data CafeF cho foreign_trade")
    except Exception:
        logger.info("  CafeF không khả dụng, dùng VCI trading_stats")

    fetch = fetch_cafef if use_cafef else fetch_vci
    label = "Foreign Trade (CafeF)" if use_cafef else "Trading Stats (VCI)"

    return _collect_concurrent(
        label, fetch, symbols, DATA_DIR / "foreign_trade.csv"
    )


# ============================================================
# 2. PROP TRADE (vnstock_data CafeF)
# ============================================================

def collect_prop_trade(top_n: int = 50):
    """Giao dịch tự doanh CTCK theo mã."""
    symbols = _get_symbols("VCI", top_n)
    end = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")

    def fetch(symbol):
        try:
            from vnstock_data import Trading
            t = Trading(symbol=symbol, source="cafef")
            return _rate_limited_call(lambda: t.prop_trade(start=start, end=end))
        except (ImportError, AttributeError):
            return pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    # Check availability
    try:
        from vnstock_data import Trading
        logger.info("  vnstock_data Trading(cafef).prop_trade available")
    except ImportError:
        logger.warning("  vnstock_data không có, bỏ qua prop_trade")
        return False

    return _collect_concurrent(
        "Prop Trade (CafeF)", fetch, symbols, DATA_DIR / "prop_trade.csv"
    )


# ============================================================
# 3. ORDER STATS (vnstock_data CafeF)
# ============================================================

def collect_order_stats(top_n: int = 50):
    """Thống kê lệnh mua/bán (CafeF)."""
    symbols = _get_symbols("VCI", top_n)

    def fetch(symbol):
        try:
            from vnstock_data import Trading
            t = Trading(symbol=symbol, source="cafef")
            return _rate_limited_call(lambda: t.order_stats())
        except (ImportError, AttributeError):
            return pd.DataFrame()
        except Exception:
            return pd.DataFrame()

    try:
        from vnstock_data import Trading
        logger.info("  vnstock_data Trading(cafef).order_stats available")
    except ImportError:
        logger.warning("  vnstock_data không có, bỏ qua order_stats")
        return False

    return _collect_concurrent(
        "Order Stats (CafeF)", fetch, symbols, DATA_DIR / "order_stats.csv"
    )


# ============================================================
# 4. TRADING STATS (VCI Company)
# ============================================================

def collect_trading_stats(top_n: int = 50):
    """Thống kê giao dịch theo mã (VCI Company.trading_stats)."""
    symbols = _get_symbols("VCI", top_n)

    def fetch(symbol):
        from vnstock.common.client import Vnstock
        client = Vnstock(source="VCI", show_log=False)
        stock = client.stock(symbol=symbol, source="VCI")
        return _rate_limited_call(lambda: stock.company.trading_stats())

    return _collect_concurrent(
        "Trading Stats (VCI)", fetch, symbols, DATA_DIR / "trading_stats.csv"
    )


# ============================================================
# 5. MATCHED PRICES (KBS)
# ============================================================

def collect_matched_prices(top_n: int = 50):
    """Thống kê khớp lệnh theo bước giá (KBS matched_by_price)."""
    symbols = _get_symbols("KBS", top_n)

    def fetch(symbol):
        from vnstock.common.client import Vnstock
        client = Vnstock(source="KBS", show_log=False)
        stock = client.stock(symbol=symbol, source="KBS")
        return _rate_limited_call(lambda: stock.trading.matched_by_price())

    return _collect_concurrent(
        "Matched Prices (KBS)", fetch, symbols, DATA_DIR / "matched_prices.csv"
    )


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Thu thập dữ liệu giao dịch chi tiết.",
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
    logger.info("THU THẬP DỮ LIỆU GIAO DỊCH CHI TIẾT")
    logger.info(f"Loại: {targets}")
    logger.info(f"Top: {args.top_n} mã")
    logger.info(f"Output: {DATA_DIR}")
    logger.info("=" * 60)

    step = 1
    total = len(targets)

    collectors = {
        "foreign_trade": collect_foreign_trade,
        "prop_trade": collect_prop_trade,
        "order_stats": collect_order_stats,
        "trading_stats": collect_trading_stats,
        "matched_prices": collect_matched_prices,
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
