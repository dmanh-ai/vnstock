"""
Tải dữ liệu OHLCV và báo cáo tài chính song song qua vnstock_pipeline.

Yêu cầu: pip install vnstock_pipeline (Insiders Program)

vnstock_pipeline chạy nhanh hơn 10x nhờ parallel processing, với retry,
rate limiting, và hỗ trợ xuất CSV/Parquet/DuckDB.

Output:
    data/pipeline/ohlcv/          - OHLCV daily cho top ~500 CP
    data/pipeline/financials/     - BCTC (balance_sheet, income, cash_flow, ratio)
    data/pipeline/intraday/       - Dữ liệu intraday (nếu bật)

Cách chạy:
    python scripts/collect_pipeline.py                       # OHLCV + financials
    python scripts/collect_pipeline.py --only ohlcv          # Chỉ OHLCV
    python scripts/collect_pipeline.py --only financials      # Chỉ BCTC
    python scripts/collect_pipeline.py --symbols VNM VCB HPG  # Chỉ 1 số mã
    python scripts/collect_pipeline.py --max-workers 2        # Giảm concurrency
"""

import sys
import logging
import argparse
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

import pandas as pd
from utils import init_rate_limiter

# ============================================================
# CẤU HÌNH
# ============================================================

DATA_DIR = PROJECT_ROOT / "data" / "pipeline"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("pipeline")


# ============================================================
# HELPERS
# ============================================================

def get_top_symbols(top_n: int = 500) -> list:
    """Lấy danh sách top mã cổ phiếu theo vốn hóa."""
    try:
        from vnstock import Vnstock
        client = Vnstock(show_log=False)
        stock = client.stock(symbol="ACB", source="KBS")

        # Lấy tất cả mã HOSE + HNX
        all_syms = []
        for group in ["HOSE", "HNX"]:
            try:
                df = stock.listing.symbols_by_group(group=group)
                if df is not None and "symbol" in df.columns:
                    all_syms.extend(df["symbol"].tolist())
            except Exception:
                pass

        return all_syms[:top_n] if all_syms else []
    except Exception as e:
        logger.warning(f"  Không lấy được danh sách mã: {e}")
        return []


# ============================================================
# OHLCV PIPELINE
# ============================================================

def run_ohlcv_pipeline(tickers: list, max_workers: int = 3, request_delay: float = 0.5):
    """Tải OHLCV song song qua vnstock_pipeline."""
    from vnstock_pipeline.tasks.ohlcv_daily import run_task

    ohlcv_dir = DATA_DIR / "ohlcv"
    ohlcv_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"  Chạy OHLCV pipeline cho {len(tickers)} mã...")
    logger.info(f"  Workers: {max_workers}, Delay: {request_delay}s")
    logger.info(f"  Output: {ohlcv_dir}")

    try:
        run_task(
            tickers=tickers,
            start="2024-01-01",
            interval="1D",
            max_workers=max_workers,
            request_delay=request_delay,
            rate_limit_wait=35.0,
        )
        logger.info(f"  OHLCV pipeline hoàn tất!")
        return True
    except Exception as e:
        logger.error(f"  OHLCV pipeline lỗi: {e}")
        return False


# ============================================================
# FINANCIAL PIPELINE
# ============================================================

def run_financial_pipeline(tickers: list, max_workers: int = 3, request_delay: float = 0.5):
    """Tải báo cáo tài chính song song qua vnstock_pipeline."""
    from vnstock_pipeline.tasks.financial import run_financial_task

    fin_dir = DATA_DIR / "financials"
    fin_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"  Chạy Financial pipeline cho {len(tickers)} mã...")

    try:
        run_financial_task(
            tickers=tickers,
            balance_sheet_period="year",
            income_statement_year_period="year",
            income_statement_quarter_period="quarter",
            cash_flow_period="year",
            ratio_period="year",
            lang="vi",
            dropna=True,
            max_workers=max_workers,
            request_delay=request_delay,
            rate_limit_wait=35.0,
        )
        logger.info(f"  Financial pipeline hoàn tất!")
        return True
    except Exception as e:
        logger.error(f"  Financial pipeline lỗi: {e}")
        return False


# ============================================================
# MAIN
# ============================================================

def collect_pipeline(
    symbols: list = None,
    only: str = None,
    max_workers: int = 3,
    request_delay: float = 0.5,
    top_n: int = 500,
):
    """Chạy pipeline tải dữ liệu."""
    try:
        import vnstock_pipeline
    except ImportError:
        logger.error(
            "vnstock_pipeline chưa cài đặt. Package thuộc Insiders Program. "
            "Xem: https://vnstocks.com/onboard-member"
        )
        return False

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Lấy danh sách mã
    tickers = symbols
    if not tickers:
        logger.info(f"  Lấy top {top_n} mã cổ phiếu...")
        tickers = get_top_symbols(top_n=top_n)
        if not tickers:
            logger.error("  Không lấy được danh sách mã!")
            return False

    logger.info(f"  Tổng: {len(tickers)} mã")

    # Tự điều chỉnh concurrency theo số lượng mã
    if len(tickers) > 200:
        max_workers = min(max_workers, 2)
        request_delay = max(request_delay, 1.0)
    if len(tickers) > 500:
        max_workers = 1
        request_delay = max(request_delay, 2.0)

    success = True

    # OHLCV
    if only is None or only == "ohlcv":
        logger.info(f"\n--- OHLCV Pipeline ---")
        if not run_ohlcv_pipeline(tickers, max_workers, request_delay):
            success = False

    # Financials
    if only is None or only == "financials":
        logger.info(f"\n--- Financial Pipeline ---")
        if not run_financial_pipeline(tickers, max_workers, request_delay):
            success = False

    return success


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Tải dữ liệu song song qua vnstock_pipeline.",
    )
    parser.add_argument("--symbols", nargs="+", default=None,
                        help="Danh sách mã cổ phiếu (mặc định: top 500)")
    parser.add_argument("--only", choices=["ohlcv", "financials"], default=None,
                        help="Chỉ chạy 1 loại pipeline")
    parser.add_argument("--max-workers", type=int, default=3,
                        help="Số worker song song (mặc định 3)")
    parser.add_argument("--request-delay", type=float, default=0.5,
                        help="Delay giữa các request (giây)")
    parser.add_argument("--top-n", type=int, default=500,
                        help="Số mã top theo vốn hóa (mặc định 500)")
    args = parser.parse_args()

    init_rate_limiter()

    logger.info("=" * 60)
    logger.info("PIPELINE TẢI DỮ LIỆU SONG SONG (vnstock_pipeline)")
    logger.info(f"Loại: {args.only or 'OHLCV + Financials'}")
    logger.info(f"Mã: {args.symbols or f'Top {args.top_n}'}")
    logger.info(f"Workers: {args.max_workers}, Delay: {args.request_delay}s")
    logger.info(f"Output: {DATA_DIR}")
    logger.info("=" * 60)

    collect_pipeline(
        symbols=args.symbols,
        only=args.only,
        max_workers=args.max_workers,
        request_delay=args.request_delay,
        top_n=args.top_n,
    )

    logger.info("\n" + "=" * 60)
    logger.info("HOÀN TẤT!")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
