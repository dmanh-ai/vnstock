"""
Tính toán chỉ báo kỹ thuật (Technical Analysis) từ vnstock_ta.

Yêu cầu: pip install vnstock_ta (Insiders Program)

Tính 7 chỉ báo chính cho top chỉ số và cổ phiếu:
    SMA(20,50), EMA(12,26), RSI(14), MACD(12,26,9), Bollinger Bands(20,2),
    Stochastic(14,3,3), ADX(14)

Output:
    data/ta/indices/VNINDEX.csv     - TA cho VNINDEX
    data/ta/indices/VN30.csv        - TA cho VN30
    data/ta/indices/HNX.csv         - TA cho HNX
    data/ta/stocks/VNM.csv          - TA cho từng mã top VN30
    data/ta/stocks/VCB.csv
    data/ta/signals.csv             - Tổng hợp tín hiệu (RSI overbought/oversold)

Cách chạy:
    python scripts/collect_ta.py                     # Mặc định: 3 chỉ số + VN30
    python scripts/collect_ta.py --symbols VNM VCB HPG  # Chỉ tính cho 1 số mã
    python scripts/collect_ta.py --skip-stocks        # Chỉ tính cho chỉ số
"""

import sys
import logging
import argparse
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

import pandas as pd
from utils import init_rate_limiter, is_file_fresh

# ============================================================
# CẤU HÌNH
# ============================================================

DATA_DIR = PROJECT_ROOT / "data" / "ta"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("ta")

# Chỉ số mặc định
DEFAULT_INDICES = ["VNINDEX", "VN30", "HNX"]


# ============================================================
# TA CALCULATOR
# ============================================================

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Tính 7 chỉ báo kỹ thuật chính cho OHLCV DataFrame.
    Cần vnstock_ta.Indicator.
    """
    from vnstock_ta import Indicator

    # Indicator cần DatetimeIndex named 'time'
    if "time" in df.columns:
        df = df.set_index("time")
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)
    df.index.name = "time"

    indicator = Indicator(data=df)

    # 1. SMA
    df["SMA_20"] = indicator.sma(length=20)
    df["SMA_50"] = indicator.sma(length=50)

    # 2. EMA
    df["EMA_12"] = indicator.ema(length=12)
    df["EMA_26"] = indicator.ema(length=26)

    # 3. RSI
    df["RSI_14"] = indicator.rsi(length=14)

    # 4. MACD
    macd = indicator.macd(fast=12, slow=26, signal=9)
    if isinstance(macd, pd.DataFrame):
        for col in macd.columns:
            df[col] = macd[col]

    # 5. Bollinger Bands
    bbands = indicator.bbands(length=20, std=2)
    if isinstance(bbands, pd.DataFrame):
        for col in bbands.columns:
            df[col] = bbands[col]

    # 6. Stochastic
    stoch = indicator.stoch(k=14, d=3, smooth_k=3)
    if isinstance(stoch, pd.DataFrame):
        for col in stoch.columns:
            df[col] = stoch[col]

    # 7. ADX
    adx = indicator.adx(length=14)
    if isinstance(adx, pd.DataFrame):
        for col in adx.columns:
            df[col] = adx[col]

    return df.reset_index()


def generate_signal(row) -> str:
    """Tạo tín hiệu từ RSI + MACD."""
    signals = []
    rsi = row.get("RSI_14")
    if rsi is not None and not pd.isna(rsi):
        if rsi > 70:
            signals.append("RSI_OVERBOUGHT")
        elif rsi < 30:
            signals.append("RSI_OVERSOLD")

    macd_h = row.get("MACDh")
    if macd_h is not None and not pd.isna(macd_h):
        if macd_h > 0:
            signals.append("MACD_BULLISH")
        else:
            signals.append("MACD_BEARISH")

    return ",".join(signals) if signals else "NEUTRAL"


# ============================================================
# COLLECTORS
# ============================================================

def get_ohlcv(symbol: str, days: int = 200) -> pd.DataFrame:
    """Lấy OHLCV từ vnstock hoặc vnstock_data."""
    end = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    # Thử vnstock_data trước
    try:
        from vnstock_data import Quote
        quote = Quote(source="VCI", symbol=symbol)
        df = quote.history(start=start, end=end, interval="1D")
        if df is not None and not df.empty:
            return df
    except (ImportError, Exception):
        pass

    # Fallback vnstock free
    from vnstock import Vnstock
    client = Vnstock(show_log=False)
    stock = client.stock(symbol=symbol, source="VCI")
    return stock.quote.history(start=start, end=end, interval="1D")


def collect_ta_for_symbol(symbol: str, output_dir: Path) -> dict:
    """Tính TA cho 1 symbol, trả về signal row."""
    csv_path = output_dir / f"{symbol}.csv"
    if is_file_fresh(csv_path):
        logger.info(f"  {symbol}: đã có hôm nay, bỏ qua.")
        try:
            df = pd.read_csv(csv_path)
            if not df.empty:
                return df.iloc[-1].to_dict()
        except Exception:
            pass
        return {}

    try:
        df = get_ohlcv(symbol)
        if df is None or df.empty:
            logger.warning(f"  {symbol}: OHLCV rỗng")
            return {}

        logger.info(f"  {symbol}: {len(df)} bars → tính TA...")
        df = compute_indicators(df)

        output_dir.mkdir(parents=True, exist_ok=True)
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        logger.info(f"    → {csv_path.name}")

        # Trả về dòng cuối (tín hiệu mới nhất)
        return df.iloc[-1].to_dict() if not df.empty else {}

    except ImportError:
        logger.error("  vnstock_ta chưa cài đặt!")
        return {}
    except Exception as e:
        logger.warning(f"  {symbol}: lỗi - {e}")
        return {}


def collect_ta(symbols: list = None, skip_stocks: bool = False):
    """Thu thập TA cho chỉ số và cổ phiếu."""
    try:
        from vnstock_ta import Indicator
    except ImportError:
        logger.error(
            "vnstock_ta chưa cài đặt. Package thuộc Insiders Program. "
            "Xem: https://vnstocks.com/onboard-member"
        )
        return False

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    all_signals = []

    # --- Indices ---
    indices_dir = DATA_DIR / "indices"
    logger.info(f"\n--- Tính TA cho {len(DEFAULT_INDICES)} chỉ số ---")
    for idx in DEFAULT_INDICES:
        signal = collect_ta_for_symbol(idx, indices_dir)
        if signal:
            signal["symbol"] = idx
            signal["type"] = "index"
            signal["signal"] = generate_signal(signal)
            all_signals.append(signal)

    # --- Stocks ---
    if not skip_stocks:
        stocks_dir = DATA_DIR / "stocks"
        stock_symbols = symbols

        if not stock_symbols:
            # Lấy VN30
            try:
                from vnstock import Vnstock
                client = Vnstock(show_log=False)
                stock = client.stock(symbol="ACB", source="KBS")
                vn30 = stock.listing.symbols_by_group(group="VN30")
                stock_symbols = vn30["symbol"].tolist() if "symbol" in vn30.columns else []
            except Exception:
                stock_symbols = [
                    "VNM", "VCB", "VHM", "HPG", "FPT", "VIC", "MSN", "MWG",
                    "TCB", "CTG", "BID", "MBB", "ACB", "VPB", "SSI", "GAS",
                ]

        logger.info(f"\n--- Tính TA cho {len(stock_symbols)} cổ phiếu ---")
        for sym in stock_symbols:
            signal = collect_ta_for_symbol(sym, stocks_dir)
            if signal:
                signal["symbol"] = sym
                signal["type"] = "stock"
                signal["signal"] = generate_signal(signal)
                all_signals.append(signal)

    # --- Tổng hợp tín hiệu ---
    if all_signals:
        signals_df = pd.DataFrame(all_signals)
        # Chỉ giữ cột quan trọng
        keep_cols = ["symbol", "type", "time", "close",
                     "RSI_14", "MACDh", "SMA_20", "SMA_50",
                     "BBL", "BBU", "STOCHk", "ADX_14", "signal"]
        keep_cols = [c for c in keep_cols if c in signals_df.columns]
        signals_df = signals_df[keep_cols]

        signals_path = DATA_DIR / "signals.csv"
        signals_df.to_csv(signals_path, index=False, encoding="utf-8-sig")
        logger.info(f"\n  Signals: {len(signals_df)} mã → signals.csv")

        # Log tín hiệu đáng chú ý
        for _, row in signals_df.iterrows():
            sig = row.get("signal", "")
            if "OVERBOUGHT" in sig or "OVERSOLD" in sig:
                logger.info(f"    ⚠ {row['symbol']}: {sig} (RSI={row.get('RSI_14', 'N/A'):.1f})")

    return True


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Tính chỉ báo kỹ thuật (Technical Analysis) từ vnstock_ta.",
    )
    parser.add_argument("--symbols", nargs="+", default=None,
                        help="Danh sách mã cổ phiếu (mặc định: VN30)")
    parser.add_argument("--skip-stocks", action="store_true",
                        help="Chỉ tính cho chỉ số, bỏ qua cổ phiếu")
    args = parser.parse_args()

    init_rate_limiter()

    logger.info("=" * 60)
    logger.info("TÍNH CHỈ BÁO KỸ THUẬT (vnstock_ta)")
    logger.info(f"Chỉ số: {DEFAULT_INDICES}")
    logger.info(f"Cổ phiếu: {args.symbols or 'VN30'}")
    logger.info(f"Output: {DATA_DIR}")
    logger.info("=" * 60)

    collect_ta(symbols=args.symbols, skip_stocks=args.skip_stocks)

    logger.info("\n" + "=" * 60)
    logger.info("HOÀN TẤT!")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
