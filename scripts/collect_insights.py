"""
Thu thập thống kê thị trường / Market Insights từ thư viện vnstock.

Nguồn dữ liệu:
  - KBS quote.history(get_all=True) cho VNINDEX → volume, value, deals, foreign flow
  - KBS trading.price_board(get_all=True) → top gainers/losers hôm nay
  - VCI company.ratio_summary() cho top CP → tính PE/PB bình quân thị trường

Output:
    data/insights/market_pe.csv          - P/E bình quân thị trường (weighted by market cap)
    data/insights/market_pb.csv          - P/B bình quân thị trường (weighted by market cap)
    data/insights/market_value.csv       - Giá trị giao dịch VNINDEX lịch sử
    data/insights/market_volume.csv      - Khối lượng giao dịch VNINDEX lịch sử
    data/insights/market_deal.csv        - Số lượng deal VNINDEX lịch sử
    data/insights/market_foreign_buy.csv - KL NDTNN mua lịch sử
    data/insights/market_foreign_sell.csv- KL NDTNN bán lịch sử
    data/insights/market_gainer.csv      - Top 30 tăng giá hôm nay
    data/insights/market_loser.csv       - Top 30 giảm giá hôm nay
    data/insights/market_evaluation.csv  - Tổng hợp đánh giá thị trường hôm nay

Cách chạy:
    python scripts/collect_insights.py
    python scripts/collect_insights.py --only pe pb value volume
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
import numpy as np
from utils import init_rate_limiter, is_file_fresh

# ============================================================
# CẤU HÌNH
# ============================================================

DATA_DIR = PROJECT_ROOT / "data" / "insights"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("insights")

# Các loại insights cần thu thập
INSIGHT_TYPES = [
    "pe", "pb", "value", "volume", "deal",
    "foreign_buy", "foreign_sell",
    "gainer", "loser", "evaluation",
]


# ============================================================
# HELPERS
# ============================================================

def _save_incremental(df, csv_path, date_col="time"):
    """Merge DataFrame mới với dữ liệu cũ, loại trùng theo date_col."""
    if csv_path.exists():
        try:
            existing = pd.read_csv(csv_path)
            if date_col and date_col in df.columns and date_col in existing.columns:
                df = pd.concat([existing, df], ignore_index=True)
                df = df.drop_duplicates(subset=[date_col], keep="last")
                df = df.sort_values(date_col).reset_index(drop=True)
        except Exception:
            pass
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")


def _get_vnstock_client():
    """Tạo Vnstock client."""
    from vnstock import Vnstock
    return Vnstock(show_log=False)


# ============================================================
# 1. VNINDEX HISTORICAL STATS (volume, value, deals, foreign)
# ============================================================

def collect_index_history():
    """
    Lấy lịch sử VNINDEX với get_all=True để có:
    volume, total_value, total_trades, foreign_buy_volume, foreign_sell_volume, foreign_net_volume.
    Trả về dict {type_name: DataFrame}.
    """
    client = _get_vnstock_client()
    stock = client.stock(symbol="VNINDEX", source="KBS")

    end = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")

    logger.info(f"  Lấy VNINDEX history (KBS, get_all=True) từ {start} đến {end}...")
    df = stock.quote.history(start=start, end=end, interval="1D", get_all=True)

    if df is None or df.empty:
        logger.warning("  VNINDEX history trả về rỗng.")
        return {}

    logger.info(f"  VNINDEX history: {len(df)} rows, columns: {df.columns.tolist()}")

    results = {}

    # Volume
    if "volume" in df.columns:
        results["volume"] = df[["time", "volume"]].copy()

    # Total value
    if "total_value" in df.columns:
        results["value"] = df[["time", "total_value"]].copy()

    # Total trades (deals)
    if "total_trades" in df.columns:
        results["deal"] = df[["time", "total_trades"]].copy()

    # Foreign buy
    if "foreign_buy_volume" in df.columns:
        results["foreign_buy"] = df[["time", "foreign_buy_volume"]].copy()

    # Foreign sell
    if "foreign_sell_volume" in df.columns:
        cols = ["time", "foreign_sell_volume"]
        if "foreign_net_volume" in df.columns:
            cols.append("foreign_net_volume")
        results["foreign_sell"] = df[cols].copy()

    return results


# ============================================================
# 2. MARKET PE / PB (weighted average from top stocks)
# ============================================================

def collect_market_pe_pb():
    """
    Tính P/E và P/B bình quân thị trường từ VCI ratio_summary của top CP.
    Trả về dict {"pe": DataFrame, "pb": DataFrame}.
    """
    client = _get_vnstock_client()

    # Lấy danh sách VN30 (đại diện tốt cho thị trường)
    stock = client.stock(symbol="ACB", source="KBS")
    try:
        vn30 = stock.listing.symbols_by_group(group="VN30")
        symbols = vn30["symbol"].tolist() if "symbol" in vn30.columns else []
    except Exception as e:
        logger.warning(f"  Không lấy được VN30: {e}. Dùng danh sách mặc định.")
        symbols = [
            "VNM", "VCB", "VHM", "HPG", "FPT", "VIC", "MSN", "MWG",
            "TCB", "CTG", "BID", "MBB", "ACB", "VPB", "SSI", "GAS",
            "PLX", "SAB", "VRE", "POW", "TPB", "HDB", "STB", "PDR",
            "BCM", "GVR", "NVL", "VJC", "BVH", "KDH",
        ]

    logger.info(f"  Lấy ratio_summary cho {len(symbols)} mã (VN30)...")

    all_ratios = []
    for symbol in symbols:
        try:
            s = client.stock(symbol=symbol, source="VCI")
            rs = s.company.ratio_summary()
            if rs is not None and not rs.empty:
                # Lấy dòng mới nhất
                latest = rs.iloc[-1:].copy()
                latest["symbol"] = symbol
                all_ratios.append(latest)
        except Exception as e:
            logger.debug(f"    {symbol}: lỗi ratio_summary - {e}")
            continue

    if not all_ratios:
        logger.warning("  Không lấy được ratio_summary cho bất kỳ mã nào.")
        return {}

    ratios_df = pd.concat(all_ratios, ignore_index=True)
    today = datetime.now().strftime("%Y-%m-%d")

    results = {}

    # Market PE
    if "pe" in ratios_df.columns:
        pe_data = ratios_df[["symbol", "pe"]].dropna(subset=["pe"])
        pe_data = pe_data[pe_data["pe"] > 0]
        if not pe_data.empty:
            avg_pe = pe_data["pe"].median()  # Median tránh outlier
            pe_row = pd.DataFrame([{
                "time": today,
                "market_pe_median": round(avg_pe, 2),
                "market_pe_mean": round(pe_data["pe"].mean(), 2),
                "num_stocks": len(pe_data),
                "pe_min": round(pe_data["pe"].min(), 2),
                "pe_max": round(pe_data["pe"].max(), 2),
            }])
            results["pe"] = pe_row
            logger.info(f"  Market PE median: {avg_pe:.2f} (từ {len(pe_data)} mã)")

    # Market PB
    if "pb" in ratios_df.columns:
        pb_data = ratios_df[["symbol", "pb"]].dropna(subset=["pb"])
        pb_data = pb_data[pb_data["pb"] > 0]
        if not pb_data.empty:
            avg_pb = pb_data["pb"].median()
            pb_row = pd.DataFrame([{
                "time": today,
                "market_pb_median": round(avg_pb, 2),
                "market_pb_mean": round(pb_data["pb"].mean(), 2),
                "num_stocks": len(pb_data),
                "pb_min": round(pb_data["pb"].min(), 2),
                "pb_max": round(pb_data["pb"].max(), 2),
            }])
            results["pb"] = pb_row
            logger.info(f"  Market PB median: {avg_pb:.2f} (từ {len(pb_data)} mã)")

    return results


# ============================================================
# 3. TOP GAINERS / LOSERS (from KBS price board)
# ============================================================

def collect_top_movers():
    """
    Lấy top tăng/giảm hôm nay từ KBS price_board toàn sàn.
    Trả về dict {"gainer": DataFrame, "loser": DataFrame}.
    """
    client = _get_vnstock_client()
    stock = client.stock(symbol="ACB", source="KBS")

    # Lấy danh sách tất cả mã HOSE
    try:
        all_syms = stock.listing.symbols_by_group(group="HOSE")
        symbols = all_syms["symbol"].tolist() if "symbol" in all_syms.columns else []
    except Exception as e:
        logger.warning(f"  Không lấy được listing HOSE: {e}")
        return {}

    if not symbols:
        return {}

    # Lấy price board theo batch
    logger.info(f"  Lấy KBS price_board cho {len(symbols)} mã HOSE...")
    all_dfs = []
    batch_size = 100
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        try:
            df = stock.trading.price_board(symbols_list=batch, get_all=True)
            if df is not None and not df.empty:
                all_dfs.append(df)
        except Exception as e:
            logger.warning(f"  Batch {i}-{i+len(batch)}: lỗi - {e}")

    if not all_dfs:
        return {}

    board = pd.concat(all_dfs, ignore_index=True)
    logger.info(f"  Price board: {len(board)} mã, columns: {board.columns.tolist()}")

    results = {}
    today = datetime.now().strftime("%Y-%m-%d")

    # Xác định cột percent_change
    pct_col = None
    for col in ["percent_change", "price_change_pct", "CHP"]:
        if col in board.columns:
            pct_col = col
            break

    if pct_col is None:
        logger.warning("  Không tìm thấy cột percent_change trong price_board.")
        return {}

    board[pct_col] = pd.to_numeric(board[pct_col], errors="coerce")
    board = board.dropna(subset=[pct_col])

    # Chọn các cột hữu ích
    keep_cols = ["symbol"]
    for col in ["close_price", "open_price", "high_price", "low_price",
                 pct_col, "total_trades", "total_value",
                 "foreign_buy_volume", "foreign_sell_volume"]:
        if col in board.columns:
            keep_cols.append(col)
    board_slim = board[keep_cols].copy()
    board_slim["date"] = today

    # Top 30 gainers
    gainers = board_slim.nlargest(30, pct_col)
    results["gainer"] = gainers
    logger.info(f"  Top gainer: {gainers.iloc[0]['symbol'] if len(gainers) > 0 else 'N/A'} "
                f"({gainers.iloc[0][pct_col]:.2f}%)" if len(gainers) > 0 else "")

    # Top 30 losers
    losers = board_slim.nsmallest(30, pct_col)
    results["loser"] = losers
    logger.info(f"  Top loser: {losers.iloc[0]['symbol'] if len(losers) > 0 else 'N/A'} "
                f"({losers.iloc[0][pct_col]:.2f}%)" if len(losers) > 0 else "")

    return results


# ============================================================
# 4. MARKET EVALUATION (tổng hợp)
# ============================================================

def collect_evaluation(index_data: dict, pe_pb_data: dict, movers_data: dict):
    """
    Tổng hợp đánh giá thị trường hôm nay:
    - VNINDEX close, volume, value
    - PE/PB median
    - Foreign net
    - Breadth (gainers vs losers ratio)
    """
    client = _get_vnstock_client()
    stock = client.stock(symbol="VNINDEX", source="KBS")

    today = datetime.now().strftime("%Y-%m-%d")

    # Lấy giá VNINDEX hôm nay
    try:
        hist = stock.quote.history(
            start=(datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"),
            end=today, interval="1D"
        )
        if hist is not None and not hist.empty:
            latest = hist.iloc[-1]
            vnindex_close = latest.get("close", None)
            vnindex_volume = latest.get("volume", None)
        else:
            vnindex_close = vnindex_volume = None
    except Exception:
        vnindex_close = vnindex_volume = None

    eval_row = {"time": today}

    if vnindex_close is not None:
        eval_row["vnindex_close"] = vnindex_close
    if vnindex_volume is not None:
        eval_row["vnindex_volume"] = vnindex_volume

    # PE/PB từ đã tính
    if "pe" in pe_pb_data and not pe_pb_data["pe"].empty:
        eval_row["market_pe_median"] = pe_pb_data["pe"].iloc[0].get("market_pe_median", None)
    if "pb" in pe_pb_data and not pe_pb_data["pb"].empty:
        eval_row["market_pb_median"] = pe_pb_data["pb"].iloc[0].get("market_pb_median", None)

    # Foreign flow từ index_data
    if "foreign_buy" in index_data and not index_data["foreign_buy"].empty:
        last_fb = index_data["foreign_buy"].iloc[-1]
        eval_row["foreign_buy_volume"] = last_fb.get("foreign_buy_volume", None)
    if "foreign_sell" in index_data and not index_data["foreign_sell"].empty:
        last_fs = index_data["foreign_sell"].iloc[-1]
        eval_row["foreign_sell_volume"] = last_fs.get("foreign_sell_volume", None)
        if "foreign_net_volume" in last_fs.index:
            eval_row["foreign_net_volume"] = last_fs.get("foreign_net_volume", None)

    # Value từ index_data
    if "value" in index_data and not index_data["value"].empty:
        eval_row["total_value"] = index_data["value"].iloc[-1].get("total_value", None)

    df = pd.DataFrame([eval_row])
    return df


# ============================================================
# MAIN COLLECTOR
# ============================================================

def collect_insights(only: list = None):
    """Thu thập tất cả market insights từ vnstock library."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    targets = only if only else INSIGHT_TYPES
    success = 0
    errors = []

    # --- Phase 1: VNINDEX lịch sử (value, volume, deal, foreign_buy, foreign_sell)
    index_types = {"value", "volume", "deal", "foreign_buy", "foreign_sell"}
    need_index = bool(index_types & set(targets))

    index_data = {}
    if need_index:
        # Kiểm tra xem có file nào cần cập nhật không
        need_fetch = False
        for t in index_types & set(targets):
            if not is_file_fresh(DATA_DIR / f"market_{t}.csv"):
                need_fetch = True
                break

        if need_fetch:
            try:
                index_data = collect_index_history()
            except Exception as e:
                logger.error(f"  Lỗi lấy VNINDEX history: {e}")
        else:
            logger.info("  Tất cả file index history đã có hôm nay, bỏ qua.")

    for t in index_types & set(targets):
        csv_path = DATA_DIR / f"market_{t}.csv"
        if is_file_fresh(csv_path):
            logger.info(f"  {t}: đã có hôm nay, bỏ qua.")
            success += 1
        elif t in index_data:
            _save_incremental(index_data[t], csv_path, date_col="time")
            logger.info(f"  {t}: {len(index_data[t])} rows → market_{t}.csv")
            success += 1
        else:
            errors.append(t)

    # --- Phase 2: PE / PB (weighted from VN30 ratio_summary)
    pe_pb_types = {"pe", "pb"}
    need_pe_pb = bool(pe_pb_types & set(targets))

    pe_pb_data = {}
    if need_pe_pb:
        need_fetch = False
        for t in pe_pb_types & set(targets):
            if not is_file_fresh(DATA_DIR / f"market_{t}.csv"):
                need_fetch = True
                break

        if need_fetch:
            try:
                pe_pb_data = collect_market_pe_pb()
            except Exception as e:
                logger.error(f"  Lỗi tính PE/PB: {e}")
        else:
            logger.info("  File PE/PB đã có hôm nay, bỏ qua.")

    for t in pe_pb_types & set(targets):
        csv_path = DATA_DIR / f"market_{t}.csv"
        if is_file_fresh(csv_path):
            logger.info(f"  {t}: đã có hôm nay, bỏ qua.")
            success += 1
        elif t in pe_pb_data:
            _save_incremental(pe_pb_data[t], csv_path, date_col="time")
            logger.info(f"  {t}: → market_{t}.csv")
            success += 1
        else:
            errors.append(t)

    # --- Phase 3: Top gainers / losers (KBS price board)
    mover_types = {"gainer", "loser"}
    need_movers = bool(mover_types & set(targets))

    movers_data = {}
    if need_movers:
        need_fetch = False
        for t in mover_types & set(targets):
            if not is_file_fresh(DATA_DIR / f"market_{t}.csv"):
                need_fetch = True
                break

        if need_fetch:
            try:
                movers_data = collect_top_movers()
            except Exception as e:
                logger.error(f"  Lỗi lấy top movers: {e}")
        else:
            logger.info("  File gainer/loser đã có hôm nay, bỏ qua.")

    for t in mover_types & set(targets):
        csv_path = DATA_DIR / f"market_{t}.csv"
        if is_file_fresh(csv_path):
            logger.info(f"  {t}: đã có hôm nay, bỏ qua.")
            success += 1
        elif t in movers_data:
            movers_data[t].to_csv(csv_path, index=False, encoding="utf-8-sig")
            logger.info(f"  {t}: {len(movers_data[t])} rows → market_{t}.csv")
            success += 1
        else:
            errors.append(t)

    # --- Phase 4: Evaluation (tổng hợp)
    if "evaluation" in targets:
        csv_path = DATA_DIR / "market_evaluation.csv"
        if is_file_fresh(csv_path):
            logger.info("  evaluation: đã có hôm nay, bỏ qua.")
            success += 1
        else:
            try:
                eval_df = collect_evaluation(index_data, pe_pb_data, movers_data)
                _save_incremental(eval_df, csv_path, date_col="time")
                logger.info(f"  evaluation: → market_evaluation.csv")
                success += 1
            except Exception as e:
                logger.error(f"  Lỗi tạo evaluation: {e}")
                errors.append("evaluation")

    logger.info(f"\nKết quả: {success}/{len(targets)} OK, lỗi: {errors}")
    return success > 0


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Thu thập thống kê thị trường (Market Insights) từ vnstock.",
    )
    parser.add_argument("--only", nargs="+", default=None,
                        choices=INSIGHT_TYPES,
                        help="Chỉ lấy 1 số chỉ số cụ thể")
    args = parser.parse_args()

    # Initialize rate limiter (registers API key for proper tier detection)
    init_rate_limiter()

    logger.info("=" * 60)
    logger.info("THU THẬP THỐNG KÊ THỊ TRƯỜNG (MARKET INSIGHTS)")
    logger.info(f"Nguồn: vnstock (KBS quote + price_board, VCI ratio_summary)")
    logger.info(f"Chỉ số: {args.only or 'Tất cả ' + str(len(INSIGHT_TYPES)) + ' loại'}")
    logger.info(f"Output: {DATA_DIR}")
    logger.info("=" * 60)

    collect_insights(only=args.only)

    logger.info("\n" + "=" * 60)
    logger.info("HOÀN TẤT!")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
