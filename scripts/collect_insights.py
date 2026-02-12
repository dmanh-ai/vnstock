"""
Thu thập thống kê thị trường / Market Insights từ vnstock_data (Market + TopStock).

Yêu cầu: pip install vnstock_data (Insiders Program)
Fallback: Nếu vnstock_data không có, tự động dùng vnstock (free) với KBS/VCI APIs.

Nguồn dữ liệu:
  - vnstock_data.Market(index='VNINDEX') → PE, PB, evaluation (VND)
  - vnstock_data.TopStock() → gainer, loser, value, volume, deal, foreign (VND)
  - Fallback: vnstock KBS quote.history + VCI ratio_summary

Output:
    data/insights/market_pe.csv          - P/E thị trường VNINDEX theo thời gian
    data/insights/market_pb.csv          - P/B thị trường VNINDEX theo thời gian
    data/insights/market_value.csv       - Top CP theo giá trị giao dịch
    data/insights/market_volume.csv      - Top CP theo khối lượng đột biến
    data/insights/market_deal.csv        - Top CP theo thỏa thuận đột biến
    data/insights/market_foreign_buy.csv - Top NDTNN mua ròng
    data/insights/market_foreign_sell.csv- Top NDTNN bán ròng
    data/insights/market_gainer.csv      - Top tăng giá hôm nay
    data/insights/market_loser.csv       - Top giảm giá hôm nay
    data/insights/market_evaluation.csv  - Đánh giá thị trường (PE + PB kết hợp)

Cách chạy:
    python scripts/collect_insights.py
    python scripts/collect_insights.py --only pe pb foreign_buy
    python scripts/collect_insights.py --duration 3Y
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

DATA_DIR = PROJECT_ROOT / "data" / "insights"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("insights")

# Các loại insights
INSIGHT_TYPES = [
    "pe", "pb", "value", "volume", "deal",
    "foreign_buy", "foreign_sell",
    "gainer", "loser", "evaluation",
]

# Market methods (từ vnstock_data.Market)
MARKET_METHODS = ["pe", "pb", "evaluation"]

# TopStock methods (từ vnstock_data.TopStock)
TOPSTOCK_METHODS = ["gainer", "loser", "value", "volume", "deal", "foreign_buy", "foreign_sell"]


# ============================================================
# HELPERS
# ============================================================

def _save_incremental(df, csv_path, date_col=None):
    """Merge DataFrame mới với dữ liệu cũ, loại trùng theo date_col."""
    if csv_path.exists() and date_col:
        try:
            existing = pd.read_csv(csv_path)
            if date_col in df.columns and date_col in existing.columns:
                df = pd.concat([existing, df], ignore_index=True)
                df = df.drop_duplicates(subset=[date_col], keep="last")
                df = df.sort_values(date_col).reset_index(drop=True)
        except Exception:
            pass
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")


def _discover_methods(obj, class_name: str):
    """Log tất cả public methods có thể gọi được trên object."""
    public = [
        m for m in dir(obj)
        if not m.startswith("_") and callable(getattr(obj, m, None))
    ]
    logger.info(f"  [DISCOVERY] {class_name} có {len(public)} methods: {public}")
    return public


# ============================================================
# COLLECTOR: vnstock_data (primary)
# ============================================================

def collect_with_vnstock_data(targets: list, duration: str = "5Y"):
    """
    Thu thập insights bằng vnstock_data.Market và vnstock_data.TopStock.
    Trả về dict {type_name: DataFrame}.
    """
    results = {}

    # --- Market class (PE, PB, Evaluation) ---
    market_targets = [t for t in targets if t in MARKET_METHODS]
    if market_targets:
        try:
            from vnstock_data import Market
            market = Market(index="VNINDEX")
            _discover_methods(market, "Market(index='VNINDEX')")

            for method_name in market_targets:
                try:
                    method = getattr(market, method_name, None)
                    if method is None:
                        continue
                    logger.info(f"  Market.{method_name}(duration='{duration}')...")
                    df = method(duration=duration)
                    if isinstance(df, pd.DataFrame) and not df.empty:
                        results[method_name] = df
                        logger.info(f"    → {len(df)} rows")
                    else:
                        logger.warning(f"    → rỗng")
                except Exception as e:
                    logger.warning(f"    Market.{method_name}: lỗi - {e}")
        except ImportError:
            logger.warning("  vnstock_data.Market không khả dụng")

    # --- TopStock class (gainer, loser, value, volume, deal, foreign) ---
    top_targets = [t for t in targets if t in TOPSTOCK_METHODS]
    if top_targets:
        try:
            from vnstock_data import TopStock
            top = TopStock(show_log=False)
            _discover_methods(top, "TopStock")

            for method_name in top_targets:
                try:
                    method = getattr(top, method_name, None)
                    if method is None:
                        continue

                    # foreign_buy/sell cần param date, gainer/loser cần index
                    if method_name in ("foreign_buy", "foreign_sell"):
                        logger.info(f"  TopStock.{method_name}(limit=30)...")
                        df = method(limit=30)
                    else:
                        logger.info(f"  TopStock.{method_name}(index='VNINDEX', limit=30)...")
                        df = method(index="VNINDEX", limit=30)

                    if isinstance(df, pd.DataFrame) and not df.empty:
                        results[method_name] = df
                        logger.info(f"    → {len(df)} rows")
                    else:
                        logger.warning(f"    → rỗng")
                except Exception as e:
                    logger.warning(f"    TopStock.{method_name}: lỗi - {e}")
        except ImportError:
            logger.warning("  vnstock_data.TopStock không khả dụng")

    return results


# ============================================================
# FALLBACK: vnstock free (KBS + VCI)
# ============================================================

def collect_fallback(targets: list, already_done: set):
    """
    Fallback dùng vnstock (free) cho các targets chưa lấy được.
    """
    remaining = [t for t in targets if t not in already_done]
    if not remaining:
        return {}

    logger.info(f"  Fallback vnstock (free) cho: {remaining}")
    results = {}

    try:
        from vnstock import Vnstock
    except ImportError:
        logger.error("  vnstock cũng không cài được!")
        return {}

    client = Vnstock(show_log=False)

    # PE/PB: Tính từ VCI ratio_summary VN30
    pe_pb_needed = [t for t in remaining if t in ("pe", "pb")]
    if pe_pb_needed:
        results.update(_fallback_pe_pb(client, pe_pb_needed))

    # VNINDEX history: volume, value, deal, foreign
    index_needed = [t for t in remaining if t in ("value", "volume", "deal", "foreign_buy", "foreign_sell")]
    if index_needed:
        results.update(_fallback_index_history(client, index_needed))

    # Gainer/loser: KBS price board
    mover_needed = [t for t in remaining if t in ("gainer", "loser")]
    if mover_needed:
        results.update(_fallback_top_movers(client, mover_needed))

    # Evaluation: VNINDEX close + PE/PB
    if "evaluation" in remaining:
        results.update(_fallback_evaluation(client, results))

    return results


def _fallback_pe_pb(client, targets):
    """Tính PE/PB từ VCI ratio_summary."""
    results = {}
    stock = client.stock(symbol="ACB", source="KBS")
    try:
        vn30 = stock.listing.symbols_by_group(group="VN30")
        symbols = vn30["symbol"].tolist() if "symbol" in vn30.columns else []
    except Exception:
        symbols = ["VNM", "VCB", "VHM", "HPG", "FPT", "VIC", "MSN", "MWG",
                    "TCB", "CTG", "BID", "MBB", "ACB", "VPB", "SSI", "GAS"]

    all_ratios = []
    for symbol in symbols:
        try:
            s = client.stock(symbol=symbol, source="VCI")
            rs = s.company.ratio_summary()
            if rs is not None and not rs.empty:
                latest = rs.iloc[-1:].copy()
                latest["symbol"] = symbol
                all_ratios.append(latest)
        except Exception:
            continue

    if not all_ratios:
        return results

    ratios_df = pd.concat(all_ratios, ignore_index=True)
    today = datetime.now().strftime("%Y-%m-%d")

    if "pe" in targets and "pe" in ratios_df.columns:
        pe_data = ratios_df[["symbol", "pe"]].dropna(subset=["pe"])
        pe_data = pe_data[pe_data["pe"] > 0]
        if not pe_data.empty:
            results["pe"] = pd.DataFrame([{
                "time": today,
                "market_pe_median": round(pe_data["pe"].median(), 2),
                "market_pe_mean": round(pe_data["pe"].mean(), 2),
                "num_stocks": len(pe_data),
            }])

    if "pb" in targets and "pb" in ratios_df.columns:
        pb_data = ratios_df[["symbol", "pb"]].dropna(subset=["pb"])
        pb_data = pb_data[pb_data["pb"] > 0]
        if not pb_data.empty:
            results["pb"] = pd.DataFrame([{
                "time": today,
                "market_pb_median": round(pb_data["pb"].median(), 2),
                "market_pb_mean": round(pb_data["pb"].mean(), 2),
                "num_stocks": len(pb_data),
            }])

    return results


def _fallback_index_history(client, targets):
    """Lấy VNINDEX history từ KBS."""
    results = {}
    stock = client.stock(symbol="VNINDEX", source="KBS")
    end = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")

    try:
        df = stock.quote.history(start=start, end=end, interval="1D", get_all=True)
    except Exception as e:
        logger.warning(f"  KBS VNINDEX history: lỗi - {e}")
        return {}

    if df is None or df.empty:
        return {}

    col_map = {
        "volume": "volume", "value": "total_value", "deal": "total_trades",
        "foreign_buy": "foreign_buy_volume", "foreign_sell": "foreign_sell_volume",
    }
    for t in targets:
        col = col_map.get(t)
        if col and col in df.columns:
            results[t] = df[["time", col]].copy()

    return results


def _fallback_top_movers(client, targets):
    """Lấy top tăng/giảm từ KBS price_board."""
    results = {}
    stock = client.stock(symbol="ACB", source="KBS")

    try:
        all_syms = stock.listing.symbols_by_group(group="HOSE")
        symbols = all_syms["symbol"].tolist() if "symbol" in all_syms.columns else []
    except Exception:
        return {}

    all_dfs = []
    for i in range(0, len(symbols), 100):
        batch = symbols[i:i + 100]
        try:
            df = stock.trading.price_board(symbols_list=batch, get_all=True)
            if df is not None and not df.empty:
                all_dfs.append(df)
        except Exception:
            continue

    if not all_dfs:
        return {}

    board = pd.concat(all_dfs, ignore_index=True)
    pct_col = next((c for c in ["percent_change", "price_change_pct"] if c in board.columns), None)
    if not pct_col:
        return {}

    board[pct_col] = pd.to_numeric(board[pct_col], errors="coerce")
    board = board.dropna(subset=[pct_col])
    board["date"] = datetime.now().strftime("%Y-%m-%d")

    if "gainer" in targets:
        results["gainer"] = board.nlargest(30, pct_col)
    if "loser" in targets:
        results["loser"] = board.nsmallest(30, pct_col)

    return results


def _fallback_evaluation(client, existing_data):
    """Tạo evaluation tổng hợp."""
    stock = client.stock(symbol="VNINDEX", source="KBS")
    today = datetime.now().strftime("%Y-%m-%d")
    row = {"time": today}

    try:
        hist = stock.quote.history(
            start=(datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"),
            end=today, interval="1D"
        )
        if hist is not None and not hist.empty:
            row["vnindex_close"] = hist.iloc[-1].get("close")
            row["vnindex_volume"] = hist.iloc[-1].get("volume")
    except Exception:
        pass

    return {"evaluation": pd.DataFrame([row])}


# ============================================================
# MAIN COLLECTOR
# ============================================================

def collect_insights(only: list = None, duration: str = "5Y"):
    """Thu thập market insights. Ưu tiên vnstock_data, fallback vnstock free."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    targets = only if only else INSIGHT_TYPES
    success = 0
    errors = []

    # Kiểm tra file nào cần cập nhật
    need_update = []
    for t in targets:
        csv_path = DATA_DIR / f"market_{t}.csv"
        if is_file_fresh(csv_path):
            logger.info(f"  {t}: đã có hôm nay, bỏ qua.")
            success += 1
        else:
            need_update.append(t)

    if not need_update:
        logger.info("  Tất cả file đã mới, không cần cập nhật.")
        return True

    # Phase 1: vnstock_data (premium)
    logger.info(f"\n--- Phase 1: vnstock_data (premium) cho {need_update} ---")
    premium_data = collect_with_vnstock_data(need_update, duration=duration)

    # Phase 2: Fallback vnstock (free) cho phần còn thiếu
    done_set = set(premium_data.keys())
    remaining = [t for t in need_update if t not in done_set]
    fallback_data = {}
    if remaining:
        logger.info(f"\n--- Phase 2: Fallback vnstock (free) cho {remaining} ---")
        fallback_data = collect_fallback(remaining, done_set)

    # Merge kết quả
    all_data = {**premium_data, **fallback_data}

    # Lưu file
    for t in need_update:
        csv_path = DATA_DIR / f"market_{t}.csv"
        if t in all_data and all_data[t] is not None:
            df = all_data[t]
            if isinstance(df, pd.DataFrame) and not df.empty:
                # Tìm cột ngày để merge incremental
                date_col = None
                for col in ["reportDate", "date", "time", "Date", "Time"]:
                    if col in df.columns:
                        date_col = col
                        break
                _save_incremental(df, csv_path, date_col=date_col)
                logger.info(f"  ✓ {t}: {len(df)} rows → market_{t}.csv")
                success += 1
            else:
                logger.warning(f"  ✗ {t}: DataFrame rỗng")
                errors.append(t)
        else:
            logger.warning(f"  ✗ {t}: không có dữ liệu")
            errors.append(t)

    logger.info(f"\nKết quả: {success}/{len(targets)} OK, lỗi: {errors}")
    return success > 0


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Thu thập thống kê thị trường (Market Insights).",
    )
    parser.add_argument("--only", nargs="+", default=None,
                        choices=INSIGHT_TYPES,
                        help="Chỉ lấy 1 số chỉ số cụ thể")
    parser.add_argument("--duration", default="5Y",
                        choices=["1Y", "2Y", "3Y", "5Y", "10Y"],
                        help="Khoảng thời gian PE/PB (mặc định 5Y)")
    args = parser.parse_args()

    init_rate_limiter()

    logger.info("=" * 60)
    logger.info("THU THẬP THỐNG KÊ THỊ TRƯỜNG (MARKET INSIGHTS)")
    logger.info(f"Nguồn: vnstock_data Market+TopStock → fallback vnstock KBS/VCI")
    logger.info(f"Chỉ số: {args.only or 'Tất cả ' + str(len(INSIGHT_TYPES)) + ' loại'}")
    logger.info(f"Duration PE/PB: {args.duration}")
    logger.info(f"Output: {DATA_DIR}")
    logger.info("=" * 60)

    collect_insights(only=args.only, duration=args.duration)

    logger.info("\n" + "=" * 60)
    logger.info("HOÀN TẤT!")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
