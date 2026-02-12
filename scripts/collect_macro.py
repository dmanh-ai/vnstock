"""
Thu thập dữ liệu kinh tế vĩ mô từ vnstock_data Macro (source='mbk').

Yêu cầu: pip install vnstock_data (Insiders Program)

Output:
    data/macro/gdp.csv              - GDP Việt Nam
    data/macro/cpi.csv              - Chỉ số giá tiêu dùng CPI
    data/macro/industry_prod.csv    - Chỉ số sản xuất công nghiệp IIP
    data/macro/import_export.csv    - Xuất nhập khẩu
    data/macro/retail.csv           - Doanh thu bán lẻ
    data/macro/fdi.csv              - Vốn FDI
    data/macro/money_supply.csv     - Cung tiền M2
    data/macro/exchange_rate.csv    - Tỷ giá hối đoái
    data/macro/population_labor.csv - Dân số và lao động

Cách chạy:
    python scripts/collect_macro.py                    # Tất cả
    python scripts/collect_macro.py --only gdp cpi     # Chỉ GDP và CPI
"""

import sys
import logging
import argparse
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

import pandas as pd
from utils import init_rate_limiter, is_file_fresh

# ============================================================
# CẤU HÌNH
# ============================================================

DATA_DIR = PROJECT_ROOT / "data" / "macro"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("macro")

# Tất cả chỉ số vĩ mô có sẵn (gọi không tham số)
MACRO_METHODS = [
    "gdp",
    "cpi",
    "industry_prod",
    "import_export",
    "retail",
    "fdi",
    "money_supply",
    "exchange_rate",
]

# Methods cần tham số đặc biệt
MACRO_METHODS_WITH_PARAMS = {
    "population_labor": {"period": "year", "start": 2000},
}


# ============================================================
# COLLECTOR
# ============================================================

def _discover_methods(obj, class_name: str):
    """Log tất cả public methods có thể gọi được trên object."""
    public = [
        m for m in dir(obj)
        if not m.startswith("_") and callable(getattr(obj, m, None))
    ]
    logger.info(f"  [DISCOVERY] {class_name} có {len(public)} public methods: {public}")
    return public


def _save_incremental(df, csv_path, method_name):
    """Merge DataFrame mới với dữ liệu cũ, loại trùng."""
    if csv_path.exists():
        try:
            existing = pd.read_csv(csv_path)
            date_col = None
            for col in ['date', 'time', 'Date', 'Time', 'period', 'year']:
                if col in df.columns:
                    date_col = col
                    break
            if date_col:
                df = pd.concat([existing, df], ignore_index=True)
                df = df.drop_duplicates(subset=[date_col], keep="last")
                df = df.sort_values(date_col).reset_index(drop=True)
                logger.info(f"  {method_name}: merged ({len(df)} rows total)")
        except Exception:
            pass
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    logger.info(f"  {method_name}: {len(df)} rows → {csv_path.name}")


def collect_macro(only: list = None):
    """Thu thập dữ liệu kinh tế vĩ mô từ vnstock_data Macro."""
    try:
        from vnstock_data import Macro
    except ImportError:
        logger.error(
            "vnstock_data chưa được cài đặt. "
            "Package này thuộc Insiders Program. "
            "Liên hệ support@vnstocks.com để cài đặt."
        )
        return False

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("Khởi tạo Macro(source='mbk')")
    macro = Macro(source='mbk')

    # Discovery: log tất cả methods để debug
    _discover_methods(macro, "Macro(source='mbk')")

    # Build method list
    all_methods = list(MACRO_METHODS) + list(MACRO_METHODS_WITH_PARAMS.keys())
    methods = only if only else all_methods
    success = 0
    errors = []

    for method_name in methods:
        csv_path = DATA_DIR / f"{method_name}.csv"

        # Skip nếu file đã được cập nhật hôm nay
        if is_file_fresh(csv_path):
            logger.info(f"  {method_name}: đã có hôm nay, bỏ qua.")
            success += 1
            continue

        try:
            method = getattr(macro, method_name, None)
            if method is None:
                logger.warning(f"  {method_name}: method không tồn tại, bỏ qua.")
                errors.append(method_name)
                continue

            logger.info(f"  Đang lấy {method_name}...")

            # Call with special params if configured
            params = MACRO_METHODS_WITH_PARAMS.get(method_name, {})
            df = method(**params) if params else method()

            if df is not None and not df.empty:
                _save_incremental(df, csv_path, method_name)
                success += 1
            else:
                logger.warning(f"  {method_name}: không có dữ liệu.")
                errors.append(method_name)

        except NotImplementedError:
            logger.warning(f"  {method_name}: chưa hỗ trợ cho source 'mbk'.")
            errors.append(method_name)
        except Exception as e:
            logger.warning(f"  {method_name}: lỗi - {e}")
            errors.append(method_name)

    logger.info(f"\nKết quả: {success}/{len(methods)} OK, lỗi: {errors}")
    return success > 0


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Thu thập dữ liệu kinh tế vĩ mô từ vnstock_data Macro.",
    )
    parser.add_argument("--only", nargs="+", default=None,
                        help="Chỉ lấy 1 số chỉ số cụ thể")
    args = parser.parse_args()

    # Initialize rate limiter (registers API key for proper tier detection)
    init_rate_limiter()

    logger.info("=" * 60)
    logger.info("THU THẬP DỮ LIỆU KINH TẾ VĨ MÔ")
    logger.info(f"Nguồn: vnstock_data Macro (MBK)")
    all_count = len(MACRO_METHODS) + len(MACRO_METHODS_WITH_PARAMS)
    logger.info(f"Chỉ số: {args.only or 'Tất cả ' + str(all_count) + ' loại'}")
    logger.info(f"Output: {DATA_DIR}")
    logger.info("=" * 60)

    collect_macro(only=args.only)

    logger.info("\n" + "=" * 60)
    logger.info("HOÀN TẤT!")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
