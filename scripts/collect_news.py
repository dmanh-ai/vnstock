"""
Thu thập tin tức tài chính từ vnstock_news (BatchCrawler + TrendingAnalyzer).

Yêu cầu: pip install vnstock_news (Insiders Program)

Nguồn: 12+ trang tin tài chính VN:
    vnexpress, tuoitre, cafef, cafebiz, vietstock, vneconomy,
    baodautu, plo, baomoi, thesaigontimes, nhipcaudautu, congthuong

Output:
    data/news/YYYY-MM-DD/cafef.csv       - Tin từ CafeF
    data/news/YYYY-MM-DD/vnexpress.csv   - Tin từ VnExpress
    data/news/YYYY-MM-DD/vietstock.csv   - Tin từ VietStock
    data/news/YYYY-MM-DD/vneconomy.csv   - Tin từ VnEconomy
    data/news/YYYY-MM-DD/baodautu.csv    - Tin từ Báo Đầu Tư
    data/news/trending.csv               - Xu hướng từ khóa (append)

Cách chạy:
    python scripts/collect_news.py                       # 5 nguồn mặc định
    python scripts/collect_news.py --sites cafef vnexpress  # Chỉ 1 số nguồn
    python scripts/collect_news.py --limit 200            # Số bài tối đa/nguồn
    python scripts/collect_news.py --all-sites             # Tất cả 12 nguồn
"""

import sys
import logging
import argparse
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

import pandas as pd
from utils import init_rate_limiter, is_file_fresh

# ============================================================
# CẤU HÌNH
# ============================================================

DATA_DIR = PROJECT_ROOT / "data" / "news"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("news")

# Top 5 nguồn tin tài chính (mặc định)
DEFAULT_SITES = ["cafef", "vnexpress", "vietstock", "vneconomy", "baodautu"]

# Tất cả nguồn hỗ trợ
ALL_SITES = [
    "cafef", "vnexpress", "vietstock", "vneconomy", "baodautu",
    "tuoitre", "cafebiz", "plo", "baomoi", "thesaigontimes",
    "nhipcaudautu", "congthuong",
]


# ============================================================
# COLLECTORS
# ============================================================

def collect_news_from_site(site_name: str, limit: int = 100):
    """Thu thập tin từ 1 nguồn bằng BatchCrawler."""
    from vnstock_news import BatchCrawler

    crawler = BatchCrawler(site_name=site_name, request_delay=1.0, debug=False)
    logger.info(f"  Đang crawl {site_name} (limit={limit})...")

    df = crawler.fetch_articles(limit=limit)

    if df is not None and not df.empty:
        logger.info(f"    → {len(df)} bài viết")
        return df
    else:
        logger.warning(f"    → 0 bài viết")
        return pd.DataFrame()


def analyze_trends(all_articles: pd.DataFrame, top_n: int = 30):
    """Phân tích xu hướng từ khóa từ tiêu đề tin tức."""
    from vnstock_news.trending.analyzer import TrendingAnalyzer

    analyzer = TrendingAnalyzer(min_token_length=3)

    # Feed titles + descriptions
    for col in ["title", "short_description"]:
        if col in all_articles.columns:
            for text in all_articles[col].dropna():
                analyzer.update_trends(str(text))

    trends = analyzer.get_top_trends(top_n=top_n)
    if not trends:
        return pd.DataFrame()

    today = datetime.now().strftime("%Y-%m-%d")
    rows = [{"date": today, "keyword": k, "count": v} for k, v in trends.items()]
    return pd.DataFrame(rows).sort_values("count", ascending=False).reset_index(drop=True)


# ============================================================
# MAIN
# ============================================================

def collect_news(sites: list = None, limit: int = 100, skip_trends: bool = False):
    """Thu thập tin tức từ nhiều nguồn."""
    try:
        from vnstock_news import BatchCrawler
    except ImportError:
        logger.error(
            "vnstock_news chưa cài đặt. Package thuộc Insiders Program. "
            "Xem: https://vnstocks.com/onboard-member"
        )
        return False

    sites = sites or DEFAULT_SITES
    today = datetime.now().strftime("%Y-%m-%d")
    day_dir = DATA_DIR / today
    day_dir.mkdir(parents=True, exist_ok=True)

    success = 0
    all_articles = []

    for site in sites:
        csv_path = day_dir / f"{site}.csv"
        if is_file_fresh(csv_path):
            logger.info(f"  {site}: đã có hôm nay, bỏ qua.")
            # Vẫn load để phân tích trends
            try:
                existing = pd.read_csv(csv_path)
                all_articles.append(existing)
            except Exception:
                pass
            success += 1
            continue

        try:
            df = collect_news_from_site(site, limit=limit)
            if not df.empty:
                df.to_csv(csv_path, index=False, encoding="utf-8-sig")
                all_articles.append(df)
                success += 1
        except Exception as e:
            logger.warning(f"  {site}: lỗi - {e}")

    # Cập nhật symlink latest
    latest_link = DATA_DIR / "latest"
    if latest_link.is_symlink() or latest_link.exists():
        latest_link.unlink()
    try:
        latest_link.symlink_to(today)
    except Exception:
        pass

    # Phân tích xu hướng
    if not skip_trends and all_articles:
        try:
            combined = pd.concat(all_articles, ignore_index=True)
            logger.info(f"\n  Phân tích xu hướng từ {len(combined)} bài viết...")
            trends_df = analyze_trends(combined)
            if not trends_df.empty:
                trends_path = DATA_DIR / "trending.csv"
                # Append incremental
                if trends_path.exists():
                    existing = pd.read_csv(trends_path)
                    trends_df = pd.concat([existing, trends_df], ignore_index=True)
                    trends_df = trends_df.drop_duplicates(
                        subset=["date", "keyword"], keep="last"
                    )
                trends_df.to_csv(trends_path, index=False, encoding="utf-8-sig")
                logger.info(f"    Top trends: {trends_df.head(5)['keyword'].tolist()}")
        except ImportError:
            logger.info("  TrendingAnalyzer không khả dụng, bỏ qua trends.")
        except Exception as e:
            logger.warning(f"  Trends: lỗi - {e}")

    logger.info(f"\nKết quả: {success}/{len(sites)} nguồn OK")
    return success > 0


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Thu thập tin tức tài chính từ vnstock_news.",
    )
    parser.add_argument("--sites", nargs="+", default=None,
                        choices=ALL_SITES,
                        help="Chọn nguồn tin cụ thể")
    parser.add_argument("--all-sites", action="store_true",
                        help="Thu thập từ tất cả 12 nguồn")
    parser.add_argument("--limit", type=int, default=100,
                        help="Số bài tối đa mỗi nguồn (mặc định 100)")
    parser.add_argument("--skip-trends", action="store_true",
                        help="Bỏ qua phân tích xu hướng")
    args = parser.parse_args()

    init_rate_limiter()

    sites = ALL_SITES if args.all_sites else args.sites

    logger.info("=" * 60)
    logger.info("THU THẬP TIN TỨC TÀI CHÍNH (vnstock_news)")
    logger.info(f"Nguồn: {sites or DEFAULT_SITES}")
    logger.info(f"Limit: {args.limit} bài/nguồn")
    logger.info(f"Output: {DATA_DIR}")
    logger.info("=" * 60)

    collect_news(sites=sites, limit=args.limit, skip_trends=args.skip_trends)

    logger.info("\n" + "=" * 60)
    logger.info("HOÀN TẤT!")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
