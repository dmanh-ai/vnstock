# Hệ Thống Dữ Liệu vnstock Dashboard

> Tự động thu thập hàng ngày (T2-T6, 14:50 giờ Việt Nam) qua GitHub Actions.
> Workflow: `.github/workflows/dashboard.yml`

---

## Tổng Quan

| Nhóm dữ liệu | Script | Nguồn dữ liệu | Số file | Trạng thái |
|---|---|---|---|---|
| 1. Snapshot thị trường hàng ngày | `daily_collector.py` | KBS Price Board | 8/ngày | OK |
| 2. Giá cổ phiếu OHLCV | `collect_stocks.py` | VNDirect dchart API | ~542 | OK |
| 3. Chỉ số thị trường | `collect_indices.py` | VNDirect dchart API | 19 CSV + 25 chart | OK |
| 4. Báo cáo tài chính | `collect_financials.py` | KBS Finance API | ~3,664 (458x8) | OK |
| 5. Dữ liệu bổ sung | `collect_extras.py` | SJC, VCB, VCI, KBS | 10 | OK |
| 6. Thị trường quốc tế | `collect_market.py` | MSN Finance, FMARKET, KBS | 42 | OK |
| 7. Giá hàng hóa | `collect_commodity.py` | vnstock_data (premium) | 15 | OK (premium) |
| 8. Kinh tế vĩ mô | `collect_macro.py` | vnstock_data (premium) | 9 | 9/9 OK |
| 9. Thống kê thị trường | `collect_insights.py` | vnstock_data (premium) | 10 | Đang sửa |
| **Tổng** | | | **~4,340+** | |

---

## 1. Snapshot Thị Trường Hàng Ngày

**Script:** `scripts/daily_collector.py`
**Nguồn:** KBS Price Board API (lấy bảng giá toàn sàn, lọc top 500 vốn hóa)
**Output:** `data/YYYY-MM-DD/` (mỗi ngày 1 thư mục, symlink `data/latest` -> ngày mới nhất)
**Lịch:** Mỗi ngày giao dịch

| # | File | Mô tả | Các cột chính |
|---|------|-------|--------------|
| 1 | `top_gainers.csv` | Top 30 cổ phiếu tăng giá mạnh nhất | `symbol, close_price, percent_change, total_trades, total_value` |
| 2 | `top_losers.csv` | Top 30 cổ phiếu giảm giá mạnh nhất | `symbol, close_price, percent_change, total_trades, total_value` |
| 3 | `market_breadth.csv` | Độ rộng thị trường (tăng/giảm/đứng) theo sàn | `exchange, advancing, declining, unchanged, total_stocks, net_ad` |
| 4 | `foreign_flow.csv` | Dòng tiền NDTNN tổng hợp theo sàn | `exchange, foreign_buy_volume, foreign_sell_volume, foreign_net_volume, foreign_buy_value, foreign_sell_value, foreign_net_value` |
| 5 | `foreign_top_buy.csv` | Top 20 CP được NDTNN mua ròng nhiều nhất | `symbol, exchange, close_price, foreign_buy_volume, foreign_sell_volume, foreign_net_volume, foreign_net_value` |
| 6 | `foreign_top_sell.csv` | Top 20 CP bị NDTNN bán ròng nhiều nhất | `symbol, exchange, close_price, foreign_buy_volume, foreign_sell_volume, foreign_net_volume, foreign_net_value` |
| 7 | `index_impact_positive.csv` | Top 20 CP đóng góp tích cực VN-Index | `symbol, close_price, percent_change, market_cap, impact` |
| 8 | `index_impact_negative.csv` | Top 20 CP kéo giảm VN-Index | `symbol, close_price, percent_change, market_cap, impact` |

**Ví dụ:** `data/2026-02-12/top_gainers.csv` chứa 30 mã tăng mạnh nhất ngày 12/02/2026.

---

## 2. Giá Cổ Phiếu (OHLCV + Chỉ Báo Kỹ Thuật)

**Script:** `scripts/collect_stocks.py`
**Nguồn:** VNDirect dchart API (TradingView UDF format, không cần xác thực)
**Output:** `data/stocks/{SYMBOL}.csv` (mỗi mã 1 file)
**Số lượng:** ~542 mã (top 500 theo vốn hóa + các mã đã có)
**Khoảng thời gian:** 1 năm gần nhất, cập nhật incremental hàng ngày

| # | Cột | Mô tả | Ví dụ |
|---|-----|-------|-------|
| 1 | `time` | Ngày giao dịch | `2026-02-12` |
| 2 | `open` | Giá mở cửa | `52300` |
| 3 | `high` | Giá cao nhất | `53000` |
| 4 | `low` | Giá thấp nhất | `52000` |
| 5 | `close` | Giá đóng cửa | `52800` |
| 6 | `volume` | Khối lượng giao dịch | `1234567` |
| 7 | `sma_20` | Đường trung bình 20 phiên | `51500.5` |
| 8 | `sma_50` | Đường trung bình 50 phiên | `50200.3` |
| 9 | `sma_200` | Đường trung bình 200 phiên | `48900.1` |
| 10 | `rsi_14` | Chỉ số RSI 14 phiên | `62.5` |
| 11 | `macd` | Đường MACD (12, 26) | `350.2` |
| 12 | `macd_signal` | Đường Signal MACD (9) | `280.1` |
| 13 | `macd_hist` | MACD Histogram | `70.1` |
| 14 | `bb_upper` | Bollinger Band trên (20, 2σ) | `54000` |
| 15 | `bb_lower` | Bollinger Band dưới (20, 2σ) | `49000` |
| 16 | `daily_return` | Lợi suất ngày (%) | `0.95` |
| 17 | `volatility_20d` | Biến động 20 phiên (%) | `2.1` |
| 18 | `symbol` | Mã cổ phiếu | `VNM` |

**Ví dụ:** `data/stocks/VNM.csv` - Dữ liệu giá VNM 1 năm với đầy đủ chỉ báo kỹ thuật.

---

## 3. Chỉ Số Thị Trường

**Script:** `scripts/collect_indices.py`
**Nguồn:** VNDirect dchart API
**Output:** `data/indices/`

### 3a. Danh sách 18 chỉ số

| Nhóm | Chỉ số | Mô tả |
|-------|--------|-------|
| Chính | VNINDEX | Chỉ số chung sàn HOSE |
| Chính | HNXINDEX | Chỉ số sàn HNX |
| Chính | UPCOMINDEX | Chỉ số sàn UPCOM |
| Chính | VN30 | Top 30 CP lớn nhất HOSE |
| Chính | HNX30 | Top 30 CP lớn nhất HNX |
| Quy mô | VN100 | Top 100 CP HOSE |
| Quy mô | VNMID | CP vốn hóa trung bình |
| Quy mô | VNSML | CP vốn hóa nhỏ |
| Ngành | VNFIN | Tài chính |
| Ngành | VNREAL | Bất động sản |
| Ngành | VNIT | Công nghệ |
| Ngành | VNHEAL | Y tế |
| Ngành | VNENE | Năng lượng |
| Ngành | VNCONS | Hàng tiêu dùng thiết yếu |
| Ngành | VNMAT | Nguyên vật liệu |
| Ngành | VNCOND | Hàng tiêu dùng không thiết yếu |
| Đầu tư | VNDIAMOND | VN Diamond |
| Đầu tư | VNFINSELECT | VN Finance Select |

### 3b. File dữ liệu

| # | File | Mô tả |
|---|------|-------|
| 1 | `data/indices/{INDEX}.csv` (x18) | OHLCV + chỉ báo kỹ thuật mỗi chỉ số (cùng cột như stocks) |
| 2 | `data/indices/all_indices.csv` | Gộp tất cả 18 chỉ số vào 1 file |

### 3c. Biểu đồ (PNG)

| # | File | Mô tả |
|---|------|-------|
| 1 | `charts/overview_main.png` | So sánh 5 chỉ số chính (VNINDEX, HNX, UPCOM, VN30, HNX30) |
| 2 | `charts/overview_size.png` | So sánh 3 chỉ số quy mô (VN100, VNMID, VNSML) |
| 3 | `charts/overview_sectors.png` | So sánh 8 chỉ số ngành |
| 4 | `charts/overview_invest.png` | So sánh 2 chỉ số đầu tư (VNDIAMOND, VNFINSELECT) |
| 5 | `charts/overview_all.png` | So sánh toàn bộ 18 chỉ số |
| 6 | `charts/{INDEX}.png` (x18) | Biểu đồ chi tiết 4 panel: Giá+MA+BB, Volume, RSI, MACD |
| 7 | `charts/volume_comparison.png` | So sánh khối lượng giao dịch các chỉ số chính |

---

## 4. Báo Cáo Tài Chính

**Script:** `scripts/collect_financials.py`
**Nguồn:** KBS Finance REST API
**Output:** `data/financials/{SYMBOL}/` (mỗi mã 1 thư mục, 8 file)
**Số lượng:** 458 mã x 8 file = ~3,664 file
**Cập nhật:** Theo quý (chạy riêng, không nằm trong workflow hàng ngày)

| # | File | Mô tả | Cột |
|---|------|-------|-----|
| 1 | `balance_sheet_year.csv` | Bảng cân đối kế toán (năm) | `item, item_en, 2020, 2021, 2022, ...` |
| 2 | `balance_sheet_quarter.csv` | Bảng cân đối kế toán (quý) | `item, item_en, 2024-Q1, 2024-Q2, ...` |
| 3 | `income_statement_year.csv` | Báo cáo kết quả kinh doanh (năm) | `item, item_en, 2020, 2021, ...` |
| 4 | `income_statement_quarter.csv` | Báo cáo kết quả kinh doanh (quý) | `item, item_en, 2024-Q1, ...` |
| 5 | `cash_flow_year.csv` | Lưu chuyển tiền tệ (năm) | `item, item_en, 2020, 2021, ...` |
| 6 | `cash_flow_quarter.csv` | Lưu chuyển tiền tệ (quý) | `item, item_en, 2024-Q1, ...` |
| 7 | `ratio_year.csv` | Chỉ số tài chính (năm): định giá, sinh lời, tăng trưởng, thanh khoản, chất lượng tài sản | `item, item_en, 2020, 2021, ...` |
| 8 | `ratio_quarter.csv` | Chỉ số tài chính (quý) | `item, item_en, 2024-Q1, ...` |

**Ví dụ:** `data/financials/VNM/income_statement_year.csv` - BCKQKD của Vinamilk theo năm.

---

## 5. Dữ Liệu Bổ Sung

**Script:** `scripts/collect_extras.py`
**Output:** `data/` (top-level)

| # | File | Nguồn | Mô tả | Cập nhật | Các cột chính |
|---|------|-------|-------|----------|--------------|
| 1 | `gold_prices.csv` | SJC API | Giá vàng SJC (mua/bán) | Nối thêm hàng ngày | `date, name, branch, buy_price, sell_price` |
| 2 | `exchange_rates.csv` | Vietcombank API | Tỷ giá ngoại tệ VCB | Nối thêm hàng ngày | `date, currency_code, currency_name, buy_cash, buy_transfer, sell` |
| 3 | `company_overview.csv` | VCI API | Tổng quan 200 CP lớn nhất | Ghi đè hàng ngày | `symbol, PE, PB, sector, market_cap, ...` |
| 4 | `company_ratios.csv` | VCI API | Chỉ số tài chính tổng hợp 200 CP | Ghi đè hàng ngày | `symbol, year_report, revenue, revenue_growth, net_profit, roe, roa, pe, pb, eps, ...` (40+ cột) |
| 5 | `company_events.csv` | KBS API | Sự kiện doanh nghiệp top 50 CP | Ghi đè hàng ngày | `symbol, msg, code, ...` |
| 6 | `insider_trading.csv` | KBS API | Giao dịch nội bộ top 50 CP | Ghi đè hàng ngày | `stock_code, content, buy_volume, sell_volume, volume_before, volume_after, ...` |
| 7 | `shareholders.csv` | KBS API | Cổ đông lớn top 50 CP | Ghi đè hàng ngày | `name, update_date, shares_owned, ownership_percentage, symbol` |
| 8 | `company_news.csv` | VCI API | Tin tức top 50 CP | Ghi đè hàng ngày | `news_title, created_at, news_short_content, close_price, price_change_pct, symbol` |
| 9 | `company_officers.csv` | VCI API | Ban lãnh đạo top 50 CP | Ghi đè hàng ngày | `officer_name, officer_position, officer_own_percent, quantity, symbol` |
| 10 | `subsidiaries.csv` | KBS API | Công ty con top 50 CP | Ghi đè hàng ngày | `name, charter_capital, ownership_percent, currency, type, symbol` |

---

## 6. Thị Trường Quốc Tế

**Script:** `scripts/collect_market.py`

### 6a. Tỷ Giá Ngoại Tệ (FX)

**Nguồn:** MSN Finance API
**Output:** `data/fx/{PAIR}.csv`
**Cập nhật:** Incremental (nối thêm ngày mới)

| # | File | Cặp tiền | Cột |
|---|------|----------|-----|
| 1 | `USDVND.csv` | USD/VND | `time, open, high, low, close, symbol` |
| 2 | `EURUSD.csv` | EUR/USD | (giống trên) |
| 3 | `USDJPY.csv` | USD/JPY | |
| 4 | `GBPUSD.csv` | GBP/USD | |
| 5 | `AUDUSD.csv` | AUD/USD | |
| 6 | `USDCAD.csv` | USD/CAD | |
| 7 | `USDCHF.csv` | USD/CHF | |
| 8 | `USDCNY.csv` | USD/CNY | |
| 9 | `USDKRW.csv` | USD/KRW | |
| 10 | `USDSGD.csv` | USD/SGD | |
| 11 | `EURJPY.csv` | EUR/JPY | |
| 12 | `EURGBP.csv` | EUR/GBP | |
| 13 | `GBPJPY.csv` | GBP/JPY | |
| 14 | `NZDUSD.csv` | NZD/USD | |
| 15 | `JPYVND.csv` | JPY/VND | |

### 6b. Tiền Điện Tử (Crypto)

**Nguồn:** MSN Finance Cryptocurrency API
**Output:** `data/crypto/{SYMBOL}.csv`

| # | File | Đồng coin | Cột |
|---|------|-----------|-----|
| 1 | `BTC.csv` | Bitcoin | `time, open, high, low, close, volume, symbol` |
| 2 | `ETH.csv` | Ethereum | (giống trên) |
| 3 | `BNB.csv` | Binance Coin | |
| 4 | `XRP.csv` | Ripple | |
| 5 | `ADA.csv` | Cardano | |
| 6 | `SOL.csv` | Solana | |
| 7 | `DOGE.csv` | Dogecoin | |

### 6c. Chỉ Số Thế Giới

**Nguồn:** MSN Finance Charts API
**Output:** `data/world_indices/{SYMBOL}.csv`

| # | File | Chỉ số | Thị trường | Cột |
|---|------|--------|-----------|-----|
| 1 | `DJI.csv` | Dow Jones Industrial | Mỹ | `time, open, high, low, close, volume, symbol, name` |
| 2 | `INX.csv` | S&P 500 | Mỹ | (giống trên) |
| 3 | `COMP.csv` | Nasdaq Composite | Mỹ | |
| 4 | `RUT.csv` | Russell 2000 | Mỹ | |
| 5 | `N225.csv` | Nikkei 225 | Nhật Bản | |
| 6 | `HSI.csv` | Hang Seng | Hồng Kông | |
| 7 | `000001.csv` | Shanghai Composite | Trung Quốc | |
| 8 | `SENSEX.csv` | BSE Sensex | Ấn Độ | |
| 9 | `DAX.csv` | DAX 40 | Đức | |
| 10 | `UKX.csv` | FTSE 100 | Anh | |
| 11 | `PX1.csv` | CAC 40 | Pháp | |

### 6d. Quỹ Đầu Tư Mở

**Nguồn:** FMARKET API
**Output:** `data/funds/`

| # | File | Mô tả | Các cột chính |
|---|------|-------|--------------|
| 1 | `fund_listing.csv` | Danh sách quỹ mở | `short_name, name, fund_type, management_fee, nav, nav_change_1m/3m/6m/12m, inception_date` |
| 2 | `fund_nav.csv` | Lịch sử NAV các quỹ | `date, nav_per_unit, short_name, fund` |
| 3 | `fund_holdings.csv` | Danh mục cổ phiếu nắm giữ | `stock_code, industry, net_asset_percent, short_name` |
| 4 | `fund_industry_holding.csv` | Phân bổ theo ngành | `industry, net_asset_percent, short_name` |
| 5 | `fund_asset_holding.csv` | Phân bổ theo loại tài sản | `asset_percent, asset_type, short_name` |

### 6e. Metadata Niêm Yết

**Nguồn:** KBS Listing API
**Output:** `data/metadata/`

| # | File | Mô tả |
|---|------|-------|
| 1 | `symbols_by_industry.csv` | Phân ngành CP theo ICB |
| 2 | `futures.csv` | Danh sách hợp đồng tương lai |
| 3 | `covered_warrants.csv` | Danh sách chứng quyền |
| 4 | `corporate_bonds.csv` | Danh sách trái phiếu doanh nghiệp |

---

## 7. Giá Hàng Hóa (Premium)

**Script:** `scripts/collect_commodity.py`
**Nguồn:** `vnstock_data.CommodityPrice(source='spl')` - Gói premium, cài qua CLI installer
**Output:** `data/commodity/`
**Khoảng thời gian:** 3 năm, incremental merge

| # | File | Hàng hóa | Loại dữ liệu |
|---|------|----------|--------------|
| 1 | `gold_vn.csv` | Vàng Việt Nam | Giá mua/bán |
| 2 | `gold_global.csv` | Vàng thế giới | OHLCV |
| 3 | `oil_crude.csv` | Dầu thô | OHLCV |
| 4 | `gas_natural.csv` | Khí tự nhiên | OHLCV |
| 5 | `gas_vn.csv` | Xăng Việt Nam (RON95/92/DO) | Giá bán lẻ |
| 6 | `coke.csv` | Than cốc | OHLCV |
| 7 | `steel_d10.csv` | Thép cuộn D10 | Giá |
| 8 | `iron_ore.csv` | Quặng sắt | OHLCV |
| 9 | `steel_hrc.csv` | Thép cuộn cán nóng (HRC) | OHLCV |
| 10 | `fertilizer_ure.csv` | Phân Ure | OHLCV |
| 11 | `soybean.csv` | Đậu nành | OHLCV |
| 12 | `corn.csv` | Ngô | OHLCV |
| 13 | `sugar.csv` | Đường | OHLCV |
| 14 | `pork_north_vn.csv` | Lợn hơi miền Bắc VN | Giá |
| 15 | `pork_china.csv` | Lợn hơi Trung Quốc | Giá |

---

## 8. Kinh Tế Vĩ Mô (Premium)

**Script:** `scripts/collect_macro.py`
**Nguồn:** `vnstock_data.Macro(source='mbk')` - Gói premium
**Output:** `data/macro/`
**Cập nhật:** Incremental merge theo cột date/period/year

| # | File | Chỉ số | Gọi API |
|---|------|--------|---------|
| 1 | `gdp.csv` | GDP Việt Nam | `macro.gdp()` |
| 2 | `cpi.csv` | Chỉ số giá tiêu dùng CPI | `macro.cpi()` |
| 3 | `industry_prod.csv` | Chỉ số sản xuất công nghiệp IIP | `macro.industry_prod()` |
| 4 | `import_export.csv` | Xuất nhập khẩu | `macro.import_export()` |
| 5 | `retail.csv` | Doanh thu bán lẻ | `macro.retail()` |
| 6 | `fdi.csv` | Vốn FDI | `macro.fdi()` |
| 7 | `money_supply.csv` | Cung tiền M2 | `macro.money_supply()` |
| 8 | `exchange_rate.csv` | Tỷ giá hối đoái | `macro.exchange_rate()` |
| 9 | `population_labor.csv` | Dân số và lao động | `macro.population_labor(period='year', start=2000)` |

---

## 9. Thống Kê Thị Trường / Market Insights (Premium)

**Script:** `scripts/collect_insights.py`
**Nguồn:** `vnstock_data.Trading(source='cafef', symbol='VNINDEX')` - Gói premium
**Output:** `data/insights/`
**Trạng thái:** Đang sửa - auto-discovery để tìm đúng tên methods

| # | File | Chỉ số (dự kiến) |
|---|------|-------------------|
| 1 | `market_pe.csv` | P/E thị trường |
| 2 | `market_pb.csv` | P/B thị trường |
| 3 | `market_value.csv` | Giá trị giao dịch |
| 4 | `market_volume.csv` | Khối lượng giao dịch |
| 5 | `market_deal.csv` | Số lượng deal |
| 6 | `market_foreign_buy.csv` | NDTNN mua ròng |
| 7 | `market_foreign_sell.csv` | NDTNN bán ròng |
| 8 | `market_gainer.csv` | Top tăng giá |
| 9 | `market_loser.csv` | Top giảm giá |
| 10 | `market_evaluation.csv` | Đánh giá thị trường |

> **Lưu ý:** Script đã thêm auto-discovery - nếu các method trên không tồn tại, sẽ tự động tìm và gọi tất cả methods có sẵn trên Trading class.

---

## Cấu Trúc Thư Mục

```
data/
├── 2026-02-10/                    # Snapshot ngày 10/02
├── 2026-02-11/                    # Snapshot ngày 11/02
├── 2026-02-12/                    # Snapshot ngày 12/02
│   ├── top_gainers.csv
│   ├── top_losers.csv
│   ├── market_breadth.csv
│   ├── foreign_flow.csv
│   ├── foreign_top_buy.csv
│   ├── foreign_top_sell.csv
│   ├── index_impact_positive.csv
│   └── index_impact_negative.csv
├── latest -> 2026-02-12           # Symlink đến ngày mới nhất
│
├── stocks/                        # ~542 file OHLCV
│   ├── VNM.csv
│   ├── VCB.csv
│   ├── HPG.csv
│   └── ...
│
├── indices/                       # 18 chỉ số + biểu đồ
│   ├── VNINDEX.csv
│   ├── VN30.csv
│   ├── all_indices.csv
│   └── charts/
│       ├── overview_main.png
│       ├── VNINDEX.png
│       └── ...
│
├── financials/                    # ~458 mã x 8 file
│   ├── VNM/
│   │   ├── balance_sheet_year.csv
│   │   ├── balance_sheet_quarter.csv
│   │   ├── income_statement_year.csv
│   │   ├── income_statement_quarter.csv
│   │   ├── cash_flow_year.csv
│   │   ├── cash_flow_quarter.csv
│   │   ├── ratio_year.csv
│   │   └── ratio_quarter.csv
│   └── ...
│
├── fx/                            # 15 cặp tỷ giá
│   ├── USDVND.csv
│   ├── EURUSD.csv
│   └── ...
│
├── crypto/                        # 7 đồng coin
│   ├── BTC.csv
│   ├── ETH.csv
│   └── ...
│
├── world_indices/                 # 11 chỉ số thế giới
│   ├── DJI.csv
│   ├── INX.csv
│   └── ...
│
├── funds/                         # 5 file quỹ đầu tư
│   ├── fund_listing.csv
│   ├── fund_nav.csv
│   ├── fund_holdings.csv
│   ├── fund_industry_holding.csv
│   └── fund_asset_holding.csv
│
├── metadata/                      # 4 file metadata
│   ├── symbols_by_industry.csv
│   ├── futures.csv
│   ├── covered_warrants.csv
│   └── corporate_bonds.csv
│
├── commodity/                     # 15 hàng hóa (premium)
│   ├── gold_vn.csv
│   ├── oil_crude.csv
│   └── ...
│
├── macro/                         # 9 chỉ số vĩ mô (premium)
│   ├── gdp.csv
│   ├── cpi.csv
│   └── ...
│
├── insights/                      # 10 thống kê TT (premium)
│   ├── market_pe.csv
│   └── ...
│
├── gold_prices.csv                # Giá vàng SJC
├── exchange_rates.csv             # Tỷ giá VCB
├── company_overview.csv           # Tổng quan 200 CP
├── company_ratios.csv             # Chỉ số tài chính 200 CP
├── company_events.csv             # Sự kiện DN top 50
├── insider_trading.csv            # Giao dịch nội bộ top 50
├── shareholders.csv               # Cổ đông lớn top 50
├── company_news.csv               # Tin tức top 50
├── company_officers.csv           # Ban lãnh đạo top 50
└── subsidiaries.csv               # Công ty con top 50
```

---

## Nguồn Dữ Liệu

| Nguồn | Loại | Xác thực | Sử dụng trong |
|--------|------|----------|---------------|
| KBS (KB Securities) | REST API | Không | Price board, Financials, Listing, Events, Insider, Shareholders, Subsidiaries |
| VCI (Vietcap) | REST API | Không | Company overview/ratios/news/officers |
| VNDirect dchart | REST API (TradingView UDF) | Không | Stock OHLCV, Index OHLCV |
| MSN Finance | REST API | Dynamic API key (tự lấy) | FX, Crypto, World Indices |
| FMARKET | REST API | Không | Quỹ đầu tư mở |
| SJC | REST API | Không | Giá vàng |
| Vietcombank | REST API | Không | Tỷ giá ngoại tệ |
| vnstock_data (premium) | Python package | CLI installer | Commodity, Macro, Insights |

---

## Cơ Chế Cập Nhật

| Cơ chế | Áp dụng cho | Mô tả |
|--------|-------------|-------|
| **Incremental (nối thêm)** | Stocks, Indices, FX, Crypto, World, Gold, Exchange rates, Commodity, Macro | Đọc file cũ → fetch dữ liệu mới → merge → loại trùng theo ngày → ghi đè |
| **Freshness check (bỏ qua nếu mới)** | Daily snapshot, Extras, Fund, Macro, Insights | Kiểm tra mtime file < 20h → skip nếu đã cập nhật hôm nay |
| **Snapshot (ghi đè)** | Company overview/ratios, Events, Insider, Shareholders, News, Officers, Subsidiaries | Lấy mới hoàn toàn, ghi đè file cũ |
