import tushare as ts
import pandas as pd
import json
import os
import akshare as ak
import yfinance as yf
import requests
from datetime import datetime

class DataFetcher:
    def __init__(self, config_path="config/config.json"):
        self.config = self._load_config(config_path)
    
    def _load_config(self, path):
        if not os.path.exists(path):
            return {}
        with open(path, 'r') as f:
            return json.load(f)

    def fetch(self):
        raise NotImplementedError

from dotenv import load_dotenv


def _normalize_token(value):
    if value is None:
        return None
    normalized = str(value).strip().strip('"').strip("'")
    return normalized or None

class TushareFetcher(DataFetcher):
    def __init__(self, config_path="config/config.json"):
        super().__init__(config_path)
        load_dotenv() # Load .env file
        token = _normalize_token(os.getenv("TUSHARE_TOKEN"))
        # Fallback to config if needed, but per request we strictly prefer env or separate auth
        if not token:
             token = _normalize_token(self.config.get("tushare_token"))
             
        if token:
            ts.set_token(token)
            try:
                # Prefer explicit token injection so spawned worker processes do not
                # depend on tushare's local token cache file.
                self.pro = ts.pro_api(token=token)
            except TypeError:
                self.pro = ts.pro_api(token)
        else:
            print("Warning: Tushare token not found in config.")
            self.pro = None

    def fetch_macro_money(self, start_date, end_date):
        """Fetch M2 supply data."""
        return self.pro.cn_m(start_m=start_date, end_m=end_date)

    def fetch_macro_ppi(self, start_date, end_date):
        """Fetch PPI data."""
        return self.pro.cn_ppi(start_m=start_date, end_m=end_date)
    
    def fetch_macro_pmi(self, start_date, end_date):
        """Fetch PMI data."""
        return self.pro.cn_pmi(start_m=start_date, end_m=end_date)

    def fetch_macro_gdp(self, start_date, end_date):
        """Fetch GDP data."""
        # Tushare cn_gdp typically returns quarterly data
        return self.pro.cn_gdp(start_q=start_date, end_q=end_date)

    def fetch_index_valuation(self, ts_code, start_date, end_date):
        """Fetch index daily basic (PE, PB)."""
        return self.pro.index_dailybasic(ts_code=ts_code, start_date=start_date, end_date=end_date)

    def fetch_index_daily(self, ts_code, start_date, end_date):
        """
        Fetch index daily OHLC data from Tushare index_daily API.
        单次最多8000行，超出时分批获取。同时支持akshare备选。
        """
        # 优先使用 Tushare
        if self.pro:
            try:
                # 先尝试直接获取
                df = self.pro.index_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
                if df is not None and len(df) < 8000:
                    df['trade_date'] = pd.to_datetime(df['trade_date'])
                    return df.sort_values('trade_date')

                # 超过8000行，分批获取
                if len(df) >= 8000:
                    all_data = []
                    # 分割时间范围（例如按年分割）
                    years = ['2024', '2023', '2022', '2021', '2020', '2019']
                    for year in years:
                        batch = self.pro.index_daily(
                            ts_code=ts_code,
                            start_date=f"{year}0101",
                            end_date=f"{year}1231"
                        )
                        if batch is not None and not batch.empty:
                            all_data.append(batch)
                    if all_data:
                        df = pd.concat(all_data, ignore_index=True)
                        df['trade_date'] = pd.to_datetime(df['trade_date'])
                        return df.sort_values('trade_date')
            except Exception as e:
                print(f"Tushare index_daily failed: {e}")

        # Tushare失败时，使用akshare备选
        try:
            import akshare as ak
            # akshare接口映射
            akshare_map = {
                "000001.SH": "sh000001",
                "000016.SH": "sh000016",
                "000300.SH": "sh000300",
                "000905.SH": "sh000905",
                "399001.SZ": "sz399001",
                "399005.SZ": "sz399005",
                "399006.SZ": "sz399006",
                # "399300.SZ": "sz399300",
                # "399905.SZ": "sz399905",
            }
            akshare_code = akshare_map.get(ts_code)
            if akshare_code:
                df = ak.stock_zh_index_daily(akshare_code)
                df['trade_date'] = pd.to_datetime(df['date'])
                return df[['trade_date', 'open', 'high', 'low', 'close', 'volume', 'amount']]
        except Exception as e:
            print(f"akshare index_daily failed: {e}")

        return pd.DataFrame()

    def resample_index_to_weekly(self, df_daily):
        """
        Resample index daily data to weekly bars (Friday ending).
        参照个股分析中的 resample_daily_to_weekly 实现。
        """
        if df_daily.empty:
            return pd.DataFrame()

        df = df_daily.copy()

        # 重命名列以统一格式
        if 'vol' in df.columns and 'volume' not in df.columns:
            df = df.rename(columns={'vol': 'volume'})

        # 设置trade_date为索引（如果还没有设置）
        if 'trade_date' in df.columns:
            if not isinstance(df.index, pd.DatetimeIndex):
                df = df.set_index('trade_date')
        elif 'date' in df.columns:
            if not isinstance(df.index, pd.DatetimeIndex):
                df = df.set_index('date')

        # 确保索引是DatetimeIndex
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)

        # 周线聚合
        weekly = df.resample('W-FRI', closed='right', label='right').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
        })

        # 删除NaN行但保留DatetimeIndex
        weekly = weekly.dropna()

        # 重置索引，将DatetimeIndex变回列，但保留'date'作为普通列用于API返回
        weekly = weekly.reset_index()
        weekly = weekly.rename(columns={'trade_date': 'date'})

        return weekly

    def fetch_stock_basic(self, ts_code, start_date, end_date):
        """Fetch stock daily basic (PE, PB, PS, DivYield)."""
        return self.pro.daily_basic(ts_code=ts_code, start_date=start_date, end_date=end_date)

    def _adjust_prices(self, df, df_adj, adj_type):
        """
        Apply price adjustment (forward or backward).

        Args:
            df: DataFrame with OHLCV data and 'trade_date'
            df_adj: DataFrame with adj_factor data
            adj_type: 'qfq' for forward-adjusted (前复权),
                      'hfq' for backward-adjusted (后复权)

        Returns:
            DataFrame with adjusted OHLCV columns
        """
        import logging
        logger = logging.getLogger(__name__)

        # Make a copy
        df = df.copy()

        # Merge adjustment factor
        df = df.merge(df_adj, on='trade_date', how='left')

        # Handle NaN adj_factor
        nan_count = df['adj_factor'].isna().sum()
        if nan_count > 0:
            logger.warning(f"Found {nan_count} rows with NaN adj_factor, using ffill/bfill")
            df['adj_factor'] = df['adj_factor'].ffill().bfill()

        # Handle zero or negative adj_factor
        zero_count = (df['adj_factor'] <= 0).sum()
        if zero_count > 0:
            logger.warning(f"Found {zero_count} rows with zero or negative adj_factor")
            df.loc[df['adj_factor'] <= 0, 'adj_factor'] = 1.0

        if adj_type == 'qfq':
            # 前复权: 复权后价格 = 原始价格 × 当日复权因子 / 最新复权因子
            latest_adj_factor = df['adj_factor'].iloc[-1]
            adj_ratio = df['adj_factor'] / latest_adj_factor
        elif adj_type == 'hfq':
            # 后复权: 复权后价格 = 原始价格 × 当日复权因子
            adj_ratio = df['adj_factor']
        else:
            raise ValueError(f"Invalid adj_type: {adj_type}")

        # Apply adjustment to OHLC columns
        price_cols = ['open', 'high', 'low', 'close', 'pre_close']
        for col in price_cols:
            df[col] = df[col] * adj_ratio

        # Drop adj_factor column
        df = df.drop(columns=['adj_factor'])

        return df

    def fetch_daily_bars(self, ts_code, start_date, end_date):
        """
        Fetch stock daily OHLC data without adjustment.

        Args:
            ts_code: Stock code (e.g., '000001.SZ')
            start_date: Start date (YYYYMMDD)
            end_date: End date (YYYYMMDD)

        Returns:
            DataFrame with OHLCV columns
        """
        df_daily = self.pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        if 'trade_date' in df_daily.columns:
            df_daily['trade_date'] = pd.to_datetime(df_daily['trade_date'])
        return df_daily.sort_values('trade_date')
    
    def fetch_adjusted_daily_bars(self, ts_code, start_date, end_date, adj='qfq'):
        """
        Fetch stock daily OHLC data with adjustment.

        Args:
            ts_code: Stock code (e.g., '000001.SZ')
            start_date: Start date (YYYYMMDD)
            end_date: End date (YYYYMMDD)
            adj: Adjustment type - 'qfq' for forward-adjusted (前复权, default),
                 'hfq' for backward-adjusted (后复权), None for raw data

        Returns:
            DataFrame with OHLCV columns (adjusted if adj is specified)
        """
        # Fetch raw daily data
        df_daily = self.fetch_daily_bars(ts_code=ts_code, start_date=start_date, end_date=end_date)
        if df_daily.empty:
            return df_daily

        # If no adjustment needed, return raw data
        if adj is None:
            return df_daily

        # Fetch adjustment factor using the existing method
        df_adj = self.fetch_adj_factor(ts_code, start_date, end_date)
        if df_adj.empty:
            print(f"Warning: No adj_factor data for {ts_code}, returning raw data")
            return df_daily
        
        # Apply adjustment
        df = self._adjust_prices(df_daily, df_adj, adj)

        return df

    def fetch_adj_factor(self, ts_code, start_date, end_date):
        """
        Fetch adjustment factor for a stock.
        """
        df = self.pro.adj_factor(ts_code=ts_code, start_date=start_date, end_date=end_date)
        if 'trade_date' in df.columns:
            df['trade_date'] = pd.to_datetime(df['trade_date'])
        return df.sort_values('trade_date')
    
    def resample_daily_to_weekly(self, df_daily):
        """
        Resample daily OHLCV data to weekly bars using pandas resample.
        IMPORTANT: Assumes input daily data is already adjusted.

        Args:
            df_daily: DataFrame with 'trade_date', 'open', 'high', 'low', 'close', 'volume', 'pre_close'

        Returns:
            DataFrame with weekly OHLCV columns (date = Friday of each week)
        """
        # Make a copy and set trade_date as index
        df = df_daily.copy()
        df = df.set_index('trade_date')

        # Resample to weekly bars, anchored to Friday
        # closed='right': week ends on Friday
        # label='right': label is Friday
        weekly = df.resample('W-FRI', closed='right', label='right').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
            'pre_close': 'first'
        })

        # Reset index to make date a column
        weekly = weekly.reset_index()
        weekly = weekly.rename(columns={'trade_date': 'date'})

        # Calculate pre_close as previous week's close
        weekly['pre_close'] = weekly['close'].shift(1)

        # Fill NaN pre_close for the first week
        weekly.loc[weekly['pre_close'].isna(), 'pre_close'] = weekly.loc[weekly['pre_close'].isna(), 'close']

        return weekly

    def fetch_adjusted_weekly_bars(self, ts_code, start_date, end_date, adj='qfq'):
        """
        Fetch stock weekly OHLC data with adjustment.
        Resamples from daily data to avoid Tushare weekly data issues.

        IMPORTANT: To ensure accurate weekly data, the last incomplete week
        (where the week has not closed on Friday) is excluded from the result.

        Args:
            ts_code: Stock code (e.g., '000001.SZ')
            start_date: Start date (YYYYMMDD)
            end_date: End date (YYYYMMDD)
            adj: Adjustment type - 'qfq' for forward-adjusted (前复权, default),
                 'hfq' for backward-adjusted (后复权), None for raw data

        Returns:
            DataFrame with OHLCV columns (adjusted if adj is specified)
        """
        import logging
        logger = logging.getLogger(__name__)

        # Fetch daily bars
        df_daily = self.fetch_adjusted_daily_bars(ts_code, start_date, end_date, adj=adj)
        
        # Filter out the last incomplete week
        # If the last day is not Friday, exclude it from weekly resampling
        df_daily = df_daily.copy()
        # Rename 'vol' to 'volume' if needed
        if 'vol' in df_daily.columns and 'volume' not in df_daily.columns:
            df_daily = df_daily.rename(columns={'vol': 'volume'})

        # if not df_daily.empty:
        #     last_date = df_daily['trade_date'].iloc[-1]
        #     # If last day is not Friday (weekday 4), exclude it
        #     if last_date.weekday() != 4:
        #         logger.debug(f"Excluding incomplete week data (last date: {last_date.strftime('%Y-%m-%d')}, weekday: {last_date.weekday()})")
        #         df_daily = df_daily[df_daily['trade_date'] <= last_date]

        # Resample adjusted daily data to weekly
        df_weekly = self.resample_daily_to_weekly(df_daily)

        logger.debug(f"[{ts_code}] Weekly data from daily resampling:\n{df_weekly}")

        return df_weekly

    def fetch_daily_market(self, trade_date):
        """
        Fetch daily basic data for ALL stocks on a specific trade_date.
        Used for market-wide screening.
        """
        # fields: ts_code, name (not in daily_basic usually, need stock_basic), pe, pb... 
        # Actually daily_basic doesn't have name. We might need fetch_stock_list too.
        return self.pro.daily_basic(trade_date=trade_date)

    def fetch_financial_snapshot(self, period):
        """
        Fetch financial indicators for ALL stocks for a specific reporting period (e.g. '20231231').
        """
        return self.pro.fina_indicator(period=period)
        
    def fetch_financial_indicator(self, ts_code, start_date, end_date):
        """Fetch financial indicators (ROE, etc.)."""
        fields = (
            "ts_code,ann_date,end_date,roa,roe,roe_yearly,q_roe,"
            "grossprofit_margin,q_gsprofit_margin,assets_turn,current_ratio,"
            "debt_to_assets,netprofit_margin,q_opincome,q_dtprofit"
        )
        return self.pro.fina_indicator(
            ts_code=ts_code, start_date=start_date, end_date=end_date, fields=fields
        )
    
    def fetch_stock_list(self):
        """
        Fetch basic stock list (code, name, industry).
        """
        return self.pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')

    def fetch_cashflow(self, ts_code, start_date, end_date):
        """Fetch cashflow data."""
        return self.pro.cashflow(ts_code=ts_code, start_date=start_date, end_date=end_date)

    def fetch_balancesheet(self, ts_code, start_date, end_date):
        """Fetch balancesheet data."""
        return self.pro.balancesheet(ts_code=ts_code, start_date=start_date, end_date=end_date)

    def fetch_income(self, ts_code, start_date, end_date):
        """Fetch income statement data."""
        return self.pro.income(ts_code=ts_code, start_date=start_date, end_date=end_date)

    def fetch_industry_levels(self, ts_code):
        """
        Fetch industry classification levels (L1, L2, L3) for a stock.
        Returns a DataFrame with l1_name, l2_name, l3_name, l1_code, etc.
        """
        return self.pro.index_member_all(ts_code=ts_code)

    def fetch_sw_daily(self, ts_code, start_date, end_date):
        """
        Fetch Shenwan Industry Index Daily Metrics (PE, PB).
        ts_code: Industry Code (e.g. from l1_code)
        """
        return self.pro.sw_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)

    def fetch_local_index_valuation(self, index_name):
        """
        Fetch index valuation (PE TTM) from local Excel file data/IndexEvaluationData.xlsx.
        Returns DataFrame with 'trade_date' and 'pe_ttm'.
        """
        file_path = os.path.join(os.getcwd(), 'data', 'IndexEvaluationData.xlsx')
        if not os.path.exists(file_path):
            print(f"Local valuation file not found: {file_path}")
            return pd.DataFrame()

        try:
            # Read first 2 rows to verify structure (optional optimization, but we just read all)
            # Based on inspection:
            # Col 0: Date
            # Col 1: S&P 500 PE
            # Col 2: Nasdaq 100 PE
            # Col 3: Hang Seng PE
            
            use_cols = [0]
            col_map = {
                '标普500': 1,
                '纳斯达克100': 2,
                '恒生指数': 3
            }
            
            target_col_idx = col_map.get(index_name)
            if target_col_idx is None:
                return pd.DataFrame()
                
            use_cols.append(target_col_idx)
            
            # Read only necessary columns
            # header=1 means Row 1 (0-indexed) is used as header? No, row 1 contains names.
            # We skip row 0 (title) and use row 1 as header, effectively.
            # Actually, standard read with header=1 might be best.
            
            df = pd.read_excel(file_path, header=1, usecols=use_cols)
            
            # The columns will be named by Row 1 content.
            # Col 0 name might be 'Unnamed: 0' or similar if Row 1 was empty there?
            # In output 27, Col 0 Row 1 was 'nan'.
            # So the date column will be named 'Unnamed: 0'.
            # The value column will be named '标普500' etc.
            
            # Let's rename columns
            df.columns = ['trade_date', 'pe_ttm']
            
            # Convert types
            df['trade_date'] = pd.to_datetime(df['trade_date'], errors='coerce')
            df['pe_ttm'] = pd.to_numeric(df['pe_ttm'], errors='coerce')
            
            df = df.dropna(subset=['trade_date', 'pe_ttm'])
            return df.sort_values('trade_date')
            
        except Exception as e:
            print(f"Error reading local valuation file: {e}")
            return pd.DataFrame()


class AkshareFetcher(DataFetcher):
    def fetch_us_gdp(self):
        """Fetch US GDP monthly."""
        return ak.macro_usa_gdp_monthly()

    def fetch_us_cpi(self):
        """Fetch US CPI YoY."""
        return ak.macro_usa_cpi_yoy()

class YahooFetcher(DataFetcher):

    def fetch_price(self, ticker, start_date, end_date):
        """Fetch US/HK stock prices."""
        return yf.download(ticker, start=start_date, end=end_date)

class AlphaVantageFetcher(DataFetcher):
    def __init__(self, config_path="config/config.json"):
        super().__init__(config_path)
        load_dotenv()
        self.api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
        if not self.api_key:
            # Try config
            self.api_key = self.config.get("alpha_vantage_key")

        if not self.api_key:
            print("Warning: Alpha Vantage API Key not found.")

        # Rate limiting: track last request time (AlphaVantage free tier: 1 request/second)
        self._last_request_time = None
        self._min_request_interval = 1.1  # seconds (slightly more than 1 second for safety)

    def _rate_limit(self):
        """
        Enforce rate limiting to avoid AlphaVantage API errors.
        """
        import time
        if self._last_request_time is not None:
            elapsed = time.time() - self._last_request_time
            if elapsed < self._min_request_interval:
                sleep_time = self._min_request_interval - elapsed
                print(f"AlphaVantage rate limiting: sleeping {sleep_time:.2f}s...")
                time.sleep(sleep_time)
        self._last_request_time = time.time()

    def _get_data_with_cache(self, url, file_name):
        """
        Helper to fetch data from API or local cache.
        """
        data_dir = os.path.join(os.getcwd(), 'data')
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
        
        file_path = os.path.join(data_dir, file_name)
        
        # Check if local file exists and is from today
        if os.path.exists(file_path):
            mtime = os.path.getmtime(file_path)
            file_date = datetime.fromtimestamp(mtime).date()
            if file_date == datetime.now().date():
                try:
                    with open(file_path, 'r') as f:
                        print(f"Loading {file_name} from local cache")
                        return json.load(f)
                except Exception as e:
                    print(f"Error reading local cache {file_name}: {e}")

        # Fetch from API
        try:
            # Rate limiting to avoid AlphaVantage API errors
            self._rate_limit()
            print(f"Fetching {file_name} from AlphaVantage API")
            r = requests.get(url)
            data = r.json()
            
            if 'data' in data:
                try:
                    with open(file_path, 'w') as f:
                        json.dump(data, f)
                except Exception as e:
                    print(f"Error saving {file_name}: {e}")
                return data
            else:
                print(f"Error fetching {file_name}: {data}")
                return data
        except Exception as e:
            print(f"Exception fetching {file_name}: {e}")
            return {}

    def fetch_us_gdp(self):
        """
        Fetch US Real GDP (Quarterly).
        """
        if not self.api_key:
            return pd.DataFrame()
            
        url = f'https://www.alphavantage.co/query?function=REAL_GDP&interval=quarterly&apikey={self.api_key}'
        
        data = self._get_data_with_cache(url, 'us_gdp.json')
        
        if 'data' in data:
            df = pd.DataFrame(data['data'])
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date')
            return df
        else:
            return pd.DataFrame()

    def fetch_us_cpi(self):
        """
        Fetch US CPI (Monthly).
        """
        if not self.api_key:
            return pd.DataFrame()

        url = f'https://www.alphavantage.co/query?function=CPI&interval=monthly&datatype=json&apikey={self.api_key}'
        
        data = self._get_data_with_cache(url, 'us_cpi.json')
        
        if 'data' in data:
            df = pd.DataFrame(data['data'])
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date')
            return df
        else:
            return pd.DataFrame()
