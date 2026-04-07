"""
Shared utilities for stock data services.

Contains helper functions, configuration constants, and indicator calculation
logic used by both Tushare and FDH service implementations.
"""

import math
import datetime
import pandas as pd
import numpy as np
from typing import Optional, Tuple, Dict, List, Any

# ---------- Configuration ----------

VALUATION_CONFIG = {
    "percentile_window_years": 5,
    "data_fetch_window_years": 10,
    "min_data_points": 50,
}

RSI_CONFIG = {
    "oversold_threshold": 30,
}


# ---------- Data Helpers ----------

def safe_float(value) -> Optional[float]:
    """Convert value to safe float, handling NaN and None."""
    if value is None or pd.isna(value) or (isinstance(value, float) and (value != value)):
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def safe_json_friendly(obj):
    """Recursively convert NaN/inf to None for JSON serialization."""
    if isinstance(obj, dict):
        return {k: safe_json_friendly(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [safe_json_friendly(v) for v in obj]
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    else:
        return obj


def ensure_ts_code(code: str) -> str:
    """Ensure stock code has market suffix (.SH / .SZ)."""
    if code.endswith('.SZ') or code.endswith('.SH'):
        return code
    if code.startswith('000') or code.startswith('002') or code.startswith('300'):
        return code + '.SZ'
    return code + '.SH'


# ---------- Technical Indicators ----------

def resample_to_weekly(df: pd.DataFrame) -> pd.DataFrame:
    """Resample daily data to weekly (Friday ending)."""
    if df.empty:
        return df
    logic = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'}
    if 'vol' in df.columns:
        logic['vol'] = 'sum'
    return df.resample('W-FRI').agg(logic).dropna()


def calculate_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate technical indicators for weekly data.
    - MA20, MA50 for trend
    - RSI (14) for overbought/oversold
    - MACD (12, 26, 9) for momentum
    - RSI Divergence detection
    - Enhanced Entry Signal (Valuation + Technical)
    """
    df = df.copy()

    # Convert Decimal values to float to avoid type errors with pandas operations
    from decimal import Decimal
    for col in df.columns:
        if df[col].dtype == object:
            # Check if any values are Decimal
            has_decimal = df[col].apply(lambda x: isinstance(x, Decimal)).any()
            if has_decimal:
                df[col] = df[col].apply(lambda x: float(x) if isinstance(x, Decimal) else x)
            # Convert None to NaN for numeric operations
            df[col] = df[col].replace({None: np.nan})

    if 'close' not in df.columns:
        return df

    # MA
    df['ma20'] = df['close'].rolling(window=20, min_periods=1).mean()
    df['ma50'] = df['close'].rolling(window=50, min_periods=1).mean()
    # 新增MA用于分级止损（出场信号）
    df['ma100'] = df['close'].rolling(window=100, min_periods=1).mean()
    df['ma120'] = df['close'].rolling(window=120, min_periods=1).mean()

    # Volume MA
    if 'vol' in df.columns:
        df['vol_ma8'] = df['vol'].rolling(window=8, min_periods=1).mean()

    # RSI (14)
    if 'close' in df.columns:
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).ewm(alpha=1 / 14, adjust=False).mean()
        loss = (-delta.where(delta < 0, 0)).ewm(alpha=1 / 14, adjust=False).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))

    # MACD (12, 26, 9)
    if 'close' in df.columns:
        ema12 = df['close'].ewm(span=12, adjust=False).mean()
        ema26 = df['close'].ewm(span=26, adjust=False).mean()
        df['dif'] = ema12 - ema26
        df['dea'] = df['dif'].ewm(span=9, adjust=False).mean()
        df['macd'] = (df['dif'] - df['dea']) * 2

    # ---- Trading Signals ----
    _calculate_trading_signals(df)

    return df


def calculate_signals_from_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate trading signals from pre-computed indicator columns.

    Expects df to already have: close, ma20, ma50, rsi, dif (macd_dif), dea (macd_dea),
    macd (macd_hist). Used by FDH mode where base indicators come from DB.

    If volume is present, calculates vol_ma8.
    """
    df = df.copy()

    # Convert Decimal values to float to avoid type errors with pandas operations
    # Database returns Decimal for numeric columns, which can cause issues with None
    from decimal import Decimal
    for col in df.columns:
        if df[col].dtype == object:
            # Check if any values are Decimal
            has_decimal = df[col].apply(lambda x: isinstance(x, Decimal)).any()
            if has_decimal:
                df[col] = df[col].apply(lambda x: float(x) if isinstance(x, Decimal) else x)
            # Convert None to NaN for numeric operations
            df[col] = df[col].replace({None: np.nan})

    # Volume MA (if volume present)
    vol_col = None
    for c in ['vol', 'volume']:
        if c in df.columns:
            vol_col = c
            break
    if vol_col:
        df['vol_ma8'] = df[vol_col].rolling(window=8, min_periods=1).mean()

    # Rename fdh columns to match signal code expectations if needed
    col_map = {}
    if 'ma_20' in df.columns and 'ma20' not in df.columns:
        col_map['ma_20'] = 'ma20'
    if 'ma_50' in df.columns and 'ma50' not in df.columns:
        col_map['ma_50'] = 'ma50'
    if 'rsi_14' in df.columns and 'rsi' not in df.columns:
        col_map['rsi_14'] = 'rsi'
    if 'macd_dif' in df.columns and 'dif' not in df.columns:
        col_map['macd_dif'] = 'dif'
    if 'macd_dea' in df.columns and 'dea' not in df.columns:
        col_map['macd_dea'] = 'dea'
    if 'macd_hist' in df.columns and 'macd' not in df.columns:
        col_map['macd_hist'] = 'macd'
    if col_map:
        df = df.rename(columns=col_map)

    # 确保MA100和MA120存在（用于分级止损），如果数据库没有则计算
    if 'ma100' not in df.columns and 'close' in df.columns:
        df['ma100'] = df['close'].rolling(window=100, min_periods=1).mean()
    if 'ma120' not in df.columns and 'close' in df.columns:
        df['ma120'] = df['close'].rolling(window=120, min_periods=1).mean()

    _calculate_trading_signals(df)
    return df


def _calculate_trading_signals(df: pd.DataFrame) -> None:
    """
    Internal: attach signal columns to df (mutates in place).

    Expects: close, ma50, dif, dea, rsi, low columns.

    New features:
    - Signal strength scoring (0-100) for granular buy signal quality
    - Signal decay mechanism to avoid chasing old signals
    - Enhanced divergence detection with strength classification
    - Multi-timeframe trend confirmation
    """
    oversold_thresh = RSI_CONFIG.get("oversold_threshold", 30)

    # ============================================
    # 1. MA50 Trend Analysis (Enhanced)
    # ============================================
    # Multi-timeframe trend confirmation
    ma50_slope_5 = df['ma50'].diff(5)
    ma50_slope_10 = df['ma50'].diff(10)
    ma50_slope_20 = df['ma50'].diff(20)

    # Require consistent trend across timeframes (at least 2 out of 3)
    trend_consistency = (
        (ma50_slope_5 > 0).astype(int) +
        (ma50_slope_10 > 0).astype(int) +
        (ma50_slope_20 > 0).astype(int)
    )
    df['is_ma_trend_up'] = trend_consistency >= 2

    # MA trend strength score (0-15)
    df['ma_trend_score'] = trend_consistency * 5

    # Price position relative to MA50 (0-15)
    # 价值投资视角：刚突破MA50时风险收益比最优，远离MA50反而追高风险大
    price_ma_ratio = df['close'] / df['ma50']
    df['ma_position_score'] = np.where(
        price_ma_ratio > 1.15, 3,    # 远离MA50(>15%), 追高风险大
        np.where(price_ma_ratio > 1.10, 6,   # 明显偏高(10-15%)
        np.where(price_ma_ratio > 1.05, 10,  # 稳健站上(5-10%)
        np.where(price_ma_ratio > 1.02, 15,  # 刚突破(2-5%), 最佳风险收益比
        np.where(price_ma_ratio > 1.00, 12,  # 刚站上(0-2%)
        0)))))

    # ============================================
    # 2. MACD Analysis with Freshness Scoring
    # ============================================
    if 'dif' in df.columns and 'dea' in df.columns:
        gc_event = (df['dif'] > df['dea']) & (df['dif'].shift(1) <= df['dea'].shift(1))
        dc_event = (df['dif'] < df['dea']) & (df['dif'].shift(1) >= df['dea'].shift(1))
        valid_gc_event = gc_event & (df['dif'] < 0)

        events = pd.Series(0, index=df.index)
        events[valid_gc_event] = 1
        events[dc_event] = -1
        state_signal = events.replace(0, np.nan).ffill().fillna(0)
        df['is_macd_gold'] = state_signal == 1

        # MACD golden cross freshness scoring (0-20)
        # Calculate weeks since golden cross
        gc_points = df.index[valid_gc_event]
        weeks_since_gc = pd.Series(index=df.index, dtype=float)
        for i, idx in enumerate(df.index):
            past_gcs = [g for g in gc_points if g <= idx]
            if past_gcs:
                last_gc = max(past_gcs)
                weeks_since_gc.iloc[i] = (idx - last_gc).days / 7 if hasattr(idx, 'days') else i - df.index.get_loc(last_gc)
            else:
                weeks_since_gc.iloc[i] = 999

        df['macd_freshness_score'] = np.where(
            weeks_since_gc <= 2, 20,
            np.where(weeks_since_gc <= 4, 15,
            np.where(weeks_since_gc <= 8, 10,
            np.where(weeks_since_gc <= 12, 5, 0)))
        )

        # DIF position score (0-10)
        df['macd_position_score'] = np.where(
            df['dif'] < -0.5, 10,
            np.where(df['dif'] < 0, 8,
            np.where(df['dif'] < 0.5, 5, 2))
        )

        # DIF momentum (0-5)
        dif_slope = df['dif'].diff(3)
        df['macd_momentum_score'] = np.where(dif_slope > 0, 5, 0)

        df['macd_score'] = df['macd_freshness_score'] + df['macd_position_score'] + df['macd_momentum_score']
    else:
        df['is_macd_gold'] = False
        df['macd_score'] = 0

    # ============================================
    # 3. RSI Signals with Enhanced Divergence
    # ============================================
    df['rsi_oversold_detected'] = False
    df['rsi_neutral_breakup_detected'] = False
    df['rsi_div_detected'] = False
    df['rsi_div_strength'] = None

    if 'rsi' in df.columns:
        rsi = df['rsi']
        df.loc[rsi < oversold_thresh, 'rsi_oversold_detected'] = True

        rsi_above_40 = rsi > 40
        rsi_above_50 = rsi > 50
        rsi_was_below_40 = rsi.shift(1) <= 40
        rsi_was_below_50 = rsi.shift(1) <= 50
        df['rsi_neutral_breakup_detected'] = (
            ((rsi_above_40 & rsi_was_below_40) | (rsi_above_50 & rsi_was_below_50))
        ) & (rsi < 60)

        # Enhanced RSI divergence detection
        if 'low' in df.columns:
            # Find local minima using rolling window - separate for price and RSI
            # Use larger window (20) to capture more significant lows
            roll_window = 20
            df['price_min'] = df['low'].rolling(window=roll_window, center=True, min_periods=5).min()
            df['rsi_min'] = df['rsi'].rolling(window=roll_window, center=True, min_periods=5).min()

            # Find price pivot points (local lows)
            is_price_pivot = (df['low'] == df['price_min']) & (df['price_min'].notna())
            price_pivot_indices = df.index[is_price_pivot]

            if len(price_pivot_indices) >= 2:
                for i in range(1, len(price_pivot_indices)):
                    curr_idx = price_pivot_indices[i]
                    prev_idx = price_pivot_indices[i - 1]
                    curr_loc = df.index.get_loc(curr_idx)
                    prev_loc = df.index.get_loc(prev_idx)

                    # Time span check (5-50 periods)
                    if curr_loc - prev_loc < 5 or curr_loc - prev_loc > 50:
                        continue

                    p_curr = df.loc[curr_idx, 'low']
                    p_prev = df.loc[prev_idx, 'low']

                    # Price must make a new low (>2%, lowered from 3%)
                    price_drop = (p_prev - p_curr) / p_prev if p_prev > 0 else 0
                    if price_drop < 0.02:
                        continue

                    # Find the corresponding RSI values at/near these price pivots
                    # Look for RSI low within ±3 bars of each price pivot
                    r_curr_window_start = max(0, curr_loc - 3)
                    r_curr_window_end = min(len(df) - 1, curr_loc + 3)
                    r_prev_window_start = max(0, prev_loc - 3)
                    r_prev_window_end = min(len(df) - 1, prev_loc + 3)

                    r_curr = df.iloc[r_curr_window_start:r_curr_window_end + 1]['rsi'].min()
                    r_prev = df.iloc[r_prev_window_start:r_prev_window_end + 1]['rsi'].min()

                    # RSI must be higher (at least 1.5 points, lowered from 2)
                    rsi_lift = r_curr - r_prev
                    if rsi_lift < 1.5:
                        continue

                    # Determine which index to mark (use the current price pivot)
                    mark_idx = curr_idx

                    # Classify divergence strength
                    if price_drop > 0.10 and rsi_lift > 8:
                        df.at[mark_idx, 'rsi_div_strength'] = 'strong'
                    elif price_drop > 0.05 and rsi_lift > 4:
                        df.at[mark_idx, 'rsi_div_strength'] = 'medium'
                    elif price_drop > 0.02 and rsi_lift > 1.5:
                        df.at[mark_idx, 'rsi_div_strength'] = 'weak'
                    else:
                        continue

                    df.at[mark_idx, 'rsi_div_detected'] = True

        # RSI oversold score (0-15)
        df['rsi_oversold_score'] = np.where(
            rsi < 20, 15,
            np.where(rsi < 30, 12,
            np.where(rsi < 40, 8, 0))
        )

        # RSI divergence score (0-15)
        df['rsi_div_score'] = np.where(
            df['rsi_div_strength'] == 'strong', 15,
            np.where(df['rsi_div_strength'] == 'medium', 10,
            np.where(df['rsi_div_strength'] == 'weak', 5, 0))
        )

        # RSI neutral breakup score (0-5)
        bu_points = df.index[df['rsi_neutral_breakup_detected']]
        weeks_since_bu = pd.Series(index=df.index, dtype=float)
        for i, idx in enumerate(df.index):
            past_bus = [b for b in bu_points if b <= idx]
            if past_bus:
                last_bu = max(past_bus)
                weeks_since_bu.iloc[i] = (idx - last_bu).days / 7 if hasattr(idx, 'days') else i - df.index.get_loc(last_bu)
            else:
                weeks_since_bu.iloc[i] = 999

        df['rsi_breakup_score'] = np.where(
            weeks_since_bu <= 2, 5,
            np.where(weeks_since_bu <= 4, 3, 0)
        )

        # RSI total score (take max of components)
        df['rsi_score'] = np.maximum.reduce([
            df['rsi_oversold_score'],
            df['rsi_div_score'],
            df['rsi_breakup_score']
        ])

    # Signal persistence
    df['is_rsi_oversold_active'] = df['rsi_oversold_detected'].rolling(window=20, min_periods=1).max() > 0
    df['is_rsi_neutral_breakup_active'] = df['rsi_neutral_breakup_detected'].rolling(window=15, min_periods=1).max() > 0
    df['is_rsi_div_active'] = df['rsi_div_detected'].rolling(window=20, min_periods=1).max() > 0

    df['is_rsi_favorable'] = df['is_rsi_oversold_active'] | df['is_rsi_neutral_breakup_active'] | df['is_rsi_div_active']

    # ============================================
    # 3.5 Volume Confirmation: Net Distribution/Accumulation (NDA)
    # 净派发/吸筹成本：20期窗口内，成交量最大的top 25%中涨跌日之差
    # 范围 [-5, +5]，方向比规模更重要
    # ============================================
    df['nda_value'] = 0
    df['nda_score'] = 0
    df['volume_confirmed'] = False

    vol_col = None
    for _vc in ['vol', 'volume']:
        if _vc in df.columns:
            vol_col = _vc
            break

    if vol_col and not df[vol_col].isna().all():
        NDA_WINDOW = 20
        top_count = max(1, int(NDA_WINDOW * 0.25))  # 5 days for 20-period window

        for _i in range(NDA_WINDOW - 1, len(df)):
            _window = df.iloc[_i - NDA_WINDOW + 1:_i + 1]
            # Sort by volume descending and take top 25%
            _top_idx = _window[vol_col].nlargest(top_count).index
            # Count up days (close > open) vs down days in those high-volume bars
            _up = sum(1 for _idx in _top_idx
                      if df.loc[_idx, 'close'] > df.loc[_idx, 'open'])
            _nda = _up - (top_count - _up)  # up_days - down_days

            df.iat[_i, df.columns.get_loc('nda_value')] = _nda
            df.iat[_i, df.columns.get_loc('volume_confirmed')] = _nda >= 1

            # NDA score: >=+3 极强多头; >=+1 偏多头; 0 中性; <0 空头派发
            if _nda >= 3:
                df.iat[_i, df.columns.get_loc('nda_score')] = 15
            elif _nda >= 1:
                df.iat[_i, df.columns.get_loc('nda_score')] = 10
            elif _nda == 0:
                df.iat[_i, df.columns.get_loc('nda_score')] = 5
            else:
                df.iat[_i, df.columns.get_loc('nda_score')] = 0

    # ============================================
    # 4. Combined Signal Strength Score (0-90)
    # 总分 = MA趋势(0-15) + MA位置(0-15) + max(MACD(0-35), RSI(0-35))
    #       + 共振加分(0-10) + NDA成交量(0-15)
    # ============================================
    macd_s = df.get('macd_score', pd.Series(0, index=df.index))
    rsi_s = df.get('rsi_score', pd.Series(0, index=df.index))

    # 共振加分：MACD金叉状态 与 RSI有利状态 同时成立时额外+10分
    resonance_bonus = np.where(
        df['is_macd_gold'] & df['is_rsi_favorable'], 10, 0
    )
    df['resonance_active'] = df['is_macd_gold'] & df['is_rsi_favorable']

    nda_s = df.get('nda_score', pd.Series(0, index=df.index))

    df['signal_strength'] = (
        df['ma_trend_score'] + df['ma_position_score'] +
        np.maximum(macd_s, rsi_s) +
        resonance_bonus +
        nda_s
    )

    # Signal grade classification
    conditions = [
        df['signal_strength'] >= 80,
        df['signal_strength'] >= 65,
        df['signal_strength'] >= 50,
        df['signal_strength'] >= 35,
    ]
    choices = ['A', 'B', 'C', 'D']
    df['signal_grade'] = np.select(conditions, choices, default='F')

    # ============================================
    # 5. Signal Decay Mechanism
    # ============================================
    # Calculate signal decay based on time since trigger
    signal_points = df.index[df['signal_strength'] >= 65].tolist()
    decay_scores = []

    for i, idx in enumerate(df.index):
        past_signals = [s for s in signal_points if s <= idx]
        if not past_signals:
            decay_scores.append(0)
            continue

        last_signal = max(past_signals)
        weeks_since = i - df.index.get_loc(last_signal)

        # Decay curve: 100% -> 50% -> 20% -> 0%
        if weeks_since <= 4:
            decay = 1.0
        elif weeks_since <= 8:
            decay = 0.5 + (8 - weeks_since) * 0.125
        elif weeks_since <= 12:
            decay = 0.2 + (12 - weeks_since) * 0.075
        else:
            decay = max(0, 0.2 - (weeks_since - 12) * 0.05)

        decay_scores.append(decay)

    df['signal_decay'] = decay_scores
    df['signal_effective_strength'] = df['signal_strength'] * df['signal_decay']

    # ============================================
    # 6. Final Signal Definitions (Backward Compatible)
    # ============================================
    # Active signal state (continuous, not edge-triggered)
    # 分层条件：满足其一即可
    # 层级1: 水下MACD金叉（最可靠的多头反转，DIF<0时金叉）
    _cond_underwater_gc = df['is_macd_gold'] & (df['dif'] < 0)
    # 层级2: MACD金叉状态 + RSI有利 共振（双重确认）
    _cond_resonance = df['is_macd_gold'] & df['is_rsi_favorable']
    # 层级3: RSI有利 + 底背离（无需MACD金叉，但需背离支撑）
    _cond_rsi_div = df['is_rsi_favorable'] & df['is_rsi_div_active']

    df['signal_active'] = (
        (df['close'] > df['ma50']) &
        df['is_ma_trend_up'] &
        (_cond_underwater_gc | _cond_resonance | _cond_rsi_div)
    )

    # Fresh signal (within 4 weeks of B-grade or above)
    df['signal_fresh'] = df['signal_active'] & (df['signal_decay'] >= 0.5)

    # Original edge-triggered signal (for chart marking)
    df['signal_entry'] = (
        (df['close'] > df['ma50']) &
        df['is_ma_trend_up'] &
        (df['is_macd_gold'] | df['is_rsi_favorable']) &
        (~(
            (df['close'] > df['ma50']).shift(1) &
            df['is_ma_trend_up'].shift(1) &
            (df['is_macd_gold'].shift(1) | df['is_rsi_favorable'].shift(1))
        ))
    )

    # Strong signal (A-grade)
    df['signal_entry_strong'] = df['signal_strength'] >= 80

    # Ensure NDA/volume fields exist even if not calculated (no volume data)
    for _col in ['nda_value', 'nda_score', 'volume_confirmed', 'resonance_active']:
        if _col not in df.columns:
            df[_col] = False if _col in ['volume_confirmed', 'resonance_active'] else 0


# ---------- Exit Signal Calculation ----------

def calculate_exit_signals_for_entry(
    df: pd.DataFrame,
    entry_idx: int,
    entry_price: float,
    f_score_data: Optional[Dict] = None,
    industry_val_data: Optional[Dict] = None,
    entry_type: str = 'Buy',
    continue_after_exit: bool = False,
) -> Dict[str, Any]:
    """
    Calculate exit signals (stop-loss, take-profit, add-position) for a specific entry point.

    Args:
        df: DataFrame with price data and indicators (must have ma50, ma100, ma120)
        entry_idx: Index of the entry point (iloc position)
        entry_price: Price at entry (typically the next period's open)
        f_score_data: Optional F-Score history for fundamental filtering
        industry_val_data: Optional industry valuation data with core_indicator info
        entry_type: Entry signal type - 'Buy', 'StrongBuy', or 'ReverseBuy'
        continue_after_exit: Continue updating trailing stop after first exit trigger

    Returns:
        Dict with stop_loss_line, take_profit_line, add_signals, etc.
    """
    n = len(df)

    # Initialize output arrays
    stop_loss_line = [None] * n
    stop_loss_type = [None] * n  # 'initial', 'break_even', 'protect', 'lock'
    take_profit_line = [None] * n
    take_profit_type = [None] * n  # 'valuation_target', 'percentile_target'
    add_position_signals = [False] * n
    add_position_ratios = [None] * n
    reduce_position_signals = [False] * n  # 减仓信号

    # Track if stop-loss or take-profit has been triggered
    stop_triggered = False
    profit_triggered = False
    exit_idx = None  # 记录出场点位置

    # ===== 分批止盈（减仓）配置 =====
    # 基于估值分位修复的分批减仓策略
    reduce_position_signals = [False] * n
    reduce_position_ratios = [None] * n
    reduce_triggered_50 = False  # 50%分位减仓是否已触发
    reduce_triggered_80 = False  # 80%分位减仓是否已触发

    # 获取进场时的估值分位
    entry_val_pct = industry_val_data.get('core_indicator_pct_1250d') if industry_val_data else None
    if entry_val_pct is not None and entry_val_pct > 1:
        entry_val_pct = entry_val_pct / 100.0  # 转换为0-1范围

    # 止盈线不再使用固定目标价，改用分批减仓+移动止损
    # 仅保留止盈标记用于图表显示（从移动止损触发点获取）
    tp_price = None
    tp_type = None

    # ===== 根据进场类型选择止损策略 =====
    is_reverse_buy = entry_type == 'ReverseBuy'

    # 底部反转专用止损策略：使用均线突破状态切换阶段
    if is_reverse_buy:
        initial_stop_pct = 0.75  # 成本价的75% = -25%
    else:
        initial_stop_pct = 0.85  # 成本价的85% = -15%

    # ===== 分级移动止损状态机（单向升级，只上不下）=====
    max_stage_reached = 0  # 0=initial, 1=break_even, 2=protect, 3=lock
    last_stop_price = entry_price * initial_stop_pct

    # 跟踪均线突破状态（仅用于底部反转信号）
    ma50_crossed = False
    ma100_crossed = False

    for i in range(entry_idx + 1, n):
        if (stop_triggered or profit_triggered) and not continue_after_exit:
            break

        current_price = df.iloc[i]['close']
        current_date = df.iloc[i]['date']
        profit_pct = (current_price - entry_price) / entry_price

        # 获取当前均线值
        ma20 = df.iloc[i].get('ma20')
        ma50 = df.iloc[i].get('ma50')
        ma100 = df.iloc[i].get('ma100')

        # ===== 阶段判定（基于均线突破状态）=====
        # 阶段定义：0=初始, 1=保本, 2=浮盈保护, 3=保护, 4=锁定
        if is_reverse_buy:
            # 底部反转信号：使用均线突破状态切换阶段
            # print(f"current_date: {current_date}, ma50: {ma50}, ma100: {ma100}, current_price: {current_price}, profit_pct: {profit_pct}, max_stage_reached: {max_stage_reached}")
            # 检查是否站上MA50
            if not ma50_crossed and pd.notna(ma50) and current_price > ma50:
                ma50_crossed = True
                max_stage_reached = max(max_stage_reached, 1)  # 进入保本阶段

            # 检查是否站上MA100
            if not ma100_crossed and pd.notna(ma100) and current_price > ma100:
                ma100_crossed = True
                # 突破MA100后，如果浮盈>=25%直接进入锁定阶段，否则进入过渡阶段（仍用MA50）
                if profit_pct >= 0.30:
                    max_stage_reached = max(max_stage_reached, 3)  # 直接锁定
                elif profit_pct >= 0.50:
                    max_stage_reached = max(max_stage_reached, 3)  # 过渡阶段（MA100突破但浮盈不足）

            # 浮盈保护阶段：已突破MA50但未突破MA100，且浮盈≥20%
            if ma50_crossed and not ma100_crossed and profit_pct >= 0.25:
                max_stage_reached = max(max_stage_reached, 2)  # 进入浮盈保护阶段

            # 锁定阶段：突破MA100后浮盈达到25%（从过渡阶段升级）
            if ma100_crossed and profit_pct >= 0.50:
                max_stage_reached = max(max_stage_reached, 4)
        else:
            # 正常信号：使用浮盈比例切换阶段
            # 阶段映射：0=初始, 1=保本, 3=保护(20%浮盈), 4=锁定(30%浮盈)
            # 注意：stage 2 (profit_protect) 仅用于 ReverseBuy
            if profit_pct >= 0.30:
                max_stage_reached = max(max_stage_reached, 4)  # 锁定阶段
            elif profit_pct >= 0.20:
                max_stage_reached = max(max_stage_reached, 3)  # 保护阶段
            elif profit_pct >= 0.10:
                max_stage_reached = max(max_stage_reached, 1)  # 保本阶段

        # ===== Stop-Loss Calculation (基于最高阶段，不回落) =====
        # 阶段定义：0=初始, 1=保本, 2=浮盈保护(ReverseBuy专用), 3=保护, 4=锁定
        if max_stage_reached == 0:
            # 初始阶段：固定止损位
            stop_price = entry_price * initial_stop_pct
            stop_type = 'bottom_initial' if is_reverse_buy else 'initial'

        elif max_stage_reached == 1:
            # 保本阶段：成本价（固定，不随MA波动）
            stop_price = entry_price
            stop_type = 'break_even'

        elif max_stage_reached == 2:
            # 浮盈保护阶段（仅ReverseBuy）：使用MA50保护部分利润
            stop_price = ma50 if pd.notna(ma50) else entry_price
            stop_type = 'profit_protect'

        elif max_stage_reached == 3:
            # 过渡阶段（MA100已突破但浮盈<25%）：继续使用MA50，避免被震荡出来
            stop_price = ma50 if pd.notna(ma50) else entry_price
            stop_type = 'transition'

        else:  # max_stage_reached == 4
            # 锁定阶段（MA100突破且浮盈>=25%）：使用MA50锁定利润
            ma50_current = df.iloc[i].get('ma50')
            stop_price = ma50_current if pd.notna(ma50_current) else entry_price
            stop_type = 'lock'

        # 止损价只上不下（保护利润）
        stop_price = max(stop_price, last_stop_price)
        last_stop_price = stop_price

        stop_loss_line[i] = stop_price
        stop_loss_type[i] = stop_type

        # Check if stop-loss is triggered (移动止损清仓)
        if current_price < stop_price:
            stop_triggered = True
            if exit_idx is None:
                exit_idx = i

        # ===== 分批止盈（减仓）逻辑 =====
        # 基于估值分位修复的分批减仓，非清仓
        # 获取当前估值分位（如果有动态更新）
        current_val_pct = df.iloc[i].get('core_indicator_pct')
        if current_val_pct is None and industry_val_data:
            current_val_pct = industry_val_data.get('core_indicator_pct_1250d')
        if current_val_pct is not None and current_val_pct > 1:
            current_val_pct = current_val_pct / 100.0

        # 第一批减仓：估值分位修复至50%
        if not reduce_triggered_50 and current_val_pct is not None and current_val_pct >= 0.50:
            reduce_triggered_50 = True
            reduce_position_signals[i] = True
            reduce_position_ratios[i] = 0.30  # 减仓30%
            take_profit_line[i] = current_price  # 标记减仓点
            take_profit_type[i] = 'reduce_50'

        # 第二批减仓：估值分位修复至80%
        if not reduce_triggered_80 and current_val_pct is not None and current_val_pct >= 0.80:
            reduce_triggered_80 = True
            reduce_position_signals[i] = True
            reduce_position_ratios[i] = 0.30  # 再减仓30%
            take_profit_line[i] = current_price  # 标记减仓点
            take_profit_type[i] = 'reduce_80'

        # 剩余仓位使用移动止损保护，不设固定止盈价

        # ===== Add-Position Signal (金字塔加仓) =====
        # Only within 8 weeks of entry, and price drops 10-15%
        weeks_since_entry = i - entry_idx
        if weeks_since_entry <= 8 and profit_pct < -0.10:
            # Check F-Score if available (skip if F-Score < 4)
            f_score_ok = True
            if f_score_data:
                # Get date string for lookup
                try:
                    idx_val = df.index[i]
                    if hasattr(idx_val, 'strftime'):
                        current_date = idx_val.strftime("%Y-%m-%d")
                    else:
                        current_date = str(idx_val)[:10]
                    f_score = f_score_data.get(current_date)
                    if f_score is not None and f_score < 4:
                        f_score_ok = False
                except (IndexError, AttributeError):
                    pass

            if f_score_ok:
                add_position_signals[i] = True
                # 30% if drop < 15%, 50% if drop >= 15%
                add_position_ratios[i] = 0.50 if profit_pct <= -0.15 else 0.30

    return {
        'stop_loss_line': stop_loss_line,
        'stop_loss_type': stop_loss_type,
        'take_profit_line': take_profit_line,
        'take_profit_type': take_profit_type,
        'add_position_signals': add_position_signals,
        'add_position_ratios': add_position_ratios,
        'reduce_position_signals': reduce_position_signals,
        'reduce_position_ratios': reduce_position_ratios,
        'reduce_triggered_50': reduce_triggered_50,
        'reduce_triggered_80': reduce_triggered_80,
        'stop_triggered': stop_triggered,
        'profit_triggered': profit_triggered,
        'exit_idx': exit_idx,
        'initial_stop_price': entry_price * initial_stop_pct,  # 初始止损价（ReverseBuy: -25%, 其他: -15%）
        'entry_type': entry_type,
    }


def calculate_take_profit_target(
    entry_price: float,
    current_pct: Optional[float],
    indicator_type: Optional[str] = None,
    fair_pct: float = 0.50
) -> Optional[float]:
    """
    Calculate take-profit target based on core indicator percentile recovery.

    Args:
        entry_price: Price at entry
        current_pct: Current core indicator percentile (0-1)
        indicator_type: 'PE', 'PB', 'PS', or 'PEG'
        fair_pct: Target percentile (default 50%)

    Returns:
        Target price or None if already at/above fair value
    """
    if current_pct is None or current_pct >= fair_pct:
        return None

    # Elasticity factors by indicator type
    elasticity = {
        "PE": 0.6,
        "PB": 0.5,
        "PS": 0.7,
        "PEG": 0.5,
    }
    e = elasticity.get(indicator_type, 0.5)

    # Target upside = (fair_pct - current_pct) / current_pct * elasticity
    upside = (fair_pct - current_pct) / current_pct * e
    target_price = entry_price * (1 + upside)

    return target_price


def classify_entry_type(row: pd.Series) -> str:
    """
    Classify entry signal type with priority: ReverseBuy > StrongBuy > Buy.
    When multiple signals exist on same K-line, return the highest priority.
    """
    if row.get('bottom_buy_signal'):
        return 'ReverseBuy'
    elif row.get('enhanced_entry'):
        return 'StrongBuy'
    elif row.get('signal_entry'):
        return 'Buy'
    return 'Unknown'
