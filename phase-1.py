"""
Project: Pulse915 Backtesting System
Phase: 1 - Smart Gatekeeper with Gap Protection & Symbol Filtering
Version: 1.4
Date: 2026-01-10
Updated By: Sabrish Surender

Description:
This script performs the initial filtering of stocks from the Nifty 500 universe. 
It applies multiple "gates" including Liquidity (20D Avg Turnover), ATR% (Volatility), 
Price Range, Gap Protection, and Robust Momentum (Volume Spike vs 5-Day average with VWAP confirmation).

Recent Changes (v1.4):
- NEW: Gap Protection Logic implemented:
  - Gap Down Rule: If Gap% <= -2.0, HARD REJECT
  - Gap Up Rule: If Gap% >= +2.0, calculate Exhaustion Ratio (Gap%/ATR%)
    - If Ratio >= 0.8, REJECT (Exhaustion Gap)
    - Else, KEEP (Momentum Gap)
- Updated parameter thresholds:
  - MIN_TURNOVER_CR = 30
  - PRICE_MIN = 100, PRICE_MAX = 3000
  - MIN_ATR_PERCENT = 2.0
  - VOLUME_MULTIPLIER = 1.25
  - MAX_SPREAD_PERCENT = 1.0
- History Check: MIN_VALID_DAYS = 2, HistoryQualityFlag for 2-4 days
- Retains look-ahead bias fixes from v1.3

Previous Changes (v1.3):
- Fixed P1-1: ATR14 now calculated per trade_date using only historical data (no look-ahead bias)
- Fixed P1-2: Spread calculation now uses High.max() - Low.min() for full window range
- Fixed P1-3: Liquidity now calculated per trade_date excluding current day
- Fixed P1-4: ATR calculated per trade_date, not once per symbol
- Fixed P1-5: Liquidity calculated per trade_date, not once per symbol
- Robust Momentum Gate refined for 5-day historical lookback.
- Optimized for large dataset processing with intraday caching.
"""

import pandas as pd
import numpy as np
import os
from datetime import time

# ==============================
# CONFIG
# ==============================
DAILY_FILE = "downloaded_data/daily_candles_nifty500.xlsx"
INTRADAY_PATH = "downloaded_data/5min"
OUTPUT_FILE = "phase-1results/phase1_results.xlsx"

# Final Parameter Thresholds (v1.4)
MIN_TURNOVER_CR = 30            # Updated from 20
PRICE_MIN = 100                 # Updated from 80
PRICE_MAX = 3000                # Updated from 5000
MIN_ATR_PERCENT = 2.0           # Updated from 1.5
VOLUME_MULTIPLIER = 1.25        # Updated from 1.15
MAX_SPREAD_PERCENT = 1.0        # Same as before

# History Check Configuration
MIN_VALID_DAYS = 2              # Minimum days of history required (was implicit 14)

# Gap Protection Thresholds
GAP_DOWN_THRESHOLD = -2.0       # Gap% below this = HARD REJECT
GAP_UP_THRESHOLD = 2.0          # Gap% above this = check exhaustion
EXHAUSTION_RATIO_THRESHOLD = 0.8  # Gap%/ATR% >= this = Exhaustion Gap, REJECT

# Time window configuration
TIME_START = time(9, 15)
TIME_END = time(9, 35)
USE_ALTERNATE_TIME = False
ALT_TIME_START = time(9, 15)
ALT_TIME_END = time(9, 45)

# Diagnostic cross-check (prints gate-level counts to console only)
DIAGNOSTIC = True

print("Phase-1 Started (Smart Gatekeeper with Gap Protection) - v1.4")


# -- Validate input paths early to fail fast
if not os.path.exists(DAILY_FILE):
    raise FileNotFoundError(f"Daily data not found: {DAILY_FILE}")
if not os.path.exists(INTRADAY_PATH):
    raise FileNotFoundError(f"Intraday path not found: {INTRADAY_PATH}")
# Ensure output directory exists
out_dir = os.path.dirname(OUTPUT_FILE)
if out_dir:
    os.makedirs(out_dir, exist_ok=True)

# ==============================
# LOAD DAILY DATA
# ==============================
daily_df = pd.read_excel(DAILY_FILE)
daily_df["Datetime"] = pd.to_datetime(daily_df["Datetime"])

for col in ["Open", "High", "Low", "Close", "Volume"]:
    daily_df[col] = pd.to_numeric(daily_df[col], errors="coerce")

daily_df = daily_df.sort_values(["Symbol", "Datetime"])

# ==============================
# HELPERS
# ==============================
def calculate_daily_atr_percent_raw(df):
    """
    Calculate ATR% using Wilder's EMA method.
    Expects df to be sorted by Datetime and contain only historical data.
    """
    if len(df) < 14:
        return 0.0
    hl = df["High"] - df["Low"]
    hc = (df["High"] - df["Close"].shift()).abs()
    lc = (df["Low"] - df["Close"].shift()).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    # Use Wilder's EMA method for ATR (alpha = 1/14) - standard ATR calculation
    atr14 = tr.ewm(alpha=1/14, min_periods=14, adjust=False).mean().iloc[-1]
    close = df["Close"].iloc[-1]
    return (atr14 / close) * 100 if close > 0 else 0.0

def calculate_vwap_typical(df):
    typical_price = (df["High"] + df["Low"] + df["Close"]) / 3
    vol = df["Volume"].sum()
    return (typical_price * df["Volume"]).sum() / vol if vol > 0 else 0.0

def calculate_liquidity_for_date(symbol_df, trade_date_str):
    """
    Calculate 20D average turnover using only data BEFORE trade_date.
    This avoids look-ahead bias.
    """
    trade_date = pd.to_datetime(trade_date_str).date()
    # Filter to only include completed days before trade_date
    historical = symbol_df[symbol_df["Datetime"].dt.date < trade_date].copy()
    
    if len(historical) < 20:
        # Not enough historical data, use what we have
        if len(historical) == 0:
            return 0.0
        historical["TurnoverCr"] = (historical["Close"] * historical["Volume"]) / 1e7
        return historical["TurnoverCr"].mean()
    
    historical["TurnoverCr"] = (historical["Close"] * historical["Volume"]) / 1e7
    return historical["TurnoverCr"].tail(20).mean()

def calculate_atr_for_date(symbol_df, trade_date_str):
    """
    Calculate ATR% using only data BEFORE trade_date.
    This avoids look-ahead bias.
    """
    trade_date = pd.to_datetime(trade_date_str).date()
    # Filter to only include completed days before trade_date
    historical = symbol_df[symbol_df["Datetime"].dt.date < trade_date].copy()
    historical = historical.sort_values("Datetime")
    
    if len(historical) < 14:
        return 0.0
    
    return calculate_daily_atr_percent_raw(historical)

# ==============================
# PROCESS EACH STOCK
# ==============================
results = []
intraday_cache = {}

# Diagnostic counters / samples (console-only)
diag = {
    'total_symbols': 0,
    'no_intraday_folder': [],
    'too_few_daily_rows': [],
    'history_low_quality': [],  # 2-4 days of valid history
    'gap_down_rejected': [],    # Gap% <= -2.0
    'gap_exhaustion_rejected': [],  # Gap% >= +2.0 with Exhaustion Ratio >= 0.8
    'gap_momentum_kept': [],    # Gap% >= +2.0 with Exhaustion Ratio < 0.8
    'liquidity_failed': [],
    'atr_failed': [],
    'no_intraday_date_match': [],
    'sanity_failed': [],
    'price_failed': [],
    'momentum_failed': [],
    'spread_failed': [],
    'final_passed': []
}

for symbol, sdf in daily_df.groupby("Symbol"):
    sdf = sdf.dropna().sort_values("Datetime")
    diag['total_symbols'] += 1
    
    # History Check - need at least MIN_VALID_DAYS of daily data
    valid_days = len(sdf)
    if valid_days < MIN_VALID_DAYS:
        diag['too_few_daily_rows'].append(symbol)
        continue
    
    # Determine history quality flag
    if MIN_VALID_DAYS <= valid_days < 5:
        history_quality_flag = "LOW"
        diag['history_low_quality'].append(symbol)
    else:
        history_quality_flag = "OK"

    stock_folder = os.path.join(INTRADAY_PATH, symbol)
    if not os.path.isdir(stock_folder):
        diag['no_intraday_folder'].append(symbol)
        continue

    intraday_files = sorted(os.listdir(stock_folder))

    for i, file in enumerate(intraday_files):
        trade_date = file.replace(".csv", "")
        
        if trade_date not in sdf["Datetime"].dt.strftime("%Y-%m-%d").values:
            # intraday date doesn't match daily data for this symbol
            diag['no_intraday_date_match'].append((symbol, trade_date))
            continue
        
        # ============================================
        # FIX P1-4 & P1-5: Calculate per trade_date
        # ============================================
        
        # 1️⃣ LIQUIDITY GATE - calculated per trade_date using only historical data
        avg_20d_turnover = calculate_liquidity_for_date(sdf, trade_date)
        liquidity_pass = avg_20d_turnover >= MIN_TURNOVER_CR
        
        # 2️⃣ ATR GATE (RAW) - calculated per trade_date using only historical data
        atr_percent_raw = calculate_atr_for_date(sdf, trade_date)
        atr_pass = atr_percent_raw >= MIN_ATR_PERCENT
        
        # ============================================
        # 3️⃣ GAP PROTECTION LOGIC (NEW in v1.4)
        # ============================================
        # Get today's row and yesterday's close from daily data
        trade_date_dt = pd.to_datetime(trade_date).date()
        today_row = sdf[sdf["Datetime"].dt.date == trade_date_dt]
        historical_before = sdf[sdf["Datetime"].dt.date < trade_date_dt].sort_values("Datetime")
        
        # Initialize gap variables
        gap_percent = 0.0
        exhaustion_ratio = 0.0
        gap_status = "NORMAL"  # NORMAL, GAP_DOWN_REJECT, EXHAUSTION_REJECT, MOMENTUM_GAP
        gap_pass = True  # Default pass
        
        if not today_row.empty and not historical_before.empty:
            open_today = today_row["Open"].iloc[0]
            close_yesterday = historical_before["Close"].iloc[-1]
            
            if close_yesterday > 0:
                gap_percent = ((open_today - close_yesterday) / close_yesterday) * 100
                
                # Gap Down Rule: If Gap% <= -2.0, HARD REJECT
                if gap_percent <= GAP_DOWN_THRESHOLD:
                    gap_status = "GAP_DOWN_REJECT"
                    gap_pass = False
                    diag['gap_down_rejected'].append((symbol, trade_date, round(gap_percent, 2)))
                
                # Gap Up Rule: If Gap% >= +2.0, check exhaustion
                elif gap_percent >= GAP_UP_THRESHOLD:
                    # Calculate Exhaustion Ratio: Gap% / ATR%
                    if atr_percent_raw > 0:
                        exhaustion_ratio = gap_percent / atr_percent_raw
                        
                        if exhaustion_ratio >= EXHAUSTION_RATIO_THRESHOLD:
                            gap_status = "EXHAUSTION_REJECT"
                            gap_pass = False
                            diag['gap_exhaustion_rejected'].append((symbol, trade_date, round(gap_percent, 2), round(exhaustion_ratio, 2)))
                        else:
                            gap_status = "MOMENTUM_GAP"
                            gap_pass = True  # KEEP - Momentum Gap
                            diag['gap_momentum_kept'].append((symbol, trade_date, round(gap_percent, 2), round(exhaustion_ratio, 2)))
                    else:
                        # No ATR data, treat large gap up as risky
                        gap_status = "EXHAUSTION_REJECT"
                        gap_pass = False
                        diag['gap_exhaustion_rejected'].append((symbol, trade_date, round(gap_percent, 2), "NO_ATR"))
        
        # ============================================
        # Load and process intraday data
        # ============================================
        file_path = os.path.join(stock_folder, file)
        if file_path not in intraday_cache:
            intraday_cache[file_path] = pd.read_csv(file_path)
        df = intraday_cache[file_path].copy()
        for col in ["Open", "High", "Low", "Close", "Volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df["Datetime"] = pd.to_datetime(trade_date + " " + df["Time"].astype(str))
        df.set_index("Datetime", inplace=True)
        # remove duplicate timestamps if present
        if df.index.duplicated().any():
            df = df[~df.index.duplicated(keep='first')]
        df = df.sort_index()

        # choose time window (respects USE_ALTERNATE_TIME)
        ts = ALT_TIME_START if USE_ALTERNATE_TIME else TIME_START
        te = ALT_TIME_END if USE_ALTERNATE_TIME else TIME_END
        window = df.between_time(ts, te)
        if window.empty:
            continue

        # basic sanity checks (skip only this slot if invalid)
        if window["Volume"].sum() == 0 or window["Close"].iloc[-1] <= 0:
            diag['sanity_failed'].append((symbol, trade_date))
            continue

        # 3️⃣ PRICE GATE
        cmp_price = window["Close"].iloc[-1]
        if cmp_price <= 0:
            continue
        price_pass = PRICE_MIN <= cmp_price <= PRICE_MAX
        if not price_pass:
            diag['price_failed'].append((symbol, trade_date, cmp_price))

        # ============================================
        # SPREAD FILTER DISABLED (v1.4.1)
        # Reason: The (High.max - Low.min) calculation measures volatility/range,
        # NOT actual bid-ask spread. Bid-Ask spread cannot be calculated from OHLC data.
        # This was incorrectly rejecting valid high-volatility breakout candidates.
        # ============================================
        spread_pct = ((window["High"].max() - window["Low"].min()) / cmp_price) * 100
        spread_pass = True  # FORCED TRUE - Spread filter disabled
        # Original logic (disabled):
        # spread_pass = spread_pct <= MAX_SPREAD_PERCENT
        # if not spread_pass:
        #     diag['spread_failed'].append((symbol, trade_date, round(spread_pct,4)))

        # 4️⃣ ROBUST MOMENTUM GATE
        current_volume = window["Volume"].sum()
        prev_volumes = []
        # explicit lookback over up to 5 prior files (previous trading days)
        lookback_start = max(0, i - 5)
        for j in range(lookback_start, i):
            prev_path = os.path.join(stock_folder, intraday_files[j])
            if prev_path not in intraday_cache:
                intraday_cache[prev_path] = pd.read_csv(prev_path)
            pdf = intraday_cache[prev_path].copy()
            for col in ["Open", "High", "Low", "Close", "Volume"]:
                pdf[col] = pd.to_numeric(pdf[col], errors="coerce")

            pdf["Datetime"] = pd.to_datetime(
                intraday_files[j].replace(".csv", "") + " " + pdf["Time"].astype(str)
            )
            pdf.set_index("Datetime", inplace=True)

            # remove duplicate timestamps if present in historical intraday
            if pdf.index.duplicated().any():
                pdf = pdf[~pdf.index.duplicated(keep='first')]

            pw = pdf.between_time(ts, te)
            vol = pw["Volume"].sum() if not pw.empty else 0
            if vol > 0:
                prev_volumes.append(vol)

        # Calculate momentum metrics regardless of how many days of historical data we have
        if len(prev_volumes) == 0:
            # No historical data available - cannot calculate momentum
            momentum_pass = False
            avg_5d_volume = 0
            vol_mult = 0
            vwap = 0
            above_vwap = "NO"
        else:
            # Calculate with whatever historical data is available
            avg_5d_volume = np.mean(prev_volumes)
            vol_mult = current_volume / avg_5d_volume if avg_5d_volume > 0 else 0
            vwap = calculate_vwap_typical(window)
            above_vwap = "YES" if cmp_price >= vwap else "NO"

            momentum_pass = (
                current_volume >= avg_5d_volume * VOLUME_MULTIPLIER
                and cmp_price >= vwap
            )
            if not momentum_pass:
                diag['momentum_failed'].append((symbol, trade_date))

        final_pass = liquidity_pass and price_pass and atr_pass and momentum_pass and spread_pass and gap_pass
        if final_pass:
            diag['final_passed'].append((symbol, trade_date))

        # Real-time filter status print (like Phase 2)
        liq_status = "✓" if liquidity_pass else "✗"
        price_status = "✓" if price_pass else "✗"
        atr_status = "✓" if atr_pass else "✗"
        gap_status_symbol = "✓" if gap_pass else "✗"
        momentum_status = "✓" if momentum_pass else "✗"
        spread_status = "✓" if spread_pass else "✗"
        final_status = "PASS ✓" if final_pass else "FAIL ✗"
        
        print(f"[{symbol}|{trade_date}] Liq:{liq_status} Price:{price_status} ATR:{atr_status} Gap:{gap_status_symbol} Mom:{momentum_status} Spread:{spread_status} → {final_status}")

        results.append({
            "Date": trade_date,
            "Symbol": symbol,
            "HistoryQualityFlag": history_quality_flag,

            "20D Avg Turnover ₹Cr": round(avg_20d_turnover, 4),
            "CMP ₹": round(cmp_price, 4),

            "ATR% Raw": round(atr_percent_raw, 6),
            "ATR% Rounded": round(atr_percent_raw, 2),

            # Gap Protection fields
            "Gap%": round(gap_percent, 2),
            "Exhaustion Ratio": round(exhaustion_ratio, 2),
            "Gap Status": gap_status,
            "Gap Pass": "YES" if gap_pass else "NO",

            "Current Slot Volume": int(current_volume),
            "5D Slot Avg Volume": round(avg_5d_volume, 2),
            "VolMult": round(vol_mult, 4),

            "VWAP": round(vwap, 4),
            "Above VWAP": above_vwap,
            "Spread %": round(spread_pct, 4),
            "Spread Pass": "YES" if spread_pass else "NO",

            "Liquidity Pass": "YES" if liquidity_pass else "NO",
            "Price Pass": "YES" if price_pass else "NO",
            "ATR Pass": "YES" if atr_pass else "NO",
            "Momentum Pass": "YES" if momentum_pass else "NO",
            "Phase-1 Final Pass": "YES" if final_pass else "NO"
        })

# ==============================
# OUTPUT
# ==============================
out_df = pd.DataFrame(results)
try:
    out_df.to_excel(OUTPUT_FILE, index=False)
    print(f"Phase-1 Completed → {OUTPUT_FILE}")
except PermissionError:
    # Fallback: write CSV if the Excel file is locked / not writable
    alt = OUTPUT_FILE.replace('.xlsx', '.csv')
    out_df.to_csv(alt, index=False)
    print(f"Phase-1 Completed → {alt} (fallback, original XLSX not writable)")

if DIAGNOSTIC:
    def sample(lst, n=5):
        try:
            return lst[:n]
        except Exception:
            return []

    print("\n=== Diagnostic Summary ===")
    print(f"Total symbols in daily feed: {diag['total_symbols']}")
    print(f"Final passed count: {len(diag['final_passed'])}")
    print(f"Symbols missing intraday folder: {len(diag['no_intraday_folder'])} -> samples: {sample(diag['no_intraday_folder'])}")
    print(f"Too few daily rows (<{MIN_VALID_DAYS}): {len(diag['too_few_daily_rows'])} -> samples: {sample(diag['too_few_daily_rows'])}")
    print(f"History LOW quality (2-4 days): {len(diag['history_low_quality'])} -> samples: {sample(diag['history_low_quality'])}")
    print("--- Gap Protection ---")
    print(f"Gap Down Rejected (Gap% <= {GAP_DOWN_THRESHOLD}): {len(diag['gap_down_rejected'])} -> samples: {sample(diag['gap_down_rejected'])}")
    print(f"Exhaustion Gap Rejected (Ratio >= {EXHAUSTION_RATIO_THRESHOLD}): {len(diag['gap_exhaustion_rejected'])} -> samples: {sample(diag['gap_exhaustion_rejected'])}")
    print(f"Momentum Gap Kept (Ratio < {EXHAUSTION_RATIO_THRESHOLD}): {len(diag['gap_momentum_kept'])} -> samples: {sample(diag['gap_momentum_kept'])}")
    print("--- Other Gates ---")
    print(f"Liquidity failures (per date): {len(diag['liquidity_failed'])} -> samples: {sample(diag['liquidity_failed'])}")
    print(f"ATR failures (per date): {len(diag['atr_failed'])} -> samples: {sample(diag['atr_failed'])}")
    print(f"Sanity failures (zero vol / non-positive close): {len(diag['sanity_failed'])} -> samples: {sample(diag['sanity_failed'])}")
    print(f"Price failures: {len(diag['price_failed'])} -> samples: {sample(diag['price_failed'])}")
    print(f"Momentum failures (slot): {len(diag['momentum_failed'])} -> samples: {sample(diag['momentum_failed'])}")
    print(f"Spread failures: {len(diag['spread_failed'])} -> samples: {sample(diag['spread_failed'])}")
    print(f"Intraday date mismatches logged: {len(diag['no_intraday_date_match'])} -> samples: {sample(diag['no_intraday_date_match'])}")
    print("===========================\n")