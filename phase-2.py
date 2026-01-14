"""
Project: Pulse915 Backtesting System
Phase: 2 - Physics Scoring Engine with Dual-Lane Scoring
Version: 1.6
Date: 2026-01-10
Updated By: Sabrish Surender

Description:
Assigns quantitative scores to stocks that passed Phase 1 using DUAL-LANE Physics Scoring.
Calculates two separate scores for each stock:
- Score_Breakout (Mode A/C): For stocks breaking out ABOVE VWAP
- Score_Reclaim (Mode B): For stocks reclaiming VWAP from slightly below

Time Window: 09:15-09:45 with 5-min candles

Recent Changes (v1.6):
- NEW: Dual-Lane Scoring System to support both Breakout and Reclaim setups
- Score_Breakout: Hard fail if P_end <= VWAP_end (must be above VWAP)
  - Scoring: Volatility (0-40) + Volume (0-30) + Momentum (0-30)
- Score_Reclaim: Hard fail if P_end < VWAP_end * 0.98 (allow slightly below)
  - Proximity Bonus: +30 if |P_end - VWAP_end| < 0.5%
  - Scoring: Proximity + Volatility (0-40) + Moderate Volume (VolMult 1.5-3.0)
- Regime Column: 'BREAKOUT', 'RECLAIM', or 'NONE'
- Output Filtering: Keep if Score_Breakout >= 50 OR Score_Reclaim >= 50
- Starvation Guard: Relax to 40 if total count < 25

Previous Changes (v1.5):
- Physics Scoring Engine replaces old Velocity Scoring
- Time Window: 09:15-09:45 with 5-min candles

Previous Changes (v1.4):
- Fixed P2-10: Google News RSS GMT timestamps properly converted to IST
- Fixed P2-1 through P2-9: Various look-ahead bias and date sync fixes
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
from datetime import time as dtime


# ==============================
# CONFIG
# ==============================
PHASE1_FILE = "phase-1results/phase1_results.xlsx"
STOCK_30M_DIR = "downloaded_data/5min"

OUTPUT_DIR = "phase-2results"
OUTPUT_FILE = os.path.join(
    OUTPUT_DIR,
    "phase2_results.xlsx"
)

# ==============================
# STANDARDIZED TIME WINDOW - PHYSICS SCORING
# ==============================
# v1.5: Updated time window for Physics Scoring Engine
TIME_WINDOW_START = dtime(9, 15)
TIME_WINDOW_END = dtime(9, 45)  # Changed from 9:35 for Physics Scoring

# ==============================
# PHYSICS SCORING THRESHOLDS
# ==============================
# Volatility Score Thresholds (0-40) - Used by both lanes
ATR_HIGH_THRESHOLD = 2.0      # >= 2.0 -> 40 points
ATR_MED_THRESHOLD = 1.5       # >= 1.5 and < 2.0 -> 20 points

# Volume Score Thresholds (0-30) - For Breakout lane
VOLMULT_HIGH_THRESHOLD = 3.0  # >= 3.0 -> 30 points
VOLMULT_MED_THRESHOLD = 2.0   # >= 2.0 and < 3.0 -> 20 points
VOLMULT_LOW_THRESHOLD = 1.5   # >= 1.5 and < 2.0 -> 10 points

# Momentum Score Thresholds (0-30) - RS_Excess for Breakout lane
RS_HIGH_THRESHOLD = 1.0       # >= 1.0 -> 30 points
RS_MED_THRESHOLD = 0.5        # >= 0.5 and < 1.0 -> 15 points
RS_LOW_THRESHOLD = 0.25       # >= 0.25 and < 0.5 -> 5 points

# ==============================
# RECLAIM LANE THRESHOLDS (Mode B)
# ==============================
# Trend Gate for Reclaim: Allow slightly below VWAP (up to 2% below)
RECLAIM_VWAP_TOLERANCE = 0.98  # P_end >= VWAP_end * 0.98 to pass

# Proximity Bonus: If |P_end - VWAP_end| < 0.5%, award +30 points
PROXIMITY_THRESHOLD_PCT = 0.5  # 0.5% proximity threshold
PROXIMITY_BONUS_POINTS = 30    # Points for proximity bonus

# Reclaim Volume Thresholds (moderate volume is sufficient)
RECLAIM_VOLMULT_HIGH = 3.0    # >= 3.0 -> 30 points
RECLAIM_VOLMULT_MED = 1.5     # >= 1.5 and < 3.0 -> 15 points

# Starvation Guard
PRIMARY_SCORE_THRESHOLD = 50  # Primary filter: Score >= 50
RELAXED_SCORE_THRESHOLD = 40  # Relaxed filter if < 25 stocks pass primary
MIN_STOCKS_FOR_PRIMARY = 25   # Minimum stocks before relaxing threshold

# NOTE: Catalyst/News Engine removed in v1.5 - no external API calls required
# This version runs purely on CSV data for faster execution

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("Phase-2 Started - v1.6 with Dual-Lane Scoring (Breakout + Reclaim)")

# ==============================
# ATR & VOLMULT CALCULATION FUNCTIONS (from Phase-1)
# ==============================
def calculate_daily_atr_percent_raw(df):
    """
    Calculate daily ATR% using Wilder's EMA method.
    Expects df to be sorted by Datetime.
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
    """Calculate VWAP using typical price"""
    typical_price = (df["High"] + df["Low"] + df["Close"]) / 3
    vol = df["Volume"].sum()
    return (typical_price * df["Volume"]).sum() / vol if vol > 0 else 0.0

# ==============================
# LOAD PHASE-1 DATA
# ==============================
df_raw = pd.read_excel(PHASE1_FILE)

# Only need Symbol column from Phase-1 (we'll calculate ATR% and VolMult ourselves)
required_cols = ["Symbol"]
missing = [c for c in required_cols if c not in df_raw.columns]
if missing:
    raise ValueError(f"Missing required columns in Phase-1 file: {missing}")

# ==============================
# FILTER: Only process stocks with ALL "YES" criteria
# ==============================
print(f"Total stocks in Phase-1: {len(df_raw)}")

# Filter for stocks where all YES/NO columns are "YES"
yes_columns = [col for col in df_raw.columns if col.startswith("Above") or "Pass" in col or "Gate" in col]
if yes_columns:
    df_filtered = df_raw.copy()
    for col in yes_columns:
        if col in df_filtered.columns:
            df_filtered = df_filtered[df_filtered[col] == "YES"]
    print(f"Stocks with all YES criteria: {len(df_filtered)}")
else:
    # If no YES/NO columns, use all stocks
    df_filtered = df_raw.copy()
    print(f"No YES/NO columns found. Using all stocks: {len(df_filtered)}")

if len(df_filtered) == 0:
    raise ValueError("No stocks passed the filter! Check your Phase-1 results.")

# ==============================
# KEEP ONLY NECESSARY COLUMNS FROM PHASE-1
# ==============================
# Essential columns to keep from Phase-1
columns_to_keep = ["Symbol"]

# Check if Date column exists
if "Date" in df_filtered.columns:
    columns_to_keep.append("Date")

# Check if there are any additional useful columns (optional)
optional_cols = ["Sector", "Industry", "Market Cap", "Price"]
for col in optional_cols:
    if col in df_filtered.columns:
        columns_to_keep.append(col)

# Select only necessary columns
df = df_filtered[columns_to_keep].copy()

print(f"Kept {len(columns_to_keep)} columns from Phase-1: {columns_to_keep}")
print(f"Processing {len(df)} stocks for Phase-2...")
print("Will calculate ATR% and VolMult independently...\n")

# ==============================
# FIND & LOAD NIFTY 5M DATA
# ==============================
# For 5-min data, NIFTY files are in downloaded_data/^NSEI/ folder
NIFTY_FOLDER = os.path.join(STOCK_30M_DIR, "../^NSEI")

if not os.path.exists(NIFTY_FOLDER):
    raise FileNotFoundError(
        f"Missing NIFTY folder: {NIFTY_FOLDER}. Run download_nifty_folder.py first!"
    )

# Get all available NIFTY dates
nifty_files = sorted([f for f in os.listdir(NIFTY_FOLDER) if f.endswith('.csv')])
if not nifty_files:
    raise FileNotFoundError(f"No CSV files found in {NIFTY_FOLDER}")

# Use the latest NIFTY file
NIFTY_FILE = os.path.join(NIFTY_FOLDER, nifty_files[-1])
NIFTY_DATE = nifty_files[-1].replace('.csv', '')
print(f"Using NIFTY date: {NIFTY_DATE}")

nifty_df = pd.read_csv(NIFTY_FILE)

# For 5-min data, combine date from filename with Time column
if "Time" in nifty_df.columns:
    nifty_df["Datetime"] = pd.to_datetime(NIFTY_DATE + " " + nifty_df["Time"].astype(str))
else:
    nifty_df["Datetime"] = pd.to_datetime(nifty_df["Datetime"])

nifty_df["Close"] = pd.to_numeric(nifty_df["Close"], errors="coerce")
nifty_df = nifty_df.dropna(subset=["Close"]).sort_values("Datetime")

# ============================================
# v1.5: PHYSICS SCORING - NIFTY Anchor Extraction
# ============================================
nifty_df_indexed = nifty_df.set_index("Datetime")
nifty_window = nifty_df_indexed.between_time(TIME_WINDOW_START, TIME_WINDOW_END)

if len(nifty_window) < 2:
    raise ValueError(f"Not enough valid NIFTY candles in {NIFTY_FILE} for time window {TIME_WINDOW_START}-{TIME_WINDOW_END}")

# PHYSICS SCORING ANCHORS for NIFTY
# Nifty_Start = Close of 09:15 candle (first in window)
# Nifty_End = Close of 09:45 candle (last in window)
NIFTY_Start = float(nifty_window["Close"].iloc[0])   # Close of 09:15 candle
NIFTY_End = float(nifty_window["Close"].iloc[-1])    # Close of 09:45 candle

# Calculate Return_Nifty for RS_Excess calculation
NIFTY_Return = ((NIFTY_End - NIFTY_Start) / NIFTY_Start) * 100

print(f"NIFTY Physics Scoring Anchors (Time Window: {TIME_WINDOW_START} to {TIME_WINDOW_END}):")
print(f"  Nifty_Start (09:15 Close): {NIFTY_Start:.2f}")
print(f"  Nifty_End (09:45 Close): {NIFTY_End:.2f}")
print(f"  Return_Nifty: {NIFTY_Return:.2f}%")


# ==============================
# LOAD STOCK 5M DATA - PHYSICS SCORING ANCHORS
# ==============================
# Collect all Physics Scoring anchors for each stock
P_start_list = []      # Close of 09:15 candle
P_end_list = []        # Close of 09:45 candle
Open_0915_list = []    # Open of 09:15 candle (for Trend Gate)
VWAP_end_list = []     # Cumulative VWAP from 09:15 to 09:45
Vol_tod_list = []      # Sum of Volume from 09:15 to 09:45
skipped_symbols = []

print("\n" + "="*50)
print("Extracting Physics Scoring Anchors for Stocks...")
print("="*50)

for symbol in df["Symbol"]:
    # For 5-min data, each symbol has a folder with date-based CSV files
    stock_folder = os.path.join(STOCK_30M_DIR, symbol)
    
    if not os.path.exists(stock_folder):
        P_start_list.append(np.nan)
        P_end_list.append(np.nan)
        Open_0915_list.append(np.nan)
        VWAP_end_list.append(np.nan)
        Vol_tod_list.append(np.nan)
        skipped_symbols.append((symbol, "no_folder"))
        continue
    
    # Get all CSV files for this symbol
    stock_files = sorted([f for f in os.listdir(stock_folder) if f.endswith('.csv')])
    if not stock_files:
        P_start_list.append(np.nan)
        P_end_list.append(np.nan)
        Open_0915_list.append(np.nan)
        VWAP_end_list.append(np.nan)
        Vol_tod_list.append(np.nan)
        skipped_symbols.append((symbol, "no_files"))
        continue
    
    # Strict date synchronization - skip if no matching date
    preferred_file = f"{NIFTY_DATE}.csv"
    if preferred_file not in stock_files:
        P_start_list.append(np.nan)
        P_end_list.append(np.nan)
        Open_0915_list.append(np.nan)
        VWAP_end_list.append(np.nan)
        Vol_tod_list.append(np.nan)
        skipped_symbols.append((symbol, f"no_data_for_{NIFTY_DATE}"))
        continue
    
    stock_file = os.path.join(stock_folder, preferred_file)
    sdf = pd.read_csv(stock_file)
    
    date_from_file = os.path.basename(stock_file).replace('.csv', '')
    if "Time" in sdf.columns:
        sdf["Datetime"] = pd.to_datetime(date_from_file + " " + sdf["Time"].astype(str))
    else:
        sdf["Datetime"] = pd.to_datetime(sdf["Datetime"])
    
    # Convert all price/volume columns to numeric
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        if col in sdf.columns:
            sdf[col] = pd.to_numeric(sdf[col], errors="coerce")
    
    sdf = sdf.dropna(subset=["Close"]).sort_values("Datetime")
    sdf = sdf.set_index("Datetime")

    try:
        # Apply Physics Scoring time window (09:15 to 09:45)
        stock_window = sdf.between_time(TIME_WINDOW_START, TIME_WINDOW_END)
        
        if len(stock_window) >= 2:
            # PHYSICS SCORING ANCHORS:
            # P_start = Close of the 09:15 candle (first candle in window)
            p_start = float(stock_window["Close"].iloc[0])
            
            # P_end = Close of the 09:45 candle (last candle in window)
            p_end = float(stock_window["Close"].iloc[-1])
            
            # Open_0915 = Open of the 09:15 candle (for Trend Gate)
            open_0915 = float(stock_window["Open"].iloc[0]) if "Open" in stock_window.columns else p_start
            
            # VWAP_end = Cumulative VWAP from 09:15 to 09:45
            vwap_end = calculate_vwap_typical(stock_window)
            
            # Vol_tod = Sum of Volume from 09:15 to 09:45
            vol_tod = float(stock_window["Volume"].sum()) if "Volume" in stock_window.columns else 0.0
            
            P_start_list.append(p_start)
            P_end_list.append(p_end)
            Open_0915_list.append(open_0915)
            VWAP_end_list.append(vwap_end)
            Vol_tod_list.append(vol_tod)
        else:
            # Not enough candles in window
            P_start_list.append(np.nan)
            P_end_list.append(np.nan)
            Open_0915_list.append(np.nan)
            VWAP_end_list.append(np.nan)
            Vol_tod_list.append(np.nan)
            skipped_symbols.append((symbol, "insufficient_candles_in_window"))
    except Exception as e:
        P_start_list.append(np.nan)
        P_end_list.append(np.nan)
        Open_0915_list.append(np.nan)
        VWAP_end_list.append(np.nan)
        Vol_tod_list.append(np.nan)
        skipped_symbols.append((symbol, f"error: {str(e)}"))

# Add Physics Scoring anchors to dataframe
df["P_start"] = P_start_list       # Close of 09:15 candle
df["P_end"] = P_end_list           # Close of 09:45 candle
df["Open_0915"] = Open_0915_list   # Open of 09:15 candle
df["VWAP_end"] = VWAP_end_list     # Cumulative VWAP 09:15-09:45
df["Vol_tod"] = Vol_tod_list       # Sum of Volume 09:15-09:45
df["NIFTY_Start"] = NIFTY_Start
df["NIFTY_End"] = NIFTY_End
df["NIFTY_Return"] = NIFTY_Return

# Log skipped symbols
if skipped_symbols:
    print(f"\n⚠️  Skipped {len(skipped_symbols)} symbols due to data issues:")
    for sym, reason in skipped_symbols[:10]:
        print(f"   {sym}: {reason}")
    if len(skipped_symbols) > 10:
        print(f"   ... and {len(skipped_symbols) - 10} more")

df_before_drop = len(df)
df = df.dropna(subset=["P_start", "P_end", "VWAP_end"])
print(f"\nAfter Physics Scoring anchor filter: {len(df)} stocks (dropped {df_before_drop - len(df)})")

# ==============================
# CALCULATE ATR% AND VOLMULT (Independent calculation)
# ==============================
print("\nCalculating ATR% and VolMult for each stock...")

# Load daily candles file for ATR calculation
DAILY_FILE = "downloaded_data/daily_candles_nifty500.xlsx"
if not os.path.exists(DAILY_FILE):
    raise FileNotFoundError(f"Daily candles file not found: {DAILY_FILE}")

daily_df = pd.read_excel(DAILY_FILE)

# ============================================
# FIX P2-9: Ensure daily data has proper datetime parsing and sorting
# ============================================
daily_df["Datetime"] = pd.to_datetime(daily_df["Datetime"])
for col in ["Open", "High", "Low", "Close", "Volume"]:
    daily_df[col] = pd.to_numeric(daily_df[col], errors="coerce")
daily_df = daily_df.sort_values(["Symbol", "Datetime"])

ATR_list = []
VolMult_list = []
AboveVWAP_list = []

for idx, row in df.iterrows():
    symbol = row["Symbol"]
    
    # ============================================
    # FIX P2-1 & P2-9: Calculate ATR% from daily candles with proper sorting
    # and using only historical data before NIFTY_DATE
    # ============================================
    try:
        symbol_daily = daily_df[daily_df["Symbol"] == symbol].copy()
        symbol_daily = symbol_daily.sort_values("Datetime")  # FIX P2-9: Ensure sorted
        
        # Filter to only use data before NIFTY_DATE (avoid look-ahead bias)
        nifty_date_dt = pd.to_datetime(NIFTY_DATE).date()
        symbol_daily = symbol_daily[symbol_daily["Datetime"].dt.date < nifty_date_dt]
        
        if len(symbol_daily) >= 14:  # Need at least 14 days for ATR
            atr_pct = calculate_daily_atr_percent_raw(symbol_daily)
        else:
            atr_pct = 0.0
    except Exception as e:
        print(f"  Error calculating ATR for {symbol}: {e}")
        atr_pct = 0.0
    
    ATR_list.append(atr_pct)
    
    # ============================================
    # Calculate VolMult from 5-min candles using standardized time window
    # ============================================
    try:
        stock_folder = os.path.join(STOCK_30M_DIR, symbol)
        if not os.path.exists(stock_folder):
            VolMult_list.append(0.0)
            AboveVWAP_list.append("NO")
            continue
        
        # Get all CSV files for this symbol
        stock_files = sorted([f for f in os.listdir(stock_folder) if f.endswith('.csv')])
        if not stock_files:
            VolMult_list.append(0.0)
            AboveVWAP_list.append("NO")
            continue
        
        # ============================================
        # FIX P2-4: Strict date matching - use NIFTY_DATE only
        # ============================================
        preferred_file = f"{NIFTY_DATE}.csv"
        if preferred_file not in stock_files:
            # No data for NIFTY date - skip (should have been filtered earlier)
            VolMult_list.append(0.0)
            AboveVWAP_list.append("NO")
            continue
        
        current_file = preferred_file
        
        # Load current day data
        current_path = os.path.join(stock_folder, current_file)
        current_df = pd.read_csv(current_path)
        
        # Parse datetime - FIX P2-3: Use correct filename
        date_from_file = current_file.replace('.csv', '')
        if "Time" in current_df.columns:
            current_df["Datetime"] = pd.to_datetime(date_from_file + " " + current_df["Time"].astype(str))
        else:
            current_df["Datetime"] = pd.to_datetime(current_df["Datetime"])
        
        current_df.set_index("Datetime", inplace=True)
        
        # FIX P2-7: Use standardized time window (9:15-9:35)
        window = current_df.between_time(TIME_WINDOW_START, TIME_WINDOW_END)
        
        if window.empty:
            VolMult_list.append(0.0)
            AboveVWAP_list.append("NO")
            continue
        
        current_volume = window["Volume"].sum()
        cmp_price = window["Close"].iloc[-1]
        
        # Calculate historical average (past 5 days)
        prev_volumes = []
        current_idx = stock_files.index(current_file)
        lookback_start = max(0, current_idx - 5)
        
        for j in range(lookback_start, current_idx):
            prev_path = os.path.join(stock_folder, stock_files[j])
            prev_df = pd.read_csv(prev_path)
            
            date_from_prev = stock_files[j].replace('.csv', '')
            if "Time" in prev_df.columns:
                prev_df["Datetime"] = pd.to_datetime(date_from_prev + " " + prev_df["Time"].astype(str))
            else:
                prev_df["Datetime"] = pd.to_datetime(prev_df["Datetime"])
            
            prev_df.set_index("Datetime", inplace=True)
            # FIX P2-7: Use standardized time window
            prev_window = prev_df.between_time(TIME_WINDOW_START, TIME_WINDOW_END)
            
            if not prev_window.empty:
                vol = prev_window["Volume"].sum()
                if vol > 0:
                    prev_volumes.append(vol)
        
        # Calculate VolMult
        if len(prev_volumes) > 0:
            avg_5d_volume = np.mean(prev_volumes)
            vol_mult = current_volume / avg_5d_volume if avg_5d_volume > 0 else 0
            vwap = calculate_vwap_typical(window)
            above_vwap = "YES" if cmp_price >= vwap else "NO"
        else:
            vol_mult = 0.0
            above_vwap = "NO"
        
        VolMult_list.append(vol_mult)
        AboveVWAP_list.append(above_vwap)
        
    except Exception as e:
        print(f"  Error calculating VolMult for {symbol}: {e}")
        VolMult_list.append(0.0)
        AboveVWAP_list.append("NO")

# Add calculated values to dataframe
df["ATR% Raw"] = ATR_list
df["VolMult"] = VolMult_list
df["Above VWAP"] = AboveVWAP_list

print(f"✅ Calculated ATR% and VolMult for {len(df)} stocks\n")

# ==============================
# PHYSICS SCORING ENGINE
# ==============================
print("\n" + "="*50)
print("PHYSICS SCORING ENGINE - v1.5")
print("="*50)

# ==============================
# STEP 1: Calculate Returns & RS_Excess
# ==============================
print("\nSTEP 1: Calculating Stock Returns and RS_Excess...")

# Return_Stock = (P_end - P_start) / P_start * 100
df["Return_Stock"] = ((df["P_end"] - df["P_start"]) / df["P_start"]) * 100

# Return_Nifty already calculated globally (NIFTY_Return)
df["Return_Nifty"] = NIFTY_Return

# RS_Excess = Return_Stock - Return_Nifty
df["RS_Excess"] = df["Return_Stock"] - df["Return_Nifty"]

print(f"  Return_Stock range: {df['Return_Stock'].min():.2f}% to {df['Return_Stock'].max():.2f}%")
print(f"  Return_Nifty: {NIFTY_Return:.2f}%")
print(f"  RS_Excess range: {df['RS_Excess'].min():.2f} to {df['RS_Excess'].max():.2f}")

# ==============================
# STEP 2: Calculate VolMult_tod
# ==============================
print("\nSTEP 2: Calculating VolMult_tod (using same time window avg)...")

# VolMult_tod = Vol_tod / Avg_Vol_5_Session_Window
# We already have Vol_tod and Avg_5d_Volume from VolMult calculation
# Update VolMult to use the Vol_tod field
df["VolMult_tod"] = df["VolMult"]  # Use existing VolMult calculation

print(f"  VolMult_tod range: {df['VolMult_tod'].min():.2f} to {df['VolMult_tod'].max():.2f}")

# ==============================
# STEP 3: VOLATILITY SCORE (0-40) - Used by both lanes
# ==============================
print("\nSTEP 3: Calculating Volatility Score (0-40)...")

def calculate_volatility_score(atr):
    """
    Volatility Score based on ATR%:
    - ATR >= 2.0 -> 40 points
    - 1.5 <= ATR < 2.0 -> 20 points
    - Else -> 0 points
    """
    if atr >= ATR_HIGH_THRESHOLD:
        return 40
    elif atr >= ATR_MED_THRESHOLD:
        return 20
    else:
        return 0

df["VolatilityScore"] = df["ATR% Raw"].apply(calculate_volatility_score)
print(f"  Score 40 (ATR >= {ATR_HIGH_THRESHOLD}): {(df['VolatilityScore'] == 40).sum()} stocks")
print(f"  Score 20 ({ATR_MED_THRESHOLD} <= ATR < {ATR_HIGH_THRESHOLD}): {(df['VolatilityScore'] == 20).sum()} stocks")
print(f"  Score 0 (ATR < {ATR_MED_THRESHOLD}): {(df['VolatilityScore'] == 0).sum()} stocks")

# ==============================
# STEP 4: VOLUME SCORE - Breakout Lane (0-30)
# ==============================
print("\nSTEP 4: Calculating Breakout Volume Score (0-30)...")

def calculate_breakout_volume_score(vol_mult):
    """
    Breakout Volume Score based on VolMult_tod:
    - VolMult >= 3.0 -> 30 points
    - 2.0 <= VolMult < 3.0 -> 20 points
    - 1.5 <= VolMult < 2.0 -> 10 points
    - Else -> 0 points
    """
    if vol_mult >= VOLMULT_HIGH_THRESHOLD:
        return 30
    elif vol_mult >= VOLMULT_MED_THRESHOLD:
        return 20
    elif vol_mult >= VOLMULT_LOW_THRESHOLD:
        return 10
    else:
        return 0

df["VolumeScore_Breakout"] = df["VolMult_tod"].apply(calculate_breakout_volume_score)
print(f"  Score 30 (VolMult >= {VOLMULT_HIGH_THRESHOLD}): {(df['VolumeScore_Breakout'] == 30).sum()} stocks")
print(f"  Score 20 ({VOLMULT_MED_THRESHOLD} <= VolMult < {VOLMULT_HIGH_THRESHOLD}): {(df['VolumeScore_Breakout'] == 20).sum()} stocks")
print(f"  Score 10 ({VOLMULT_LOW_THRESHOLD} <= VolMult < {VOLMULT_MED_THRESHOLD}): {(df['VolumeScore_Breakout'] == 10).sum()} stocks")
print(f"  Score 0 (VolMult < {VOLMULT_LOW_THRESHOLD}): {(df['VolumeScore_Breakout'] == 0).sum()} stocks")

# ==============================
# STEP 5: MOMENTUM SCORE - Breakout Lane (0-30)
# ==============================
print("\nSTEP 5: Calculating Breakout Momentum Score (0-30) based on RS_Excess...")

def calculate_momentum_score(rs_excess):
    """
    Momentum Score based on RS_Excess:
    - RS_Excess >= 1.0 -> 30 points
    - 0.5 <= RS_Excess < 1.0 -> 15 points
    - 0.25 <= RS_Excess < 0.5 -> 5 points
    - Else -> 0 points
    """
    if rs_excess >= RS_HIGH_THRESHOLD:
        return 30
    elif rs_excess >= RS_MED_THRESHOLD:
        return 15
    elif rs_excess >= RS_LOW_THRESHOLD:
        return 5
    else:
        return 0

df["MomentumScore"] = df["RS_Excess"].apply(calculate_momentum_score)
print(f"  Score 30 (RS >= {RS_HIGH_THRESHOLD}): {(df['MomentumScore'] == 30).sum()} stocks")
print(f"  Score 15 ({RS_MED_THRESHOLD} <= RS < {RS_HIGH_THRESHOLD}): {(df['MomentumScore'] == 15).sum()} stocks")
print(f"  Score 5 ({RS_LOW_THRESHOLD} <= RS < {RS_MED_THRESHOLD}): {(df['MomentumScore'] == 5).sum()} stocks")
print(f"  Score 0 (RS < {RS_LOW_THRESHOLD}): {(df['MomentumScore'] == 0).sum()} stocks")

# ==============================
# STEP 6: RECLAIM LANE - Volume Score (Moderate thresholds)
# ==============================
print("\nSTEP 6: Calculating Reclaim Volume Score (moderate thresholds)...")

def calculate_reclaim_volume_score(vol_mult):
    """
    Reclaim Volume Score (moderate volume is sufficient):
    - VolMult >= 3.0 -> 30 points
    - 1.5 <= VolMult < 3.0 -> 15 points
    - Else -> 0 points
    """
    if vol_mult >= RECLAIM_VOLMULT_HIGH:
        return 30
    elif vol_mult >= RECLAIM_VOLMULT_MED:
        return 15
    else:
        return 0

df["VolumeScore_Reclaim"] = df["VolMult_tod"].apply(calculate_reclaim_volume_score)
print(f"  Score 30 (VolMult >= {RECLAIM_VOLMULT_HIGH}): {(df['VolumeScore_Reclaim'] == 30).sum()} stocks")
print(f"  Score 15 ({RECLAIM_VOLMULT_MED} <= VolMult < {RECLAIM_VOLMULT_HIGH}): {(df['VolumeScore_Reclaim'] == 15).sum()} stocks")
print(f"  Score 0 (VolMult < {RECLAIM_VOLMULT_MED}): {(df['VolumeScore_Reclaim'] == 0).sum()} stocks")

# ==============================
# STEP 7: RECLAIM LANE - Proximity Bonus
# ==============================
print("\nSTEP 7: Calculating Reclaim Proximity Bonus...")

def calculate_proximity_bonus(row):
    """
    Proximity Bonus for Reclaim lane:
    - If |P_end - VWAP_end| < 0.5% of VWAP_end -> +30 points
    - Else -> 0 points
    """
    p_end = row["P_end"]
    vwap_end = row["VWAP_end"]
    
    if vwap_end == 0:
        return 0
    
    # Calculate absolute difference as percentage of VWAP
    proximity_pct = abs(p_end - vwap_end) / vwap_end * 100
    
    if proximity_pct < PROXIMITY_THRESHOLD_PCT:
        return PROXIMITY_BONUS_POINTS
    return 0

df["ProximityBonus"] = df.apply(calculate_proximity_bonus, axis=1)
print(f"  Stocks with Proximity Bonus (+{PROXIMITY_BONUS_POINTS}): {(df['ProximityBonus'] > 0).sum()} stocks")
print(f"  Stocks without Proximity Bonus: {(df['ProximityBonus'] == 0).sum()} stocks")

# ==============================
# STEP 8: DUAL-LANE SCORING ENGINE
# ==============================
print("\n" + "="*60)
print("STEP 8: DUAL-LANE SCORING ENGINE - v1.6")
print("="*60)

def calculate_dual_lane_scores(row):
    """
    Calculate Score_Breakout and Score_Reclaim for each stock.
    
    Score_Breakout (Mode A/C):
    - Trend Gate: Hard FAIL if P_end <= VWAP_end (must be ABOVE VWAP)
    - Score = Volatility (0-40) + Volume (0-30) + Momentum (0-30)
    
    Score_Reclaim (Mode B):
    - Trend Gate: Hard FAIL if P_end < VWAP_end * 0.98 (allow slightly below)
    - Score = Proximity (0-30) + Volatility (0-40) + Moderate Volume (0-30)
    """
    p_end = row["P_end"]
    vwap_end = row["VWAP_end"]
    
    volatility = row["VolatilityScore"]
    volume_breakout = row["VolumeScore_Breakout"]
    momentum = row["MomentumScore"]
    volume_reclaim = row["VolumeScore_Reclaim"]
    proximity = row["ProximityBonus"]
    
    # ----- SCORE_BREAKOUT (Mode A/C) -----
    # Trend Gate: P_end MUST be > VWAP_end
    if p_end > vwap_end:
        score_breakout = volatility + volume_breakout + momentum
        breakout_gate = "PASS"
    else:
        score_breakout = 0
        breakout_gate = "FAIL"
    
    # ----- SCORE_RECLAIM (Mode B) -----
    # Trend Gate: P_end >= VWAP_end * 0.98 (allow up to 2% below)
    reclaim_threshold = vwap_end * RECLAIM_VWAP_TOLERANCE
    if p_end >= reclaim_threshold:
        score_reclaim = proximity + volatility + volume_reclaim
        reclaim_gate = "PASS"
    else:
        score_reclaim = 0
        reclaim_gate = "FAIL"
    
    return pd.Series({
        "Score_Breakout": score_breakout,
        "Score_Reclaim": score_reclaim,
        "BreakoutGate": breakout_gate,
        "ReclaimGate": reclaim_gate
    })

# Apply dual-lane scoring
print("\nCalculating dual-lane scores for all stocks...")
dual_scores = df.apply(calculate_dual_lane_scores, axis=1)
df["Score_Breakout"] = dual_scores["Score_Breakout"]
df["Score_Reclaim"] = dual_scores["Score_Reclaim"]
df["BreakoutGate"] = dual_scores["BreakoutGate"]
df["ReclaimGate"] = dual_scores["ReclaimGate"]

# Calculate Regime based on scores
def determine_regime(row):
    """
    Determine Regime:
    - If Score_Breakout >= 50: 'BREAKOUT'
    - Elif Score_Reclaim >= 50: 'RECLAIM'
    - Else: 'NONE'
    """
    if row["Score_Breakout"] >= PRIMARY_SCORE_THRESHOLD:
        return "BREAKOUT"
    elif row["Score_Reclaim"] >= PRIMARY_SCORE_THRESHOLD:
        return "RECLAIM"
    else:
        return "NONE"

df["Regime"] = df.apply(determine_regime, axis=1)

# Statistics
print("\n" + "-"*50)
print("BREAKOUT LANE (Mode A/C) Statistics:")
print("-"*50)
breakout_pass = (df["BreakoutGate"] == "PASS").sum()
breakout_fail = (df["BreakoutGate"] == "FAIL").sum()
print(f"  Trend Gate PASS: {breakout_pass} stocks")
print(f"  Trend Gate FAIL: {breakout_fail} stocks")
print(f"  Score_Breakout range: {df['Score_Breakout'].min():.0f} to {df['Score_Breakout'].max():.0f}")
print(f"  Score_Breakout >= 50: {(df['Score_Breakout'] >= 50).sum()} stocks")
print(f"  Score_Breakout >= 40: {(df['Score_Breakout'] >= 40).sum()} stocks")

print("\n" + "-"*50)
print("RECLAIM LANE (Mode B) Statistics:")
print("-"*50)
reclaim_pass = (df["ReclaimGate"] == "PASS").sum()
reclaim_fail = (df["ReclaimGate"] == "FAIL").sum()
print(f"  Trend Gate PASS: {reclaim_pass} stocks (P_end >= VWAP * {RECLAIM_VWAP_TOLERANCE})")
print(f"  Trend Gate FAIL: {reclaim_fail} stocks")
print(f"  Score_Reclaim range: {df['Score_Reclaim'].min():.0f} to {df['Score_Reclaim'].max():.0f}")
print(f"  Score_Reclaim >= 50: {(df['Score_Reclaim'] >= 50).sum()} stocks")
print(f"  Score_Reclaim >= 40: {(df['Score_Reclaim'] >= 40).sum()} stocks")
print(f"  Stocks with Proximity Bonus: {(df['ProximityBonus'] > 0).sum()}")

print("\n" + "-"*50)
print("REGIME Distribution:")
print("-"*50)
regime_counts = df["Regime"].value_counts()
for regime, count in regime_counts.items():
    print(f"  {regime}: {count} stocks")

# ==============================
# STARVATION GUARD - DUAL-LANE OUTPUT FILTERING
# ==============================
print("\n" + "="*60)
print("Applying Starvation Guard (Dual-Lane)...")
print("="*60)

# Create "best score" column for sorting (max of both lanes)
df["BestScore"] = df[["Score_Breakout", "Score_Reclaim"]].max(axis=1)

# Sort all stocks by BestScore (descending)
df_sorted = df.sort_values("BestScore", ascending=False).reset_index(drop=True)

# PRIMARY RULE: Keep stocks where Score_Breakout >= 50 OR Score_Reclaim >= 50
primary_condition = (df_sorted["Score_Breakout"] >= PRIMARY_SCORE_THRESHOLD) | \
                    (df_sorted["Score_Reclaim"] >= PRIMARY_SCORE_THRESHOLD)
primary_passed = df_sorted[primary_condition].copy()

print(f"\nPrimary Filter (Score >= {PRIMARY_SCORE_THRESHOLD} in either lane): {len(primary_passed)} stocks")
print(f"  - Via Breakout lane: {(primary_passed['Score_Breakout'] >= PRIMARY_SCORE_THRESHOLD).sum()} stocks")
print(f"  - Via Reclaim lane only: {((primary_passed['Score_Breakout'] < PRIMARY_SCORE_THRESHOLD) & (primary_passed['Score_Reclaim'] >= PRIMARY_SCORE_THRESHOLD)).sum()} stocks")

# GUARDRAIL: If < 25 stocks pass, relax threshold to >= 40
if len(primary_passed) < MIN_STOCKS_FOR_PRIMARY:
    print(f"\n  ⚠️  Less than {MIN_STOCKS_FOR_PRIMARY} stocks passed primary filter!")
    print(f"  Applying Starvation Guard: Relaxing to Score >= {RELAXED_SCORE_THRESHOLD}")
    
    relaxed_condition = (df_sorted["Score_Breakout"] >= RELAXED_SCORE_THRESHOLD) | \
                        (df_sorted["Score_Reclaim"] >= RELAXED_SCORE_THRESHOLD)
    df_final = df_sorted[relaxed_condition].copy()
    filter_used = f"RELAXED (Score >= {RELAXED_SCORE_THRESHOLD})"
    print(f"  After relaxed filter: {len(df_final)} stocks")
else:
    df_final = primary_passed.copy()
    filter_used = f"PRIMARY (Score >= {PRIMARY_SCORE_THRESHOLD})"
    print(f"\n  Primary filter sufficient, no relaxation needed.")

# Re-sort by BestScore descending
df_final = df_final.sort_values("BestScore", ascending=False).reset_index(drop=True)

# ==============================
# FINAL SUMMARY
# ==============================
print("\n" + "="*60)
print("FINAL SUMMARY - DUAL-LANE PHYSICS SCORING ENGINE v1.6")
print("="*60)
print(f"Total stocks processed: {len(df)}")
print(f"Filter applied: {filter_used}")
print(f"Final stocks for Phase 3: {len(df_final)}")

# Regime breakdown in final output
final_regime = df_final["Regime"].value_counts()
print(f"\nFinal Regime Distribution:")
for regime, count in final_regime.items():
    print(f"  {regime}: {count} stocks")

if len(df_final) > 0:
    print(f"\nTop 10 Stocks by Best Score:")
    display_cols = ["Symbol", "Regime", "Score_Breakout", "Score_Reclaim", "BestScore",
                   "BreakoutGate", "ReclaimGate", "ProximityBonus"]
    # Only show columns that exist
    display_cols = [col for col in display_cols if col in df_final.columns]
    print(df_final[display_cols].head(10).to_string(index=False))

# ==============================
# OUTPUT
# ==============================
OUTPUT_FILE_ALL = os.path.join(OUTPUT_DIR, "phase2_all_results.xlsx")
OUTPUT_FILE_FILTERED = os.path.join(OUTPUT_DIR, "phase2_results.xlsx")

# Save full dataset (all stocks with scores)
df.to_excel(OUTPUT_FILE_ALL, index=False)

# Save filtered dataset (stocks passing Starvation Guard for Phase 3)
df_final.to_excel(OUTPUT_FILE_FILTERED, index=False)

print("\n" + "="*60)
print("Phase-2 Dual-Lane Scoring Completed Successfully!")
print("="*60)
print(f"Full dataset (all stocks):     {OUTPUT_FILE_ALL}")
print(f"Filtered dataset (for Phase 3): {OUTPUT_FILE_FILTERED}")
print("="*60)
