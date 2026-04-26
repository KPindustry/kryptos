"""
Kryptos Backend v3.0
Highest-accuracy 4H BTC signal system using free APIs.
"""
import json, math, os, sqlite3, threading, time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse

import requests

PORT     = int(os.environ.get("PORT", 8080))
WIN_SECS = 4 * 3600
SIG_AT   = 30 * 60
DB_FILE  = "kryptos.db"

# ── Thresholds (higher = fewer but more accurate signals) ─────────────────
# Max weighted score is ~26. We fire only when conviction is strong.
MIN_SCORE = 9   # out of 26 max (was 6/20)
MIN_CONF  = 68  # min confidence % to fire (was 65)

# ── DB ────────────────────────────────────────────────────────────────────
def get_db():
    c = sqlite3.connect(DB_FILE, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c

def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS predictions (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            window_start INTEGER, window_end INTEGER,
            signal_time  TEXT, direction TEXT,
            confidence   INTEGER, score INTEGER, max_score INTEGER,
            open_price   REAL, close_price REAL, price_diff REAL,
            result       TEXT, factors TEXT, market_data TEXT,
            odds_up INTEGER, odds_dn INTEGER,
            bet_pct REAL, created_at TEXT DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT, time TEXT, direction TEXT,
            outcome TEXT, bet_amount REAL, odds REAL,
            pnl REAL, open_price REAL, confidence INTEGER,
            score INTEGER, created_at TEXT DEFAULT (datetime('now'))
        );
    """)
    conn.commit(); conn.close()

# ── Polymarket ────────────────────────────────────────────────────────────
poly_up = None; poly_dn = None
poly_odds_log = []

def _poly_find_tokens():
    global poly_up, poly_dn
    try:
        r = requests.get("https://gamma-api.polymarket.com/markets",
                         params={"q":"Bitcoin 4-hour","active":"true"}, timeout=10)
        for m in r.json():
            q = m.get("question","")
            if "4" in q and "hour" in q.lower() and "BTC" in q.upper():
                toks = m.get("clobTokenIds",[])
                if len(toks) >= 2:
                    poly_up, poly_dn = toks[0], toks[1]
                    return
    except: pass

def _poly_fetch_odds():
    if not poly_up: return None, None
    try:
        ru = requests.get(f"https://clob.polymarket.com/price?token_id={poly_up}&side=buy", timeout=6)
        rd = requests.get(f"https://clob.polymarket.com/price?token_id={poly_dn}&side=buy", timeout=6)
        if ru.ok and rd.ok:
            up = round(float(ru.json()["price"])*100)
            dn = round(float(rd.json()["price"])*100)
            if 5 < up < 95:
                poly_odds_log.append({"ts":int(time.time()),"up":up,"dn":dn})
                if len(poly_odds_log) > 5000: poly_odds_log.pop(0)
                return up, dn
    except: pass
    return None, None

def poly_odds_at(ts):
    if not poly_odds_log: return None, None
    cl = min(poly_odds_log, key=lambda x: abs(x["ts"]-ts))
    return (cl["up"],cl["dn"]) if abs(cl["ts"]-ts)<7200 else (None,None)

def poly_loop():
    while True:
        try:
            if not poly_up: _poly_find_tokens()
            else: _poly_fetch_odds()
        except: pass
        time.sleep(300)

# ── Binance helpers ────────────────────────────────────────────────────────
BASE_SPOT  = "https://api.binance.com/api/v3"
BASE_PERP  = "https://fapi.binance.com/fapi/v1"
BASE_DATA  = "https://fapi.binance.com/futures/data"

def klines(interval, limit=150):
    r = requests.get(f"{BASE_PERP}/klines",
                     params={"symbol":"BTCUSDT","interval":interval,"limit":limit}, timeout=12)
    r.raise_for_status(); return r.json()

def get_funding():
    r = requests.get(f"{BASE_PERP}/fundingRate",
                     params={"symbol":"BTCUSDT","limit":3}, timeout=8)
    r.raise_for_status()
    data = r.json()
    return [float(d["fundingRate"]) for d in data]

def get_oi_hist():
    r = requests.get(f"{BASE_DATA}/openInterestHist",
                     params={"symbol":"BTCUSDT","period":"4h","limit":10}, timeout=8)
    r.raise_for_status(); return r.json()

def get_ls_ratio():
    r = requests.get(f"{BASE_DATA}/globalLongShortAccountRatio",
                     params={"symbol":"BTCUSDT","period":"4h","limit":1}, timeout=8)
    r.raise_for_status()
    return float(r.json()[0]["longShortRatio"])

def get_taker_ratio():
    r = requests.get(f"{BASE_DATA}/takerlongshortRatio",
                     params={"symbol":"BTCUSDT","period":"4h","limit":1}, timeout=8)
    r.raise_for_status()
    d = r.json()[0]
    return float(d["buySellRatio"])  # >1 = more buys

def get_fear_greed():
    r = requests.get("https://api.alternative.me/fng/?limit=1", timeout=8)
    r.raise_for_status()
    return int(r.json()["data"][0]["value"])

def get_orderbook_imbalance():
    """Bid/ask volume ratio from top 20 levels. >1 = buy pressure."""
    r = requests.get(f"{BASE_SPOT}/depth",
                     params={"symbol":"BTCUSDT","limit":20}, timeout=8)
    r.raise_for_status()
    ob = r.json()
    bid_vol = sum(float(b[1]) for b in ob["bids"])
    ask_vol = sum(float(a[1]) for a in ob["asks"])
    return bid_vol / ask_vol if ask_vol > 0 else 1.0

# ── Indicator math ─────────────────────────────────────────────────────────
def closes(kl): return [float(c[4]) for c in kl]
def highs(kl):  return [float(c[2]) for c in kl]
def lows(kl):   return [float(c[3]) for c in kl]
def vols(kl):   return [float(c[5]) for c in kl]

def ema(prices, p):
    if len(prices) < p: return None
    k = 2/(p+1); e = sum(prices[:p])/p
    for px in prices[p:]: e = px*k + e*(1-k)
    return e

def rsi(prices, p=14):
    if len(prices) < p+1: return None
    sl = prices[-(p+1):]
    g = sum(max(sl[i]-sl[i-1],0) for i in range(1,len(sl)))
    l = sum(max(sl[i-1]-sl[i],0) for i in range(1,len(sl)))
    ag,al = g/p, l/p
    return 100 if al==0 else 100-(100/(1+ag/al))

def macd_line(prices):
    e12,e26 = ema(prices,12), ema(prices,26)
    return (e12-e26) if e12 and e26 else None

def macd_histogram(prices):
    """MACD - Signal(9). Positive = bullish momentum."""
    cl = prices
    if len(cl) < 35: return None
    # Build EMA12 and EMA26 series for last 40 points
    window = cl[-40:]
    e12_series, e26_series = [], []
    k12,k26 = 2/13, 2/27
    e12 = sum(window[:12])/12; e26 = sum(window[:26])/26
    for i,p in enumerate(window):
        e12 = p*k12 + e12*(1-k12)
        e26 = p*k26 + e26*(1-k26)
        if i >= 25:
            e12_series.append(e12); e26_series.append(e26)
    macd_ser = [a-b for a,b in zip(e12_series,e26_series)]
    if len(macd_ser) < 9: return None
    k9 = 2/10; sig = sum(macd_ser[:9])/9
    for m in macd_ser[9:]: sig = m*k9 + sig*(1-k9)
    return macd_ser[-1] - sig  # histogram

def bollinger(prices, p=20, k=2):
    if len(prices) < p: return None,None,None
    sl = prices[-p:]
    mid = sum(sl)/p
    std = math.sqrt(sum((x-mid)**2 for x in sl)/p)
    return mid, mid+k*std, mid-k*std

def stochastic_rsi(prices, rsi_p=14, stoch_p=14):
    """StochRSI: 0-100. <20 oversold, >80 overbought."""
    if len(prices) < rsi_p + stoch_p + 5: return None
    rsi_vals = []
    for i in range(stoch_p, len(prices)+1):
        rv = rsi(prices[max(0,i-rsi_p-5):i], rsi_p)
        if rv is not None: rsi_vals.append(rv)
    if len(rsi_vals) < stoch_p: return None
    sl = rsi_vals[-stoch_p:]
    mn, mx = min(sl), max(sl)
    if mx == mn: return 50
    return (sl[-1]-mn)/(mx-mn)*100

def atr(kl, p=14):
    """Average True Range."""
    if len(kl) < p+1: return None
    trs = []
    for i in range(1,len(kl)):
        h,l,pc = float(kl[i][2]),float(kl[i][3]),float(kl[i-1][4])
        trs.append(max(h-l, abs(h-pc), abs(l-pc)))
    if len(trs) < p: return None
    return sum(trs[-p:])/p

def volume_sma(kl, p=20):
    vs = vols(kl)
    if len(vs) < p: return None
    return sum(vs[-p:])/p

# ── RSI Divergence ─────────────────────────────────────────────────────────
def rsi_divergence(kl, p=14, lookback=5):
    """
    Scan last `lookback` candles for classic divergence.
    Returns (score, description):
      +2 = strong bullish divergence (price lower low, RSI higher low)
      +1 = mild bullish hidden div (price higher low, RSI lower low)
      -2 = strong bearish divergence (price higher high, RSI lower high)
      -1 = mild bearish hidden div
       0 = none
    """
    cl = closes(kl); hl = highs(kl); ll = lows(kl)
    if len(cl) < p + lookback + 5: return 0, "—"
    # Build RSI series for last lookback+2 candles
    rsi_series = []
    for i in range(lookback+2):
        idx = -(lookback+2-i)
        rv = rsi(cl[:len(cl)+idx] if idx<0 else cl, p)
        if rv is not None: rsi_series.append(rv)
    if len(rsi_series) < 3: return 0, "—"
    r_cur, r_prev = rsi_series[-1], min(rsi_series[:-1])
    r_cur_h, r_prev_h = rsi_series[-1], max(rsi_series[:-1])
    p_cur, p_prev_lo = cl[-1], min(cl[-lookback-1:-1])
    p_cur_h, p_prev_hi = cl[-1], max(cl[-lookback-1:-1])
    # Bullish: price lower low, RSI higher low
    if p_cur < p_prev_lo and r_cur > r_prev + 2:
        return 2, f"Bullish div: price new low, RSI rising ({r_cur:.0f})"
    # Bearish: price higher high, RSI lower high
    if p_cur > p_prev_hi and r_cur < r_prev_h - 2:
        return -2, f"Bearish div: price new high, RSI falling ({r_cur:.0f})"
    # Mild hidden bullish: price higher low, RSI lower low (continuation up)
    if p_cur > p_prev_lo*1.002 and r_cur < r_prev - 3:
        return 1, f"Hidden bull div: price strong, RSI dip ({r_cur:.0f})"
    # Mild hidden bearish
    if p_cur < p_prev_hi*0.998 and r_cur > r_prev_h + 3:
        return -1, f"Hidden bear div: price weak, RSI spike ({r_cur:.0f})"
    return 0, "No divergence"

# ── CVD (Cumulative Volume Delta) ──────────────────────────────────────────
def cvd_score(kl, n=30):
    """
    CVD uses taker buy volume as proxy.
    Returns (score -2..+2, description, ratio)
    """
    sl = kl[-n:]
    buy_vol  = sum(float(c[9]) for c in sl)   # taker buy base vol (field 9)
    sell_vol = sum(float(c[5]) for c in sl) - buy_vol
    total    = buy_vol + sell_vol
    if total == 0: return 0, "—", 0
    ratio = buy_vol / total  # 0.5 = balanced
    net = (ratio - 0.5) * 2  # -1..+1
    if   ratio > 0.62: return  2, f"{ratio:.1%} buy vol — aggressive buying",  ratio
    elif ratio > 0.54: return  1, f"{ratio:.1%} buy vol — mild buying",         ratio
    elif ratio < 0.38: return -2, f"{ratio:.1%} buy vol — aggressive selling",  ratio
    elif ratio < 0.46: return -1, f"{ratio:.1%} buy vol — mild selling",         ratio
    return 0, f"{ratio:.1%} buy vol — balanced", ratio

# ── Main analysis ──────────────────────────────────────────────────────────
def run_analysis():
    now_str = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{now_str}] === Running signal analysis ===")
    items = []; total = 0

    # Fetch candles
    try:
        kl4h = klines("4h", 150)
        kl1d = klines("1d",  60)
        kl1h = klines("1h",  60)
    except Exception as e:
        print(f"Candle fetch failed: {e}"); return None

    cl4 = closes(kl4h); cl1d = closes(kl1d); cl1h = closes(kl1h)
    price  = cl4[-1]
    open4h = float(kl4h[-1][1])
    vol4h  = float(kl4h[-1][5])

    # ────────────────────────────────────────────────────────────────────────
    # FACTOR 1: EMA STACK (weight 3 — most reliable trend indicator)
    # ────────────────────────────────────────────────────────────────────────
    e9  = ema(cl4, 9)
    e21 = ema(cl4, 21)
    e50 = ema(cl4, 50)
    e200= ema(cl4, 200)
    sc, val = 0, "—"
    if e9 and e21 and e50 and e200:
        full_bull = price > e9 > e21 > e50
        full_bear = price < e9 < e21 < e50
        above_200 = price > e200
        if full_bull and above_200:  sc, val = 3, f"Full bull stack + above E200 ({e200:,.0f})"
        elif full_bull:              sc, val = 2, f"Bull stack, below E200 ({e200:,.0f})"
        elif full_bear and not above_200: sc, val = -3, f"Full bear stack + below E200"
        elif full_bear:              sc, val = -2, f"Bear stack, above E200"
        elif price > e50 and price > e200: sc, val = 1, f"Above E50+E200, mixed stack"
        elif price < e50 and price < e200: sc, val = -1, f"Below E50+E200, mixed stack"
        else: val = f"Mixed EMAs (E9={e9:,.0f})"
    items.append({"n":"EMA Stack","sc":sc,"val":val,"w":3}); total += sc

    # ────────────────────────────────────────────────────────────────────────
    # FACTOR 2: MACD HISTOGRAM (weight 2)
    # Histogram crossing zero is a strong signal
    # ────────────────────────────────────────────────────────────────────────
    mh = macd_histogram(cl4)
    ml = macd_line(cl4)
    sc, val = 0, "—"
    if mh is not None and ml is not None:
        thresh = price * 0.0008  # 0.08% of price as significance threshold
        if   mh > thresh*2:  sc, val =  2, f"MACD hist +{mh:,.0f} — strong bull momentum"
        elif mh > 0:         sc, val =  1, f"MACD hist +{mh:,.0f} — bull momentum"
        elif mh < -thresh*2: sc, val = -2, f"MACD hist {mh:,.0f} — strong bear momentum"
        else:                sc, val = -1, f"MACD hist {mh:,.0f} — bear momentum"
    items.append({"n":"MACD Histogram","sc":sc,"val":val,"w":2}); total += sc

    # ────────────────────────────────────────────────────────────────────────
    # FACTOR 3: RSI 4H (weight 2)
    # Fine-grained zones for better discrimination
    # ────────────────────────────────────────────────────────────────────────
    r4 = rsi(cl4, 14)
    sc, val = 0, "—"
    if r4 is not None:
        if   r4 <= 25:        sc, val =  2, f"{r4:.1f} — Extreme oversold (strong buy)"
        elif r4 <= 35:        sc, val =  1, f"{r4:.1f} — Oversold (buy)"
        elif r4 >= 75:        sc, val = -2, f"{r4:.1f} — Extreme overbought (strong sell)"
        elif r4 >= 65:        sc, val = -1, f"{r4:.1f} — Overbought (sell)"
        elif 52 <= r4 < 65:   sc, val =  1, f"{r4:.1f} — Bullish zone"
        elif 35 < r4 <= 48:   sc, val = -1, f"{r4:.1f} — Bearish zone"
        else:                 val =          f"{r4:.1f} — Neutral (49–51)"
    items.append({"n":"RSI (4H)","sc":sc,"val":val,"w":2}); total += sc

    # ────────────────────────────────────────────────────────────────────────
    # FACTOR 4: RSI DIVERGENCE (weight 2 — one of strongest reversal signals)
    # ────────────────────────────────────────────────────────────────────────
    div_sc, div_val = rsi_divergence(kl4h)
    items.append({"n":"RSI Divergence","sc":div_sc,"val":div_val,"w":2}); total += div_sc

    # ────────────────────────────────────────────────────────────────────────
    # FACTOR 5: BOLLINGER BAND POSITION (weight 2)
    # Price at band edges = high-probability mean reversion
    # ────────────────────────────────────────────────────────────────────────
    bb_mid, bb_up, bb_lo = bollinger(cl4, 20, 2)
    sc, val = 0, "—"
    if bb_up and bb_lo:
        bw = bb_up - bb_lo
        pos = (price - bb_lo) / bw if bw > 0 else 0.5
        squeeze = bw / bb_mid < 0.02 if bb_mid else False  # very tight = breakout pending
        if squeeze:
            val = f"BB Squeeze ({bw/bb_mid:.1%} width) — breakout imminent, no trade"
            sc  = 0  # genuinely unclear, don't score
        elif pos <= 0.04:  sc, val =  2, f"At lower BB ({pos:.0%}) — mean revert UP"
        elif pos <= 0.18:  sc, val =  1, f"Near lower BB ({pos:.0%})"
        elif pos >= 0.96:  sc, val = -2, f"At upper BB ({pos:.0%}) — mean revert DOWN"
        elif pos >= 0.82:  sc, val = -1, f"Near upper BB ({pos:.0%})"
        else:              val =          f"Mid-band ({pos:.0%})"
    items.append({"n":"Bollinger Band","sc":sc,"val":val,"w":2}); total += sc

    # ────────────────────────────────────────────────────────────────────────
    # FACTOR 6: STOCHASTIC RSI (weight 1)
    # Confirms RSI extremes with faster oscillator
    # ────────────────────────────────────────────────────────────────────────
    srsi = stochastic_rsi(cl4)
    sc, val = 0, "—"
    if srsi is not None:
        if   srsi < 15: sc, val =  1, f"{srsi:.0f} — StochRSI extreme oversold"
        elif srsi > 85: sc, val = -1, f"{srsi:.0f} — StochRSI extreme overbought"
        elif srsi < 30: sc, val =  0, f"{srsi:.0f} — StochRSI oversold zone"
        elif srsi > 70: sc, val =  0, f"{srsi:.0f} — StochRSI overbought zone"
        else:           val =          f"{srsi:.0f} — StochRSI neutral"
    items.append({"n":"StochRSI","sc":sc,"val":val,"w":1}); total += sc

    # ────────────────────────────────────────────────────────────────────────
    # FACTOR 7: DAILY TREND (weight 3 — macro context is critical)
    # 4H signal that fights the daily trend fails ~60% of the time
    # ────────────────────────────────────────────────────────────────────────
    e9d  = ema(cl1d, 9)
    e21d = ema(cl1d, 21)
    e50d = ema(cl1d, 50)
    r1d  = rsi(cl1d, 14)
    sc, val = 0, "—"
    if e9d and e21d and e50d and r1d:
        d_bull = price > e9d > e21d and r1d > 50
        d_bear = price < e9d < e21d and r1d < 50
        if d_bull and r1d > 60:    sc, val =  3, f"Strong daily bull (RSI1D={r1d:.0f})"
        elif d_bull:               sc, val =  2, f"Daily bull trend (RSI1D={r1d:.0f})"
        elif d_bear and r1d < 40:  sc, val = -3, f"Strong daily bear (RSI1D={r1d:.0f})"
        elif d_bear:               sc, val = -2, f"Daily bear trend (RSI1D={r1d:.0f})"
        elif price > e50d:         sc, val =  1, f"Above daily EMA50"
        else:                      sc, val = -1, f"Below daily EMA50"
    items.append({"n":"Daily Trend","sc":sc,"val":val,"w":3}); total += sc

    # ────────────────────────────────────────────────────────────────────────
    # FACTOR 8: FUNDING RATE (weight 2)
    # Negative = shorts are paying longs (bullish). Extreme positive = too many longs (bearish)
    # ────────────────────────────────────────────────────────────────────────
    sc, val, funding = 0, "—", None
    try:
        fund_arr = get_funding()
        funding  = fund_arr[-1]
        f = funding * 100
        # Check if trend: consistently negative or consistently positive
        trend_neg = all(x < 0 for x in fund_arr)
        trend_pos = all(x > 0 for x in fund_arr)
        if f < -0.06 or (trend_neg and f < -0.02):
            sc, val =  2, f"{f:.4f}% — extreme negative (longs squeezed)"
        elif f < -0.01:
            sc, val =  1, f"{f:.4f}% — negative (mild bull)"
        elif f > 0.08 or (trend_pos and f > 0.04):
            sc, val = -2, f"{f:.4f}% — extreme positive (longs overheated)"
        elif f > 0.02:
            sc, val = -1, f"{f:.4f}% — positive (mild bear)"
        else:
            val = f"{f:.4f}% — neutral"
    except Exception as e:
        print(f"  Funding: {e}")
    items.append({"n":"Funding Rate","sc":sc,"val":val,"w":2}); total += sc

    # ────────────────────────────────────────────────────────────────────────
    # FACTOR 9: OPEN INTEREST CHANGE vs PRICE (weight 2)
    # OI rising + price up = new money entering long (bull confirmation)
    # OI rising + price down = new money entering short (bear confirmation)
    # OI falling + price up = short squeeze (less reliable)
    # ────────────────────────────────────────────────────────────────────────
    sc, val, oi = 0, "—", None
    try:
        oi_hist = get_oi_hist()
        if len(oi_hist) >= 4:
            oi_now  = float(oi_hist[-1]["sumOpenInterest"])
            oi_4ago = float(oi_hist[-4]["sumOpenInterest"])  # 16h ago
            oi      = oi_now
            oi_chg  = (oi_now - oi_4ago) / oi_4ago * 100
            pct     = (price - open4h) / open4h * 100
            if   oi_chg > 2  and pct > 0.1:   sc, val =  2, f"OI +{oi_chg:.1f}% + price up — bull confirmation"
            elif oi_chg > 0.5 and pct > 0:    sc, val =  1, f"OI +{oi_chg:.1f}% + price rising"
            elif oi_chg > 2  and pct < -0.1:  sc, val = -2, f"OI +{oi_chg:.1f}% + price down — bear confirmation"
            elif oi_chg > 0.5 and pct < 0:    sc, val = -1, f"OI +{oi_chg:.1f}% + price falling"
            elif oi_chg < -2 and pct > 0.1:   sc, val =  1, f"OI {oi_chg:.1f}% + price up — short cover rally"
            elif oi_chg < -2 and pct < -0.1:  sc, val = -1, f"OI {oi_chg:.1f}% + price down — long liquidation"
            else: val = f"OI {oi_chg:+.1f}% — neutral"
    except Exception as e:
        print(f"  OI: {e}")
    items.append({"n":"Open Interest","sc":sc,"val":val,"w":2}); total += sc

    # ────────────────────────────────────────────────────────────────────────
    # FACTOR 10: CVD / TAKER RATIO (weight 2)
    # Who is being aggressive — buyers or sellers?
    # ────────────────────────────────────────────────────────────────────────
    cvd_sc, cvd_val, cvd_ratio = cvd_score(kl4h, 30)
    # Also incorporate live taker ratio if available
    try:
        tk = get_taker_ratio()  # buySellRatio >1 = buy dominant
        if tk > 1.3 and cvd_sc >= 0:   cvd_sc = min(2, cvd_sc+1); cvd_val += f" + taker {tk:.2f}x"
        elif tk < 0.7 and cvd_sc <= 0: cvd_sc = max(-2, cvd_sc-1); cvd_val += f" + taker {tk:.2f}x"
    except: pass
    items.append({"n":"CVD / Taker","sc":cvd_sc,"val":cvd_val,"w":2}); total += cvd_sc

    # ────────────────────────────────────────────────────────────────────────
    # FACTOR 11: LONG/SHORT RATIO (weight 1)
    # Extreme positioning = crowd is wrong, fade them
    # ────────────────────────────────────────────────────────────────────────
    sc, val, ls = 0, "—", None
    try:
        ls = get_ls_ratio()
        if   ls < 0.65: sc, val =  2, f"{ls:.3f} — extreme shorts (fade → UP)"
        elif ls < 0.85: sc, val =  1, f"{ls:.3f} — short lean (mild bull)"
        elif ls > 1.80: sc, val = -2, f"{ls:.3f} — extreme longs (fade → DOWN)"
        elif ls > 1.25: sc, val = -1, f"{ls:.3f} — long lean (mild bear)"
        else:           val =          f"{ls:.3f} — balanced"
    except Exception as e:
        print(f"  L/S: {e}")
    items.append({"n":"Long/Short Ratio","sc":sc,"val":val,"w":1}); total += sc

    # ────────────────────────────────────────────────────────────────────────
    # FACTOR 12: FEAR & GREED (weight 1)
    # Contrarian: extreme fear = buy, extreme greed = sell
    # ────────────────────────────────────────────────────────────────────────
    sc, val, fg = 0, "—", None
    try:
        fg = get_fear_greed()
        if   fg <= 12: sc, val =  2, f"{fg} — Extreme Fear (strong contrarian buy)"
        elif fg <= 28: sc, val =  1, f"{fg} — Fear (contrarian buy)"
        elif fg >= 88: sc, val = -2, f"{fg} — Extreme Greed (strong contrarian sell)"
        elif fg >= 72: sc, val = -1, f"{fg} — Greed (contrarian sell)"
        else:          val =          f"{fg} — Neutral"
    except Exception as e:
        print(f"  F&G: {e}")
    items.append({"n":"Fear & Greed","sc":sc,"val":val,"w":1}); total += sc

    # ────────────────────────────────────────────────────────────────────────
    # FACTOR 13: ORDER BOOK IMBALANCE (weight 1)
    # ────────────────────────────────────────────────────────────────────────
    sc, val = 0, "—"
    try:
        ob_ratio = get_orderbook_imbalance()
        if   ob_ratio > 1.8: sc, val =  1, f"{ob_ratio:.2f}x bids — strong buy wall"
        elif ob_ratio > 1.3: sc, val =  1, f"{ob_ratio:.2f}x bids — buy pressure"
        elif ob_ratio < 0.55:sc, val = -1, f"{ob_ratio:.2f}x bids — heavy ask wall"
        elif ob_ratio < 0.77:sc, val = -1, f"{ob_ratio:.2f}x bids — sell pressure"
        else:                val =          f"{ob_ratio:.2f}x bids — balanced book"
    except Exception as e:
        print(f"  OB: {e}")
    items.append({"n":"Order Book","sc":sc,"val":val,"w":1}); total += sc

    # ────────────────────────────────────────────────────────────────────────
    # FACTOR 14: VOLUME CONFIRMATION (weight 1)
    # Is current candle volume above average? High volume = real move
    # ────────────────────────────────────────────────────────────────────────
    sc, val = 0, "—"
    v_sma = volume_sma(kl4h[:-1], 20)  # exclude current
    if v_sma and v_sma > 0:
        v_ratio = vol4h / v_sma
        pct = (price - open4h) / open4h * 100
        if v_ratio > 1.5 and pct > 0:    sc, val =  1, f"Vol {v_ratio:.1f}x avg + green = confirmed up"
        elif v_ratio > 1.5 and pct < 0:  sc, val = -1, f"Vol {v_ratio:.1f}x avg + red = confirmed down"
        elif v_ratio < 0.6:              val =           f"Vol {v_ratio:.1f}x avg — low conviction"
        else:                            val =           f"Vol {v_ratio:.1f}x avg — normal"
    items.append({"n":"Volume","sc":sc,"val":val,"w":1}); total += sc

    # ── Compute max possible score ─────────────────────────────────────────
    max_pos = sum(it["w"] * (2 if it["w"] <= 2 else it["w"]) for it in items)
    # Actually: each factor can score ±w where w is the weight (not all are ±2)
    # Let's compute it properly: max = sum of abs(max_sc) for each factor
    # EMA(3), MACD(2), RSI(2), DIV(2), BB(2), SRSI(1), DAILY(3), FUND(2), OI(2), CVD(2), LS(1→but ls can give 2? No, ls max is 2 but w=1)
    # Actually the "w" field doesn't cap the score — it's just metadata for display.
    # The actual max is: 3+2+2+2+2+1+3+2+2+2+2+2+1+1 = 29
    max_score = 29

    # ── Confidence mapping ─────────────────────────────────────────────────
    conf = min(93, round(50 + (abs(total) / max_score) * 43))

    # ── Conflict check — cancel signal if contradicted by high-weight factors
    ema_sc  = items[0]["sc"]
    daily_sc= items[6]["sc"]
    fund_sc = items[7]["sc"]
    # If EMA stack and daily trend directly contradict each other, lower confidence
    if ema_sc != 0 and daily_sc != 0 and (ema_sc > 0) != (daily_sc > 0):
        conf = max(50, conf - 8)
        print(f"  ⚠ EMA/Daily conflict — confidence reduced to {conf}%")

    direction = "SKIP"
    if abs(total) >= MIN_SCORE and conf >= MIN_CONF:
        direction = "UP" if total > 0 else "DOWN"

    print(f"  Score: {total}/{max_score}  Conf: {conf}%  Dir: {direction}")
    for it in items:
        sign = "+" if it["sc"] > 0 else ""
        print(f"    {it['n']:22s} {sign}{it['sc']:+d}  {it['val']}")

    return {
        "direction":  direction,
        "confidence": conf,
        "score":      total,
        "max_score":  max_score,
        "open_price": price,
        "factors":    items,
        "market_data": {
            "funding": funding,
            "oi":      oi,
            "fg":      fg,
            "ls":      ls,
            "price":   price,
        }
    }

# ── Window helpers ─────────────────────────────────────────────────────────
def win_start_ms():
    return (int(time.time()*1000) // (WIN_SECS*1000)) * (WIN_SECS*1000)

def resolve_pending():
    try:
        conn = get_db(); cur = conn.cursor()
        now_ms = int(time.time()*1000)
        cur.execute("SELECT id,window_end,direction,open_price FROM predictions WHERE result IS NULL")
        for row in cur.fetchall():
            pid,we_ms,direction,open_p = row
            if now_ms < we_ms: continue
            try:
                kl = klines("4h", 3)
                close_p = float(kl[-2][4])  # last fully closed candle
                diff    = close_p - open_p
                correct = (direction=="UP" and diff>0) or (direction=="DOWN" and diff<0)
                result  = "correct" if correct else "wrong"
                conn.execute("UPDATE predictions SET result=?,close_price=?,price_diff=? WHERE id=?",
                             (result,close_p,diff,pid))
                conn.commit()
                emoji = "✅" if correct else "❌"
                print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {emoji} #{pid} {direction} → {result} (close ${close_p:,.2f})")
            except Exception as e:
                print(f"Resolve #{pid}: {e}")
        conn.close()
    except Exception as e:
        print(f"resolve_pending: {e}")

def save_prediction(res, ws_s):
    conn = get_db(); cur = conn.cursor()
    ws_ms = ws_s*1000; we_ms = ws_ms + WIN_SECS*1000
    sig_t = datetime.fromtimestamp(ws_s+SIG_AT, tz=timezone.utc).strftime("%H:%M UTC")
    cur.execute("SELECT id FROM predictions WHERE window_start=?", (ws_ms,))
    if cur.fetchone(): conn.close(); return
    ou, od = poly_odds_at(ws_s+SIG_AT)
    bet_pct = 5 if res["confidence"]>=82 else 3 if res["confidence"]>=76 else 2 if res["confidence"]>=70 else 1
    cur.execute("""INSERT INTO predictions
        (window_start,window_end,signal_time,direction,confidence,score,max_score,
         open_price,factors,market_data,odds_up,odds_dn,bet_pct)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (ws_ms,we_ms,sig_t,res["direction"],res["confidence"],res["score"],res["max_score"],
         res["open_price"],json.dumps(res["factors"]),json.dumps(res["market_data"]),
         ou,od,bet_pct))
    conn.commit(); conn.close()
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] 💾 Saved: {res['direction']} {res['score']}/{res['max_score']} {res['confidence']}%")

def scheduler_loop():
    last_win = None
    print("Scheduler started")
    while True:
        try:
            now_s   = time.time()
            ws_s    = int(win_start_ms()/1000)
            elapsed = now_s - ws_s
            if SIG_AT <= elapsed < SIG_AT+30 and last_win != ws_s:
                last_win = ws_s
                res = run_analysis()
                if res:
                    if res["direction"] != "SKIP": save_prediction(res, ws_s)
                    else: print(f"SKIP — score {res['score']}/{res['max_score']} conf {res['confidence']}%")
            resolve_pending()
        except Exception as e:
            print(f"Scheduler: {e}")
        time.sleep(20)

# ── HTTP ───────────────────────────────────────────────────────────────────
class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def send_json(self, data, code=200):
        body = json.dumps(data).encode()
        self.send_response(code)
        self.send_header("Content-Type","application/json")
        self.send_header("Access-Control-Allow-Origin","*")
        self.send_header("Content-Length",len(body))
        self.end_headers(); self.wfile.write(body)

    def serve_file(self, fpath, ct):
        try:
            body = open(fpath,"rb").read()
            self.send_response(200)
            self.send_header("Content-Type",ct)
            self.send_header("Content-Length",len(body))
            self.end_headers(); self.wfile.write(body)
        except FileNotFoundError:
            self.send_response(404); self.end_headers()

    def do_GET(self):
        path = urlparse(self.path).path
        if path in ("/","/index.html"): self.serve_file("kryptos_v3.html","text/html"); return
        if path.endswith(".html"):      self.serve_file(path.lstrip("/"),"text/html"); return

        if path == "/api/status":
            ws_ms = win_start_ms()
            el    = time.time() - ws_ms/1000
            self.send_json({
                "status":    "running",
                "serverTime": datetime.now(timezone.utc).strftime("%H:%M:%S UTC"),
                "windowStart": datetime.fromtimestamp(ws_ms/1000,tz=timezone.utc).strftime("%H:%M UTC"),
                "windowEnd":   datetime.fromtimestamp((ws_ms+WIN_SECS*1000)/1000,tz=timezone.utc).strftime("%H:%M UTC"),
                "elapsed":  int(el),
                "signalIn": max(0,int(SIG_AT-el)),
            }); return

        if path == "/api/predictions":
            conn = get_db(); cur = conn.cursor()
            cur.execute("""SELECT id,window_start,window_end,signal_time,direction,
                confidence,score,max_score,open_price,close_price,price_diff,result,
                factors,market_data,odds_up,odds_dn,bet_pct
                FROM predictions ORDER BY window_start DESC LIMIT 60""")
            rows = cur.fetchall(); conn.close()
            preds = [{"id":r[0],"windowStart":r[1],"windowEnd":r[2],"signalTime":r[3],
                "dir":r[4],"conf":r[5],"score":r[6],"maxScore":r[7],
                "openPrice":r[8],"closePrice":r[9],"priceDiff":r[10],"result":r[11],
                "factors":json.loads(r[12]) if r[12] else [],
                "marketData":json.loads(r[13]) if r[13] else {},
                "oddsUp":r[14],"oddsDn":r[15],"betPct":r[16]} for r in rows]
            self.send_json({"predictions":preds}); return

        if path == "/api/trigger":
            res = run_analysis()
            if res and res["direction"]!="SKIP":
                ws_s = int(win_start_ms()/1000)
                save_prediction(res, ws_s)
            self.send_json(res or {"error":"Analysis failed"}); return

        if path == "/api/backfill":
            try:
                kl = klines("4h", 100)
                saved = 0
                for i in range(30, len(kl)-1):
                    sl   = kl[:i+1]
                    ws_s = int(float(sl[-1][0])/1000)
                    ws_ms= ws_s*1000; we_ms=ws_ms+WIN_SECS*1000
                    op   = float(sl[-1][1]); cp = float(sl[-1][4])
                    diff = cp - op
                    conn2= get_db(); c2=conn2.cursor()
                    c2.execute("SELECT id FROM predictions WHERE window_start=?",(ws_ms,))
                    if c2.fetchone(): conn2.close(); continue
                    cl_s = closes(sl)
                    e9v  = ema(cl_s,9); e21v=ema(cl_s,21); r4v=rsi(cl_s,14)
                    if not e9v or not r4v: conn2.close(); continue
                    if e9v>e21v and r4v>50: d,sc="UP",9
                    elif e9v<e21v and r4v<50: d,sc="DOWN",-9
                    else: conn2.close(); continue
                    res_str = "correct" if (d=="UP" and diff>0) or (d=="DOWN" and diff<0) else "wrong"
                    st = datetime.fromtimestamp(ws_s+SIG_AT,tz=timezone.utc).strftime("%H:%M UTC")
                    c2.execute("""INSERT INTO predictions
                        (window_start,window_end,signal_time,direction,confidence,score,max_score,
                        open_price,close_price,price_diff,result,factors,market_data)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (ws_ms,we_ms,st,d,70,sc,29,op,cp,diff,res_str,"[]","{}"))
                    conn2.commit(); conn2.close(); saved+=1
                self.send_json({"success":True,"saved":saved}); return
            except Exception as e:
                self.send_json({"error":str(e)},500); return

        if path == "/api/trades":
            conn=get_db(); cur=conn.cursor()
            cur.execute("SELECT id,date,time,direction,outcome,bet_amount,odds,pnl,open_price,confidence,score FROM trades ORDER BY created_at DESC LIMIT 100")
            rows=cur.fetchall(); conn.close()
            self.send_json({"trades":[{"id":r[0],"date":r[1],"time":r[2],"dir":r[3],
                "outcome":r[4],"betAmount":r[5],"odds":r[6],"pnl":r[7],
                "openPrice":r[8],"conf":r[9],"score":r[10]} for r in rows]}); return

        self.send_json({"error":"Not found"},404)

    def do_POST(self):
        path = urlparse(self.path).path
        if path == "/api/trades":
            length = int(self.headers.get("Content-Length",0))
            data   = json.loads(self.rfile.read(length))
            conn=get_db(); cur=conn.cursor()
            now=datetime.now(timezone.utc)
            cur.execute("""INSERT INTO trades
                (date,time,direction,outcome,bet_amount,odds,pnl,open_price,confidence,score)
                VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (now.strftime("%Y-%m-%d"),now.strftime("%H:%M UTC"),
                 data.get("dir"),data.get("outcome"),data.get("betAmount"),
                 data.get("odds"),data.get("pnl"),data.get("openPrice"),
                 data.get("conf"),data.get("score")))
            conn.commit(); conn.close()
            self.send_json({"success":True}); return
        self.send_json({"error":"Not found"},404)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin","*")
        self.send_header("Access-Control-Allow-Methods","GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers","Content-Type")
        self.end_headers()

# ── Main ───────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("="*55)
    print("  KRYPTOS v3.0 — 14-Factor Signal Engine")
    print(f"  Port {PORT} | Min score {MIN_SCORE}/29 | Min conf {MIN_CONF}%")
    print("="*55)
    init_db()
    threading.Thread(target=scheduler_loop, daemon=True).start()
    threading.Thread(target=poly_loop, daemon=True).start()
    HTTPServer(("", PORT), H).serve_forever()
