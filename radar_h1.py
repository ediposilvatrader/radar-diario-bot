import os
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from zoneinfo import ZoneInfo
import pandas as pd
import yfinance as yf
import requests

# ======== SECRETS ========
TELEGRAM_TOKEN        = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID_H1   = int(os.environ["TELEGRAM_CHAT_ID_H1"])
TELEGRAM_THREAD_ID_H1 = os.environ.get("TELEGRAM_THREAD_ID_H1")

# ======== DEBUG OPCIONAL ========
DEBUG = os.getenv("DEBUG", "0") == "1"
DEBUG_TICKERS = {t.strip().upper() for t in os.getenv("DEBUG_TICKERS", "").split(",")} - {""}

# ======== PARÂMETROS ========
EMA_FAST = 21
EMA_MID  = 120
SMA_LONG = 200
EPS      = 1e-4  # tolerância ~0,01%
NY  = ZoneInfo("America/New_York")
BRT = datetime.timezone(datetime.timedelta(hours=-3))

TICKERS = [
    "AIG","AMZN","AAPL","AXP","BA","BAC","BKNG","BLK","C","CAT","COST","CSCO","CVX","DAL",
    "DD","EXPE","F","GE","GM","GOOG","GS","HLT","HPQ","IBM","INTC","JNJ","JPM","KO","MA",
    "MCD","META","MNST","MS","MSFT","NFLX","NVDA","ORCL","PEP","PG","PLTR","PM","RCL","SBUX",
    "SPOT","T","TSLA","UBER","V","WFC","WMT","XOM"
]

# ======== HELPERS ========
def ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False, min_periods=length).mean()

def sma(series: pd.Series, length: int) -> pd.Series:
    return series.rolling(window=length, min_periods=length).mean()

def _flatten_df(df: pd.DataFrame, sym: str) -> pd.DataFrame:
    """Garante colunas planas: 'Open','High','Low','Close','Adj Close','Volume'."""
    if isinstance(df.columns, pd.MultiIndex):
        # casos: nível 0 = campo OU ticker. Tentamos droplevel/slice seguro.
        lvl0 = df.columns.get_level_values(0)
        if {"Open","High","Low","Close"}.issubset(set(lvl0)):
            df = df.droplevel(-1, axis=1)
        elif sym in set(lvl0):
            df = df.xs(sym, axis=1, level=0)
    return df

def download_yf(sym: str, interval: str, period: str) -> pd.DataFrame:
    df = yf.download(
        sym,
        interval=interval,
        period=period,
        auto_adjust=True,
        prepost=False,
        progress=False,
        threads=False,
        group_by="column",   # <- força colunas planas
    )
    if df.empty:
        return df
    df = _flatten_df(df, sym)
    # timezone só para logs consistentes
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC").tz_convert(NY)
    else:
        df.index = df.index.tz_convert(NY)
    return df

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    close = pd.to_numeric(df["Close"], errors="coerce")
    df["ema_fast"] = ema(close, EMA_FAST)
    df["ema_mid"]  = ema(close, EMA_MID)
    df["sma_long"] = sma(close, SMA_LONG)
    return df.dropna()

def last_row_with_indicators(sym: str, interval: str, period: str) -> pd.Series | None:
    df = download_yf(sym, interval=interval, period=period)
    if df.empty or "Close" not in df.columns:
        return None
    df = add_indicators(df)
    if df.empty:
        return None
    return df.iloc[-1]

def above_mas(row: pd.Series) -> bool:
    # Usa .item() para evitar FutureWarning do pandas
    c    = float(row.at["Close"])
    e21  = float(row.at["ema_fast"])
    e120 = float(row.at["ema_mid"])
    s200 = float(row.at["sma_long"])
    return (c + EPS >= e21) and (e21 + EPS >= e120) and (e120 + EPS >= s200)

def check_symbol(sym: str) -> tuple[bool, str]:
    h1 = last_row_with_indicators(sym, "60m", "180d")
    if h1 is None:
        return False, "sem_h1"
    h1_ok = above_mas(h1)

    d1 = last_row_with_indicators(sym, "1d", "400d")
    if d1 is None:
        return False, "sem_d1"
    d1_ok = above_mas(d1)

    if DEBUG and (not DEBUG_TICKERS or sym in DEBUG_TICKERS):
        try:
            print(f"[{sym}] H1 {h1.name.strftime('%Y-%m-%d %H:%M NY')}  "
                  f"C={h1['Close']:.3f}  E21={h1['ema_fast']:.3f}  E120={h1['ema_mid']:.3f}  S200={h1['sma_long']:.3f} -> {h1_ok}")
            print(f"[{sym}] D1 {d1.name.strftime('%Y-%m-%d')}         "
                  f"C={d1['Close']:.3f}  E21={d1['ema_fast']:.3f}  E120={d1['ema_mid']:.3f}  S200={d1['sma_long']:.3f} -> {d1_ok}")
        except Exception:
            pass

    return (h1_ok and d1_ok), ("ok" if h1_ok and d1_ok else "filtro")

def send_telegram(text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID_H1,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }
    if TELEGRAM_THREAD_ID_H1:
        payload["message_thread_id"] = int(TELEGRAM_THREAD_ID_H1)
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception:
        pass

def main():
    tickers = TICKERS if not DEBUG_TICKERS else [t for t in TICKERS if t in DEBUG_TICKERS]

    hits, fails = [], []
    with ThreadPoolExecutor(max_workers=min(12, len(tickers))) as ex:
        futs = {ex.submit(check_symbol, t): t for t in tickers}
        for f in as_completed(futs):
            t = futs[f]
            ok, reason = f.result()
            if ok: hits.append(t)
            else:  fails.append((t, reason))

    hits.sort()
    ts_brt = datetime.datetime.now(BRT).strftime("%d/%m/%Y %H:%M")
    msg = (
        f"*🔎 Radar H1/D1 — {ts_brt} (BRT)*\n"
        f"*Critério:* Close ≥ EMA21 ≥ EMA120 ≥ SMA200 (H1 **e** D1)\n\n"
        f"*Passaram:* {', '.join(hits) if hits else '—'}"
    )
    print(msg)
    if DEBUG:
        print("Falhas:", fails)
    send_telegram(msg)

if __name__ == "__main__":
    main()
