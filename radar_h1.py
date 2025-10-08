import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import yfinance as yf
import requests

# ===== SECRETS =====
TELEGRAM_TOKEN        = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID_H1   = int(os.environ["TELEGRAM_CHAT_ID_H1"])
TELEGRAM_THREAD_ID_H1 = os.environ.get("TELEGRAM_THREAD_ID_H1")

# ===== PARÂMETROS =====
EMA_FAST = 21
EMA_MID  = 120
SMA_LONG = 200
# tolerância p/ “encostou na média”
EPS_ABS = 0.02   # US$ 0,02
EPS_REL = 0.001  # 0,10%

TICKERS = [
    "AIG","AMZN","AAPL","AXP","BA","BAC","BKNG","BLK","C","CAT","COST","CSCO","CVX","DAL",
    "DD","EXPE","F","GE","GM","GOOG","GS","HLT","HPQ","IBM","INTC","JNJ","JPM","KO","MA",
    "MCD","META","MNST","MS","MSFT","NFLX","NVDA","ORCL","PEP","PG","PLTR","PM","RCL","SBUX",
    "SPOT","T","TSLA","UBER","V","WFC","WMT","XOM"
]

# ===== FUNÇÕES =====
def ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=n, adjust=False, min_periods=n).mean()

def sma(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(window=n, min_periods=n).mean()

def fetch_last(sym: str, interval: str, period: str) -> pd.Series | None:
    df = yf.Ticker(sym).history(
        period=period, interval=interval,
        auto_adjust=False, prepost=False, actions=False
    )
    if df.empty or "Close" not in df.columns:
        return None
    close = pd.to_numeric(df["Close"], errors="coerce")
    out = pd.DataFrame({
        "Close":    close,
        "ema_fast": ema(close, EMA_FAST),
        "ema_mid":  ema(close, EMA_MID),
        "sma_long": sma(close, SMA_LONG),
    }).dropna()
    return None if out.empty else out.iloc[-1]

def tol_ge(a: float, b: float) -> bool:
    t = max(EPS_ABS, abs(b) * EPS_REL)
    return a + t >= b

def close_above_all(row: pd.Series) -> bool:
    c    = float(row.at["Close"])
    e21  = float(row.at["ema_fast"])
    e120 = float(row.at["ema_mid"])
    s200 = float(row.at["sma_long"])
    return tol_ge(c, e21) and tol_ge(c, e120) and tol_ge(c, s200)

def passes(sym: str) -> bool:
    h1 = fetch_last(sym, "60m", "180d")
    if h1 is None or not close_above_all(h1):
        return False
    d1 = fetch_last(sym, "1d", "400d")
    if d1 is None or not close_above_all(d1):
        return False
    return True

def send_telegram(text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID_H1, "text": text, "disable_web_page_preview": True}
    if TELEGRAM_THREAD_ID_H1:
        payload["message_thread_id"] = int(TELEGRAM_THREAD_ID_H1)
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception:
        pass

def main():
    hits = []
    with ThreadPoolExecutor(max_workers=min(12, len(TICKERS))) as ex:
        futs = {ex.submit(passes, t): t for t in TICKERS}
        for f in as_completed(futs):
            if f.result():
                hits.append(futs[f])
    hits.sort()
    msg = "Radar Pressão H1 - Sinais de Compra\n\nAções: " + (", ".join(hits) if hits else "—")
    send_telegram(msg)

if __name__ == "__main__":
    main()
