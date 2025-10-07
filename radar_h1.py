import os
import datetime
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import yfinance as yf
import requests

# ========= SECRETS =========
TELEGRAM_TOKEN        = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID_H1   = int(os.environ["TELEGRAM_CHAT_ID_H1"])
TELEGRAM_THREAD_ID_H1 = os.environ.get("TELEGRAM_THREAD_ID_H1")

# ========= PARÂMETROS =========
EMA_FAST = 21
EMA_MID  = 120
SMA_LONG = 200

TICKERS = [
    "AIG","AMZN","AAPL","AXP","BA","BAC","BKNG","BLK","C","CAT","COST","CSCO","CVX","DAL",
    "DD","EXPE","F","GE","GM","GOOG","GS","HLT","HPQ","IBM","INTC","JNJ","JPM","KO","MA",
    "MCD","META","MNST","MS","MSFT","NFLX","NVDA","ORCL","PEP","PG","PLTR","PM","RCL","SBUX",
    "SPOT","T","TSLA","UBER","V","WFC","WMT","XOM"
]

NY = ZoneInfo("America/New_York")
BRT = datetime.timezone(datetime.timedelta(hours=-3))

# ========= HELPERS =========
def ema(series: pd.Series, length: int) -> pd.Series:
    # EMA padrão de trading (fórmula recursiva). Equivalente ao TV.
    return series.ewm(span=length, adjust=False, min_periods=length).mean()

def sma(series: pd.Series, length: int) -> pd.Series:
    return series.rolling(window=length, min_periods=length).mean()

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df["ema_fast"] = ema(df["Close"], EMA_FAST)
    df["ema_mid"]  = ema(df["Close"], EMA_MID)
    df["sma_long"] = sma(df["Close"], SMA_LONG)
    return df.dropna()

def prep_ny_session(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    # garantir timezone
    if df.index.tz is None:
        df = df.tz_localize("UTC").tz_convert(NY)
    else:
        df = df.tz_convert(NY)
    # somente pregão regular
    return df.between_time("09:30", "16:00")

def get_last_row(sym: str, interval: str, period: str, intraday: bool) -> pd.Series | None:
    df = yf.download(
        sym,
        interval=interval,
        period=period,
        auto_adjust=True,   # OHLC ajustado (ok para comparação de médias)
        prepost=False,      # sem pre/pós mercado
        progress=False,
        threads=False
    )
    if df.empty:
        return None
    if intraday:
        df = prep_ny_session(df)
        if df.empty:
            return None
    df = add_indicators(df)
    if df.empty:
        return None
    return df.iloc[-1]

def above_mas(row: pd.Series) -> bool:
    # Close > EMA21 > EMA120 > SMA200 (estritamente acima)
    return (
        row["Close"] > row["ema_fast"] > row["ema_mid"] and
        row["ema_mid"] > row["sma_long"]
    )

def check_symbol(sym: str) -> bool:
    # H1 (último candle disponível da sessão regular)
    h1 = get_last_row(sym, interval="60m", period="180d", intraday=True)
    if h1 is None or not above_mas(h1):
        return False
    # D1 (último diário disponível)
    d1 = get_last_row(sym, interval="1d", period="400d", intraday=False)
    if d1 is None or not above_mas(d1):
        return False
    return True

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
    # varredura paralela (rápido)
    hits, errors = [], []
    with ThreadPoolExecutor(max_workers=min(12, len(TICKERS))) as ex:
        futs = {ex.submit(check_symbol, t): t for t in TICKERS}
        for f in as_completed(futs):
            t = futs[f]
            try:
                if f.result():
                    hits.append(t)
            except Exception as e:
                errors.append((t, str(e)))

    hits.sort()
    ts_brt = datetime.datetime.now(BRT).strftime("%d/%m/%Y %H:%M")
    msg = (
        f"*🔎 Radar H1/D1 — {ts_brt} (BRT)*\n"
        f"*Critério:* Close > EMA21 > EMA120 > SMA200 (H1 **e** D1)\n\n"
        f"*Passaram:* {', '.join(hits) if hits else '—'}"
    )
    print(msg)
    if errors:
        print("Erros:", errors)
    send_telegram(msg)

if __name__ == "__main__":
    main()
