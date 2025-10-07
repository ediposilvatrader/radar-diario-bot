import os
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import yfinance as yf
import requests

# ======== SECRETS ========
TELEGRAM_TOKEN        = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID_H1   = int(os.environ["TELEGRAM_CHAT_ID_H1"])
TELEGRAM_THREAD_ID_H1 = os.environ.get("TELEGRAM_THREAD_ID_H1")

# ======== PARÂMETROS ========
EMA_FAST = 21
EMA_MID  = 120
SMA_LONG = 200

# Lista de tickers (sem ATVI, COUP, EVBG, JWN)
TICKERS = [
    "AIG","AMZN","AAPL","AXP","BA","BAC","BKNG","BLK","C","CAT","COST","CSCO","CVX","DAL",
    "DD","EXPE","F","GE","GM","GOOG","GS","HLT","HPQ","IBM","INTC","JNJ","JPM","KO","MA",
    "MCD","META","MNST","MS","MSFT","NFLX","NVDA","ORCL","PEP","PG","PLTR","PM","RCL","SBUX",
    "SPOT","T","TSLA","UBER","V","WFC","WMT","XOM"
]

# ======== UTILS ========
def above_mas(row: pd.Series) -> bool:
    """Close > EMA21 > EMA120 > SMA200."""
    return (
        row["Close"] > row["ema_fast"] > row["ema_mid"] and
        row["ema_mid"] > row["sma_long"]
    )

def with_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Adiciona EMA21, EMA120 e SMA200; remove NaN iniciais."""
    df["ema_fast"] = df["Close"].ewm(span=EMA_FAST).mean()
    df["ema_mid"]  = df["Close"].ewm(span=EMA_MID).mean()
    df["sma_long"] = df["Close"].rolling(window=SMA_LONG).mean()
    return df.dropna()

def fetch_last_row(sym: str, interval: str, period: str) -> pd.Series | None:
    """
    Baixa dados do yfinance e retorna a ÚLTIMA linha (pode estar em formação — proposital).
    - interval: "60m" ou "1d"
    - period:   "180d" (H1) ou "400d" (D1)
    """
    df = yf.download(
        sym,
        interval=interval,
        period=period,
        auto_adjust=True,
        prepost=False,     # só sessão regular; você pediu simples e rápido
        progress=False,
        threads=False
    )
    if df.empty:
        return None
    df = with_indicators(df)
    if df.empty:
        return None
    return df.iloc[-1]

def check_symbol(sym: str) -> bool:
    """
    Critério único:
      - H1 (última barra disponível): Close > EMA21 > EMA120 > SMA200
      - D1 (última barra disponível): Close > EMA21 > EMA120 > SMA200
    """
    # H1
    row_h1 = fetch_last_row(sym, interval="60m", period="180d")
    if row_h1 is None or not above_mas(row_h1):
        return False

    # D1
    row_d1 = fetch_last_row(sym, interval="1d", period="400d")
    if row_d1 is None or not above_mas(row_d1):
        return False

    return True

def send_telegram(text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID_H1,
        "text": text,
        "disable_web_page_preview": True,
        "parse_mode": "Markdown",
    }
    if TELEGRAM_THREAD_ID_H1:
        payload["message_thread_id"] = int(TELEGRAM_THREAD_ID_H1)

    # pequena robustez
    for _ in range(3):
        try:
            r = requests.post(url, json=payload, timeout=10)
            if r.ok:
                break
        except Exception:
            pass

def main():
    # Varredura paralela para ser rápido
    hits, errors = [], []
    max_workers = min(12, len(TICKERS))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(check_symbol, sym): sym for sym in TICKERS}
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                if fut.result():
                    hits.append(sym)
            except Exception as e:
                errors.append((sym, str(e)))

    hits.sort()
    ts_brt = datetime.datetime.now().strftime("%d/%m/%Y %H:%M")

    msg = (
        f"*🔎 Radar H1/D1 — {ts_brt} (BRT)*\n"
        f"*Filtro:* Close > EMA21 > EMA120 > SMA200 (H1 **e** D1)\n\n"
        f"*Passaram no filtro:* {', '.join(hits) if hits else '—'}"
    )
    send_telegram(msg)

    # Logs
    print(msg)
    if errors:
        print("Erros:", errors)

if __name__ == "__main__":
    main()
