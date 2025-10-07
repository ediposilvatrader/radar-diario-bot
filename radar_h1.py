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
EPS      = 1e-3  # tolerância ~0,001 (0,1%). Ajuste se quiser mais/menos rígido.

TICKERS = [
    "AIG","AMZN","AAPL","AXP","BA","BAC","BKNG","BLK","C","CAT","COST","CSCO","CVX","DAL",
    "DD","EXPE","F","GE","GM","GOOG","GS","HLT","HPQ","IBM","INTC","JNJ","JPM","KO","MA",
    "MCD","META","MNST","MS","MSFT","NFLX","NVDA","ORCL","PEP","PG","PLTR","PM","RCL","SBUX",
    "SPOT","T","TSLA","UBER","V","WFC","WMT","XOM"
]

NY  = ZoneInfo("America/New_York")
BRT = datetime.timezone(datetime.timedelta(hours=-3))

# ======== HELPERS ========
def ema(series: pd.Series, length: int) -> pd.Series:
    # EMA padrão de trading (igual TV): recursiva, sem 'adjust'
    return series.ewm(span=length, adjust=False, min_periods=length).mean()

def sma(series: pd.Series, length: int) -> pd.Series:
    return series.rolling(window=length, min_periods=length).mean()

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df["ema_fast"] = ema(df["Close"], EMA_FAST)
    df["ema_mid"]  = ema(df["Close"], EMA_MID)
    df["sma_long"] = sma(df["Close"], SMA_LONG)
    return df.dropna()

def fetch_last_row(sym: str, interval: str, period: str) -> pd.Series | None:
    """
    Baixa do Yahoo e retorna a ÚLTIMA linha disponível.
    Sem filtro de sessão; sem preocupação com vela fechada.
    """
    df = yf.download(
        sym,
        interval=interval,
        period=period,
        auto_adjust=True,  # consistente com TV quando se usa closes ajustados
        prepost=False,     # simples: somente sessão regular quando o Yahoo trouxer
        progress=False,
        threads=False
    )
    if df.empty:
        return None
    # garantir timezone (para log apenas)
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC").tz_convert(NY)
    else:
        df.index = df.index.tz_convert(NY)
    df = add_indicators(df)
    if df.empty:
        return None
    return df.iloc[-1]

def above_mas(row: pd.Series) -> bool:
    """
    Com tolerância: Close >= EMA21 >= EMA120 e EMA120 >= SMA200.
    EPS evita falsos negativos quando o preço encosta nas médias.
    """
    c, e21, e120, s200 = float(row["Close"]), float(row["ema_fast"]), float(row["ema_mid"]), float(row["sma_long"])
    return (c + EPS >= e21) and (e21 + EPS >= e120) and (e120 + EPS >= s200)

def check_symbol(sym: str) -> tuple[bool, str]:
    # H1
    h1 = fetch_last_row(sym, interval="60m", period="180d")
    if h1 is None:
        return False, "sem_h1"
    h1_ok = above_mas(h1)

    # D1
    d1 = fetch_last_row(sym, interval="1d", period="400d")
    if d1 is None:
        return False, "sem_d1"
    d1_ok = above_mas(d1)

    if DEBUG and (sym in DEBUG_TICKERS or not DEBUG_TICKERS):
        try:
            print(f"[{sym}] H1 {h1.name.strftime('%Y-%m-%d %H:%M NY')}  "
                  f"Close={h1['Close']:.3f}  EMA21={h1['ema_fast']:.3f}  EMA120={h1['ema_mid']:.3f}  SMA200={h1['sma_long']:.3f}  -> {h1_ok}")
            print(f"[{sym}] D1 {d1.name.strftime('%Y-%m-%d')}         "
                  f"Close={d1['Close']:.3f}  EMA21={d1['ema_fast']:.3f}  EMA120={d1['ema_mid']:.3f}  SMA200={d1['sma_long']:.3f}  -> {d1_ok}")
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
    # permite depurar só alguns tickers, se quiser
    tickers = TICKERS if not DEBUG_TICKERS else [t for t in TICKERS if t in DEBUG_TICKERS]

    hits, fails = [], []
    with ThreadPoolExecutor(max_workers=min(12, len(tickers))) as ex:
        futs = {ex.submit(check_symbol, t): t for t in tickers}
        for f in as_completed(futs):
            t = futs[f]
            ok, reason = f.result()
            if ok:
                hits.append(t)
            else:
                fails.append((t, reason))

    hits.sort()
    ts_brt = datetime.datetime.now(BRT).strftime("%d/%m/%Y %H:%M")
    msg = (
        f"*🔎 Radar H1/D1 — {ts_brt} (BRT)*\n"
        f"*Critério:* Close ≥ EMA21 ≥ EMA120 ≥ SMA200 (H1 **e** D1, com ε={EPS})\n\n"
        f"*Passaram:* {', '.join(hits) if hits else '—'}"
    )
    print(msg)
    send_telegram(msg)

    if DEBUG:
        print("Falhas resumidas:", fails)

if __name__ == "__main__":
    main()

