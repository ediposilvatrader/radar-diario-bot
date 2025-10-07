import os
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from zoneinfo import ZoneInfo
import pandas as pd
import yfinance as yf
import requests

# ================== SECRETS ==================
TELEGRAM_TOKEN        = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID_H1   = int(os.environ["TELEGRAM_CHAT_ID_H1"])
TELEGRAM_THREAD_ID_H1 = os.environ.get("TELEGRAM_THREAD_ID_H1")

# ================== DEBUG ===================
# você pode ligar debug geral por env:
#   DEBUG=1
# e restringir tickers:
#   DEBUG_TICKERS=PLTR,AIG
DEBUG = os.getenv("DEBUG", "0") == "1"
DEBUG_TICKERS = {t.strip().upper() for t in os.getenv("DEBUG_TICKERS", "").split(",")} - {""}
# sempre debugar PLTR e AIG (como você pediu)
FORCE_DEBUG_TICKERS = {"PLTR", "AIG"}

# ================ PARÂMETROS ================
EMA_FAST = 21
EMA_MID  = 120
SMA_LONG = 200

# tolerância para “encostou na média”
EPS_ABS = 0.02    # US$ 0,02
EPS_REL = 0.001   # 0,10%

NY  = ZoneInfo("America/New_York")
BRT = datetime.timezone(datetime.timedelta(hours=-3))

TICKERS = [
    "AIG","AMZN","AAPL","AXP","BA","BAC","BKNG","BLK","C","CAT","COST","CSCO","CVX","DAL",
    "DD","EXPE","F","GE","GM","GOOG","GS","HLT","HPQ","IBM","INTC","JNJ","JPM","KO","MA",
    "MCD","META","MNST","MS","MSFT","NFLX","NVDA","ORCL","PEP","PG","PLTR","PM","RCL","SBUX",
    "SPOT","T","TSLA","UBER","V","WFC","WMT","XOM"
]

# ================ HELPERS ===================
def ema(series: pd.Series, length: int) -> pd.Series:
    # EMA igual TradingView: recursiva, sem adjust, com min_periods
    return series.ewm(span=length, adjust=False, min_periods=length).mean()

def sma(series: pd.Series, length: int) -> pd.Series:
    return series.rolling(window=length, min_periods=length).mean()

def fetch_history(sym: str, interval: str, period: str) -> pd.DataFrame:
    """Usa Ticker().history para evitar MultiIndex e pegar a última barra disponível."""
    df = yf.Ticker(sym).history(
        period=period,
        interval=interval,
        auto_adjust=False,   # deixe False (preço 'cru'); mude para True se preferir ajustado
        prepost=False,
        actions=False,
    )
    if df.empty:
        return df
    # timezone (apenas para log ficar coerente)
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC").tz_convert(NY)
    else:
        df.index = df.index.tz_convert(NY)
    return df

def last_row_with_indicators(sym: str, interval: str, period: str) -> pd.Series | None:
    df = fetch_history(sym, interval, period)
    if df.empty or "Close" not in df.columns:
        return None
    close = pd.to_numeric(df["Close"], errors="coerce")
    out = pd.DataFrame({
        "Close":    close,
        "ema_fast": ema(close, EMA_FAST),
        "ema_mid":  ema(close, EMA_MID),
        "sma_long": sma(close, SMA_LONG),
    }).dropna()
    if out.empty:
        return None
    return out.iloc[-1]

def gte_tol(a: float, b: float) -> bool:
    """a >= b com tolerância absoluta/relativa."""
    tol = max(EPS_ABS, abs(b) * EPS_REL)
    return a + tol >= b

def above_mas(row: pd.Series) -> bool:
    c    = float(row.at["Close"])
    e21  = float(row.at["ema_fast"])
    e120 = float(row.at["ema_mid"])
    s200 = float(row.at["sma_long"])
    return gte_tol(c, e21) and gte_tol(e21, e120) and gte_tol(e120, s200)

def _should_debug(sym: str) -> bool:
    return DEBUG or (sym in FORCE_DEBUG_TICKERS) or (sym in DEBUG_TICKERS)

def check_symbol(sym: str) -> tuple[bool, dict]:
    info: dict = {"sym": sym}

    # H1
    h1 = last_row_with_indicators(sym, "60m", "180d")
    if h1 is None:
        info["h1"] = "sem_dados"
        return False, info
    h1_vals = {
        "time": h1.name.strftime("%Y-%m-%d %H:%M NY"),
        "close": float(h1["Close"]),
        "ema21": float(h1["ema_fast"]),
        "ema120": float(h1["ema_mid"]),
        "sma200": float(h1["sma_long"]),
    }
    h1_ok = above_mas(h1)
    info["h1"] = {"ok": h1_ok, **h1_vals}

    # D1
    d1 = last_row_with_indicators(sym, "1d", "400d")
    if d1 is None:
        info["d1"] = "sem_dados"
        return False, info
    d1_vals = {
        "time": d1.name.strftime("%Y-%m-%d"),
        "close": float(d1["Close"]),
        "ema21": float(d1["ema_fast"]),
        "ema120": float(d1["ema_mid"]),
        "sma200": float(d1["sma_long"]),
    }
    d1_ok = above_mas(d1)
    info["d1"] = {"ok": d1_ok, **d1_vals}

    # debug obrigatório para PLTR/AIG + opcional via env
    if _should_debug(sym):
        try:
            print(f"[{sym}] H1 {h1_vals['time']}  "
                  f"C={h1_vals['close']:.4f}  E21={h1_vals['ema21']:.4f}  "
                  f"E120={h1_vals['ema120']:.4f}  S200={h1_vals['sma200']:.4f}  -> {h1_ok}")
            print(f"[{sym}] D1 {d1_vals['time']}       "
                  f"C={d1_vals['close']:.4f}  E21={d1_vals['ema21']:.4f}  "
                  f"E120={d1_vals['ema120']:.4f}  S200={d1_vals['sma200']:.4f}  -> {d1_ok}")
        except Exception:
            pass

    return (h1_ok and d1_ok), info

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

# ================== MAIN =====================
def main():
    hits, details = [], []

    with ThreadPoolExecutor(max_workers=min(12, len(TICKERS))) as ex:
        futs = {ex.submit(check_symbol, t): t for t in TICKERS}
        for f in as_completed(futs):
            ok, info = f.result()
            details.append(info)
            if ok:
                hits.append(info["sym"])

    hits.sort()

    # === Mensagem no formato solicitado ===
    msg = "Radar Pressão H1 - Sinais de Compra\n\nAções: " + (", ".join(hits) if hits else "—")
    print(msg)
    send_telegram(msg)

    # Se quiser ver por que PLTR/AIG não entraram, os prints acima já mostram.
    # Mas deixo um resumo final focado nelas:
    for sym in ("PLTR", "AIG"):
        for info in details:
            if info.get("sym") == sym:
                h1 = info.get("h1")
                d1 = info.get("d1")
                if isinstance(h1, dict) and isinstance(d1, dict):
                    print(
                        f"[RESUMO {sym}] "
                        f"H1 ok={h1['ok']}  C={h1['close']:.4f} E21={h1['ema21']:.4f} E120={h1['ema120']:.4f} S200={h1['sma200']:.4f}  |  "
                        f"D1 ok={d1['ok']}  C={d1['close']:.4f} E21={d1['ema21']:.4f} E120={d1['ema120']:.4f} S200={d1['sma200']:.4f}"
                    )
                else:
                    print(f"[RESUMO {sym}] dados insuficientes (h1={h1}, d1={d1})")

if __name__ == "__main__":
    main()
