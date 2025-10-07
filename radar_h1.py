import os
import datetime
from zoneinfo import ZoneInfo
import pandas as pd
import yfinance as yf
import requests
import pandas_market_calendars as mcal
from concurrent.futures import ThreadPoolExecutor, as_completed

# ======== SECRETS / AMBIENTE ========
TELEGRAM_TOKEN        = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID_H1   = int(os.environ["TELEGRAM_CHAT_ID_H1"])
TELEGRAM_THREAD_ID_H1 = os.environ.get("TELEGRAM_THREAD_ID_H1")
GITHUB_EVENT_NAME     = os.environ.get("GITHUB_EVENT_NAME", "")

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

# ======== TIMEZONES ========
NY  = ZoneInfo("America/New_York")
BRT = datetime.timezone(datetime.timedelta(hours=-3))  # Brasília

# ======== UTIL ========
def now_ny() -> datetime.datetime:
    return datetime.datetime.now(NY)

def is_market_open_nyse(now_utc: datetime.datetime) -> bool:
    """Usa calendário da NYSE para decidir se hoje é dia útil/aberto."""
    cal   = mcal.get_calendar("NYSE")
    sched = cal.schedule(start_date=now_utc.date(), end_date=now_utc.date())
    return not sched.empty

def last_closed_h1_start_ts_ny(ts_ny: datetime.datetime) -> datetime.datetime:
    """
    Retorna o TIMESTAMP DE ABERTURA da última vela H1 FECHADA em NY.
    No TradingView a H1 fecha em :30; no yfinance o índice é a HORA DE ABERTURA.
    Ex.: se agora 16:46 BRT (15:46 NY), a última fechada foi 15:30–16:30 NY,
    cujo índice é 15:30 NY → start_ts = 15:30.
    """
    minute_block_close = 30 if ts_ny.minute >= 30 else 0
    close_ts = ts_ny.replace(minute=minute_block_close, second=0, microsecond=0)
    if ts_ny.minute < 30:
        close_ts -= datetime.timedelta(minutes=30)
    # última fechada = bloco que acabou de fechar
    start_ts = close_ts - datetime.timedelta(hours=1)
    return start_ts

def prep_intraday(df: pd.DataFrame) -> pd.DataFrame:
    """Garante tz e filtra apenas sessão regular (09:30–16:00 NY)."""
    if df.empty:
        return df
    if df.index.tz is None:
        df = df.tz_localize("UTC").tz_convert(NY)
    else:
        df = df.tz_convert(NY)
    # session regular
    return df.between_time("09:30", "16:00")

def fetch_h1(sym: str) -> pd.DataFrame:
    """Baixa H1 com período longo para estabilizar SMA200."""
    df = yf.download(
        sym, interval="60m", period="180d",
        auto_adjust=True, prepost=False, progress=False, threads=False
    )
    df = prep_intraday(df)
    if df.empty:
        return df
    # Indicadores
    df["ema_fast"] = df["Close"].ewm(span=EMA_FAST).mean()
    df["ema_mid"]  = df["Close"].ewm(span=EMA_MID).mean()
    df["sma_long"] = df["Close"].rolling(window=SMA_LONG).mean()
    df = df.dropna()
    return df

def fetch_d1(sym: str) -> pd.DataFrame:
    """Baixa D1 para confirmar tendência maior."""
    df = yf.download(
        sym, interval="1d", period="400d",
        auto_adjust=True, prepost=False, progress=False, threads=False
    )
    if df.empty:
        return df
    df["ema_fast"] = df["Close"].ewm(span=EMA_FAST).mean()
    df["ema_mid"]  = df["Close"].ewm(span=EMA_MID).mean()
    df["sma_long"] = df["Close"].rolling(window=SMA_LONG).mean()
    df = df.dropna()
    return df

def above_mas(row: pd.Series) -> bool:
    """Close > EMA21 > EMA120 > SMA200."""
    return (
        row["Close"] > row["ema_fast"] > row["ema_mid"] and
        row["ema_mid"] > row["sma_long"]
    )

def check_symbol(sym: str, start_ts: datetime.datetime) -> bool:
    """
    Critério:
      - H1 (última vela FECHADA até start_ts): Close > EMA21 > EMA120 > SMA200
      - D1 (último candle diário fechado):      Close > EMA21 > EMA120 > SMA200
    """
    # H1
    df_h1 = fetch_h1(sym)
    if df_h1.empty:
        # sem dados suficientes
        return False

    # pega somente velas FECHADAS até 'start_ts' (índice H1 = horário de ABERTURA)
    df_up_to = df_h1.loc[:start_ts]
    if df_up_to.empty:
        return False
    row_h1 = df_up_to.iloc[-1]
    if not above_mas(row_h1):
        return False

    # D1 (só busca se H1 passou, pra ser mais rápido)
    df_d1 = fetch_d1(sym)
    if df_d1.empty:
        return False
    row_d1 = df_d1.iloc[-1]
    return above_mas(row_d1)

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

    # pequena robustez a erro transitório
    for _ in range(3):
        try:
            r = requests.post(url, json=payload, timeout=10)
            if r.ok:
                break
        except Exception:
            pass

def main():
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    # Se rodando pelo cron do Actions, pula dias fechados
    if GITHUB_EVENT_NAME == "schedule" and not is_market_open_nyse(now_utc):
        print("NYSE fechada/feriado — pulando execução.")
        return

    ts_ny    = now_ny()
    start_ts = last_closed_h1_start_ts_ny(ts_ny)              # ABERTURA da última H1 FECHADA
    close_ts = start_ts + datetime.timedelta(hours=1)         # FECHAMENTO da última H1

    # Varredura paralela (rápido)
    hits, errors = [], []
    max_workers = min(12, len(TICKERS))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(check_symbol, sym, start_ts): sym for sym in TICKERS}
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                if fut.result():
                    hits.append(sym)
            except Exception as e:
                errors.append((sym, str(e)))

    hits.sort()

    # Mensagem clara com horários BRT/NY
    ts_brt_str       = now_utc.astimezone(BRT).strftime("%d/%m/%Y %H:%M")
    close_ts_str_ny  = close_ts.strftime("%H:%M")
    close_ts_str_brt = close_ts.astimezone(BRT).strftime("%H:%M")
    start_ts_str_ny  = start_ts.strftime("%H:%M")

    header = (
        f"*⏰ Radar H1 US PDV — {ts_brt_str} (BRT)*\n"
        f"_Vela H1 FECHADA: {close_ts_str_brt} BRT ({close_ts_str_ny} NY) • "
        f"Índice H1 usado: {start_ts_str_ny} NY_"
    )
    body = f"\n\n*Ações acima das MAs (H1 e D1):* {', '.join(hits) if hits else '—'}"

    send_telegram(header + body)

    # Logs pro Actions
    print(header)
    print("HITS:", hits)
    if errors:
        print("Erros:", errors)

if __name__ == "__main__":
    main()
