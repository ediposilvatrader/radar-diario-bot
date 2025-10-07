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

# Padrões de barras: False = Bear, True = Bull (para as 6 últimas velas FECHADAS)
PATTERNS = [
    [True,  False, False, True,  True,  True],   # BULL BEAR BEAR BULL BULL BULL
    [False, False, True,  False, True,  True],   # BEAR BEAR BULL BEAR BULL BULL
    [False, False, True,  True,  True,  True],   # BEAR BEAR BULL BULL BULL BULL
    [False, False, True,  True,  False, True],   # BEAR BEAR BULL BULL BEAR BULL
    [False, False, False, True,  True,  True],   # BEAR BEAR BEAR BULL BULL BULL
]

# ======== TIMEZONES ========
NY  = ZoneInfo("America/New_York")
BRT = datetime.timezone(datetime.timedelta(hours=-3))  # Brasilia

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
    No TV a vela fecha em :30; no yfinance o índice é a HORA DE ABERTURA.
    Ex.: se agora 15:47 NY, a vela fechada foi 14:30–15:30; índice = 14:30.
    """
    minute_block_close = 30 if ts_ny.minute >= 30 else 0
    close_ts = ts_ny.replace(minute=minute_block_close, second=0, microsecond=0)
    if ts_ny.minute < 30:
        close_ts -= datetime.timedelta(minutes=30)
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
    return df.between_time("09:30", "16:00")

def fetch_h1(sym: str) -> pd.DataFrame:
    # Período longo pra SMA200 H1 ficar estável
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

def check_symbol_h1(sym: str, start_ts: datetime.datetime) -> bool:
    """
    Lógica PDV-H1:
      - Pega somentes velas FECHADAS até start_ts (abertura da última fechada)
      - Padrões nas 6 últimas velas fechadas
      - Close acima de EMA21 > EMA120 > SMA200 (H1)
      - Confirmação D1 idêntica (Close > EMA21 > EMA120 > SMA200)
    """
    df_h1 = fetch_h1(sym)
    if df_h1.empty:
        return False

    # Toma somente velas <= start_ts (garante vela fechada)
    df_up_to = df_h1.loc[:start_ts]
    if df_up_to.empty or len(df_up_to) < 6:
        return False

    # Linha da última vela H1 FECHADA
    row_h1 = df_up_to.iloc[-1]

    # Padrões nas 6 últimas velas FECHADAS
    last6 = df_up_to.tail(6)
    bools = (last6["Close"] > last6["Open"]).astype(bool).tolist()
    match_pattern = any(bools == p for p in PATTERNS)

    cond_h1 = (
        row_h1["Close"] > row_h1["ema_fast"] > row_h1["ema_mid"] and
        row_h1["ema_mid"] > row_h1["sma_long"]
    )

    if not (match_pattern and cond_h1):
        return False

    # Só busca D1 se passou no H1 (economiza tempo)
    df_d1 = fetch_d1(sym)
    if df_d1.empty:
        return False
    last_d1 = df_d1.iloc[-1]
    cond_d1 = (
        last_d1["Close"] > last_d1["ema_fast"] > last_d1["ema_mid"] and
        last_d1["ema_mid"] > last_d1["sma_long"]
    )
    return cond_d1

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

    # tenta algumas vezes se der erro transitório
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

    ts_ny   = now_ny()
    start_ts = last_closed_h1_start_ts_ny(ts_ny)   # abertura da última H1 fechada
    close_ts = start_ts + datetime.timedelta(hours=1)  # horário de FECHAMENTO da vela

    # Paraleliza a varredura para acelerar (ajuste max_workers conforme gosto)
    hits = []
    errors = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(check_symbol_h1, sym, start_ts): sym for sym in TICKERS}
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                if fut.result():
                    hits.append(sym)
            except Exception as e:
                errors.append((sym, str(e)))

    hits.sort()  # ordena alfabeticamente só pra ficar bonito

    # Monta mensagem
    ts_brt_str = now_utc.astimezone(BRT).strftime("%d/%m/%Y %H:%M")
    close_ts_str_ny  = close_ts.strftime("%H:%M")
    start_ts_str_ny  = start_ts.strftime("%H:%M")
    header = f"*⏰ Radar H1 US PDV — {ts_brt_str} (BRT)*\n" \
             f"_Vela H1 fechada em {close_ts_str_ny} NY (índice {start_ts_str_ny})_"
    if hits:
        body = f"\n\n*Sinais de Compra:* {', '.join(hits)}"
    else:
        body = f"\n\nNenhum sinal encontrado agora."

    send_telegram(header + body)

    # Log básico pro Actions
    print(header)
    print("HITS:", hits)
    if errors:
        print("Erros:", errors)

if __name__ == "__main__":
    main()
