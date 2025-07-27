import os
import datetime
import pandas as pd
import yfinance as yf
import requests
import pandas_market_calendars as mcal

# — Seus Secrets do GitHub
TELEGRAM_TOKEN        = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID_H1   = int(os.environ["TELEGRAM_CHAT_ID_H1"])
TELEGRAM_THREAD_ID_H1 = os.environ.get("TELEGRAM_THREAD_ID_H1")

# Parâmetros das médias
EMA_FAST = 21
EMA_MID  = 120
SMA_LONG = 200

# Padrões de barras que queremos nas últimas 5 velas H1
# False = Bear (fechamento < abertura), True = Bull (fechamento > abertura)
PATTERNS = [
    [False, False, True,  True,  True],   # BEAR BEAR BULL BULL BULL
    [False, True,  False, True,  True],   # BEAR BULL BEAR BULL BULL
    [False, True,  True,  True,  True],   # BEAR BULL BULL BULL BULL
    [False, True,  True,  False, True],   # BEAR BULL BULL BEAR BULL
]

# Lista de tickers (sem ATVI, COUP, EVBG, JWN)
TICKERS = [
    "AA","AAPL","ABBV","ABNB","ACN","ADBE","ADI","ADP","AEP","AIG","AKAM","AMAT","AMD",
    # ... (o resto da sua lista) ...
    "YELP","ZG","ZTS"
]

def is_market_open(now_utc):
    """Pula execução em feriados/fds."""
    cal = mcal.get_calendar("NYSE")
    sched = cal.schedule(start_date=now_utc.date(), end_date=now_utc.date())
    return not sched.empty

def check_symbol_h1(sym: str):
    """
    1) Checa se as últimas 5 barras de H1 batem um dos PATTERNS.
    2) Checa se o último candle de H1 fechou acima das 3 médias H1.
    3) Checa se o último candle de D1 fechou acima das 3 médias D1.
    """
    # — H1 (30 dias)
    df_h1 = yf.Ticker(sym).history(period="30d", interval="1h", auto_adjust=True)
    df_h1["ema_fast"] = df_h1["Close"].ewm(span=EMA_FAST).mean()
    df_h1["ema_mid"]  = df_h1["Close"].ewm(span=EMA_MID).mean()
    df_h1["sma_long"] = df_h1["Close"].rolling(window=SMA_LONG).mean()

    # Extrai as últimas 5 barras
    last5 = df_h1.tail(5)
    opens = last5["Open"].values
    closes= last5["Close"].values
    # Converte em lista de bools
    bools = [(closes[i] > opens[i]) for i in range(5)]
    # Verifica se bate algum pattern
    match_pattern = any(bools == p for p in PATTERNS)

    # Verifica fechamento acima das médias no último H1
    last_h1 = df_h1.iloc[-1]
    cond_h1 = (
        last_h1.Close > last_h1.ema_fast and
        last_h1.Close > last_h1.ema_mid  and
        last_h1.Close > last_h1.sma_long
    )

    # — D1 (400d)
    df_d1 = yf.Ticker(sym).history(period="400d", interval="1d", auto_adjust=True)
    df_d1["ema_fast"] = df_d1["Close"].ewm(span=EMA_FAST).mean()
    df_d1["ema_mid"]  = df_d1["Close"].ewm(span=EMA_MID).mean()
    df_d1["sma_long"] = df_d1["Close"].rolling(window=SMA_LONG).mean()

    last_d1 = df_d1.iloc[-1]
    cond_d1 = (
        last_d1.Close > last_d1.ema_fast and
        last_d1.Close > last_d1.ema_mid  and
        last_d1.Close > last_d1.sma_long
    )

    return match_pattern and cond_h1 and cond_d1

def send_telegram(msg: str):
    """Envia no chat H1 (e tópico, se definido)."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id":     TELEGRAM_CHAT_ID_H1,
        "text":        msg,
        "parse_mode":  "Markdown",
        **({"message_thread_id": int(TELEGRAM_THREAD_ID_H1)} 
           if TELEGRAM_THREAD_ID_H1 else {})
    }
    requests.post(url, json=payload)

def main():
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    # se veio via cron, pule feriado/fds
    if os.environ.get("GITHUB_EVENT_NAME")=="schedule" and not is_market_open(now_utc):
        print("Bolsa fechada ou feriado, pulando execução.")
        return

    # Timestamp em Brasília
    timestamp = now_utc.astimezone(
        datetime.timezone(datetime.timedelta(hours=-3))
    ).strftime("%d/%m/%Y %H:%M")

    hits = []
    for sym in TICKERS:
        try:
            if check_symbol_h1(sym):
                hits.append(sym)
        except Exception as e:
            print(f"Erro ao processar {sym}: {e}")

    if hits:
        msg = f"*⏰ Radar H1 US PDV — {timestamp}*\n\n*Sinais de Compra:* {', '.join(hits)}"
    else:
        msg = f"*⏰ Radar H1 US PDV — {timestamp}*\n\nNenhum sinal encontrado agora."

    send_telegram(msg)

if __name__=="__main__":
    main()
