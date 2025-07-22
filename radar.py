import os
import datetime
import pandas as pd
import yfinance as yf
import requests
import pandas_market_calendars as mcal

# — Seu TOKEN do Bot e chat_id via Secrets
TELEGRAM_TOKEN   = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

# Parâmetros das médias
EMA_FAST = 21
EMA_MID  = 120
SMA_LONG = 200

# Símbolo para debugar individualmente
DEBUG_SYMBOL = "EXPE"   # troque aqui pelo ticker que você quiser inspecionar

# Lista completa de tickers
TICKERS = [
    "AAPL","MSFT","AMZN","GOOGL","TSLA","META","NVDA",
    # ... (adicione sua lista completa aqui) ...
]

def is_market_open(now_utc):
    """Verifica se NYSE está aberta no momento UTC dado."""
    nyse = mcal.get_calendar("NYSE")
    sched = nyse.schedule(start_date=now_utc.date(), end_date=now_utc.date())
    if sched.empty:
        return False
    open_utc  = sched.iloc[0]["market_open"].tz_convert("UTC")
    close_utc = sched.iloc[0]["market_close"].tz_convert("UTC")
    return open_utc <= now_utc <= close_utc

def check_symbol(sym: str, debug: bool=False):
    """
    Se debug=False: retorna True/False.
    Se debug=True: retorna (df_d, df_w, pattern, cond_d, cond_w).
    """
    # histórico
    df_d = yf.Ticker(sym).history(period="60d", interval="1d", auto_adjust=True)
    df_w = yf.Ticker(sym).history(period="26wk", interval="1wk", auto_adjust=True)

    # médias D1
    df_d["ema_fast"] = df_d["Close"].ewm(span=EMA_FAST).mean()
    df_d["ema_mid"]  = df_d["Close"].ewm(span=EMA_MID).mean()
    df_d["sma_long"] = df_d["Close"].rolling(window=SMA_LONG).mean()

    # médias W1
    df_w["ema_fast"] = df_w["Close"].ewm(span=EMA_FAST).mean()
    df_w["ema_mid"]  = df_w["Close"].ewm(span=EMA_MID).mean()
    df_w["sma_long"] = df_w["Close"].rolling(window=SMA_LONG).mean()

    # padrão de barras D1 (1↓ + 3↑)
    last4 = df_d.tail(4)
    opens  = last4["Open"].values
    closes = last4["Close"].values
    pattern = (
        closes[0] < opens[0] and
        closes[1] > opens[1] and
        closes[2] > opens[2] and
        closes[3] > opens[3]
    )

    # condições D1 e W1 preço > médias
    ld = df_d.iloc[-1]
    lw = df_w.iloc[-1]
    cond_d = (ld.Close > ld.ema_fast and ld.Close > ld.ema_mid and ld.Close > ld.sma_long)
    cond_w = (lw.Close > lw.ema_fast and lw.Close > lw.ema_mid and lw.Close > lw.sma_long)

    if debug:
        return df_d, df_w, pattern, cond_d, cond_w

    return pattern and cond_d and cond_w

def send_telegram(msg: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"}
    requests.post(url, json=payload)

def main():
    now = datetime.datetime.now(datetime.timezone.utc)

    # só bloqueia em feriados/final de semana quando for schedule
    if os.environ.get("GITHUB_EVENT_NAME") == "schedule":
        if not is_market_open(now):
            print("Bolsa fechada ou feriado, pulando execução.")
            return

    hits = []
    for sym in TICKERS:
        try:
            if sym == DEBUG_SYMBOL:
                # modo debug para o símbolo específico
                df_d, df_w, pattern, cond_d, cond_w = check_symbol(sym, debug=True)
                print(f"\n>>> DEBUG {sym} <<<")
                print("Últimas 4 barras D1 (Open, Close):")
                print(df_d[["Open","Close"]].tail(4).to_string())
                print(f"  Pattern (1↓+3↑): {pattern}")
                print(f"  Cond D1 (>EMAs+SMA?): {cond_d}")
                print(f"  Cond W1 (>EMAs+SMA?): {cond_w}\n")
                # continua para incluir ou não no hits
                if pattern and cond_d and cond_w:
                    hits.append(sym)
            else:
                if check_symbol(sym):
                    hits.append(sym)
        except Exception as e:
            print(f"Erro ao processar {sym}: {e}")

    # monta mensagem
    if hits:
        msg = "*🚀 Radar D1 US PDV*\n\n*Sinais de Compra:* " + ", ".join(hits)
    else:
        msg = "*🚀 Radar D1 US PDV*\n\nNenhum sinal encontrado hoje."

    send_telegram(msg)

if __name__ == "__main__":
    main()
