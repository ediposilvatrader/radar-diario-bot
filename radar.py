import os
import yfinance as yf
import pandas as pd
import requests
import pytz
import pandas_market_calendars as mcal
from datetime import datetime, time

# — Carrega TOKEN e CHAT_ID via Secrets
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT  = os.getenv("TELEGRAM_CHAT_ID")

# Parâmetros das médias
EMA_FAST = 21
EMA_MID  = 120
SMA_LONG = 200

# Sua lista completa de tickers
TICKERS = [
    "AA","AAPL","ABBV","ABNB","ACN","ADBE","ADI","ADP","AEP","AIG","AKAM",
    # ... (todos os outros) ...
    "XEL","XOM","YELP","ZG","ZTS"
]

def is_nyse_open_today(now_ny):
    cal = mcal.get_calendar("NYSE")
    sched = cal.schedule(start_date=now_ny.date(), end_date=now_ny.date())
    return not sched.empty

def should_run():
    tz = pytz.timezone("America/New_York")
    now_ny = datetime.now(pytz.utc).astimezone(tz)
    # se não for dia de pregão (feriado ou fim de semana), não executa
    if not is_nyse_open_today(now_ny):
        print("Hoje não é dia de pregão NYSE. Saindo sem rodar.")
        return False
    # opcional: você pode também verificar que já passou das 16:30 local
    if now_ny.time() < time(16, 30):
        print(f"Ainda não são 16:30 NYT (agora {now_ny.time()}). Saindo.")
        return False
    return True

def check_symbol(sym):
    # baixa dados com ajuste por dividendos
    df_d = yf.Ticker(sym).history(period="250d", interval="1d", auto_adjust=True)
    df_w = yf.Ticker(sym).history(period="250wk", interval="1wk", auto_adjust=True)

    df_d["ema_fast"] = df_d["Close"].ewm(span=EMA_FAST, adjust=False).mean()
    df_d["ema_mid"]  = df_d["Close"].ewm(span=EMA_MID, adjust=False).mean()
    df_d["sma_long"] = df_d["Close"].rolling(window=SMA_LONG).mean()

    df_w["ema_fast"] = df_w["Close"].ewm(span=EMA_FAST, adjust=False).mean()
    df_w["ema_mid"]  = df_w["Close"].ewm(span=EMA_MID, adjust=False).mean()
    df_w["sma_long"] = df_w["Close"].rolling(window=SMA_LONG).mean()

    last_d = df_d.iloc[-1]
    last_w = df_w.iloc[-1]

    cond_d = (
        last_d.Close > last_d.ema_fast and
        last_d.Close > last_d.ema_mid  and
        last_d.Close > last_d.sma_long
    )
    cond_w = (
        last_w.Close > last_w.ema_fast and
        last_w.Close > last_w.ema_mid  and
        last_w.Close > last_w.sma_long
    )

    bars = df_d.tail(4)
    bear  = bars["Close"].iloc[0] < bars["Open"].iloc[0]
    bull1 = bars["Close"].iloc[1] > bars["Open"].iloc[1]
    bull2 = bars["Close"].iloc[2] > bars["Open"].iloc[2]
    bull3 = bars["Close"].iloc[3] > bars["Open"].iloc[3]
    pattern = bear and bull1 and bull2 and bull3

    return cond_d and cond_w and pattern

def send_telegram(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT,
        "text": message,
        "parse_mode": "Markdown"
    }
    resp = requests.post(url, json=payload)
    print("Telegram response:", resp.status_code, resp.text)

def main():
    if not should_run():
        return  # sai sem enviar

    hits = []
    for sym in TICKERS:
        try:
            if check_symbol(sym):
                hits.append(sym)
        except Exception as e:
            print(f"Erro em {sym}: {e}")

    if hits:
        msg = "*Radar D1 US PDV*\n\n" + f"Sinais de Compra: ({', '.join(hits)})"
    else:
        msg = "*Radar D1 US PDV*\n\nNenhum sinal de compra hoje."

    send_telegram(msg)

if __name__ == "__main__":
    main()
