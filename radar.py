import os
import yfinance as yf
import pandas as pd
import requests
from datetime import datetime

# — Seu TOKEN e CHAT_ID via secrets
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT  = os.getenv("TELEGRAM_CHAT_ID")

# Períodos das médias
EMA_FAST = 21
EMA_MID  = 120
SMA_LONG = 200

# Lista de Tickers (coloque toda a sua lista aqui)
TICKERS = [
    "AA","AAPL","ABBV","ABNB","ACN","ADBE","ADI","ADP","AEP","AIG","AKAM","AMAT","AMD","AMGN",
    # ... (coloque o resto da sua lista)
    "UPS","URBN","USB","V","VMW","VZ","W","WBA","WDAY","WDC","WEN","WFC","WHR","WM","WTW","WWE",
    "WYNN","X","XEL","XOM","YELP","ZG","ZTS"
]

def check_symbol(sym: str) -> bool:
    # Baixa 300 dias de barras diárias
    df_d = yf.Ticker(sym).history(period="300d", interval="1d", auto_adjust=True)
    # Baixa 10 anos de barras semanais
    df_w = yf.Ticker(sym).history(period="10y", interval="1wk", auto_adjust=True)

    # Calcula as médias
    df_d["ema_fast"] = df_d["Close"].ewm(span=EMA_FAST, adjust=False).mean()
    df_d["ema_mid"]  = df_d["Close"].ewm(span=EMA_MID,  adjust=False).mean()
    df_d["sma_long"] = df_d["Close"].rolling(window=SMA_LONG).mean()

    df_w["ema_fast"] = df_w["Close"].ewm(span=EMA_FAST, adjust=False).mean()
    df_w["ema_mid"]  = df_w["Close"].ewm(span=EMA_MID,  adjust=False).mean()
    df_w["sma_long"] = df_w["Close"].rolling(window=SMA_LONG).mean()

    # Últimas barras
    last_d = df_d.iloc[-1]
    last_w = df_w.iloc[-1]

    # Condições de preço acima das médias
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

    # Padrão de barras: 1 baixa seguida de 3 altas
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
    # opcional: checar status
    # print(f"Telegram response: {resp.status_code} {resp.text}")

def main():
    hits = []
    for sym in TICKERS:
        try:
            if check_symbol(sym):
                hits.append(sym)
        except Exception:
            pass

    today = datetime.now().strftime("%Y-%m-%d")
    header = f"*Radar D1 US PDV — {today}*"
    if hits:
        body = "\n".join(f"✅ {s}" for s in hits)
    else:
        body = "_Nenhum sinal encontrado hoje._"

    send_telegram(f"{header}\n\n{body}")

if __name__ == "__main__":
    main()
