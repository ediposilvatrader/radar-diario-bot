import os
import yfinance as yf
import pandas as pd
import requests
from datetime import datetime, time, timedelta
import pytz

# — Seu TOKEN do Bot (obtido no BotFather)
TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
# — O chat_id do seu grupo ou canal (começa com -100...)
TELEGRAM_CHAT  = os.environ["TELEGRAM_CHAT_ID"]

# Períodos das médias
EMA_FAST = 21
EMA_MID  = 120
SMA_LONG = 200

# Lista de Tickers para checar
TICKERS = ["AAPL","MSFT","AMZN","GOOGL","TSLA","META","NVDA","EME","RS"]

def check_symbol(sym):
    # Pega 60 dias para o Daily e 26 semanas para o Weekly
    df_d = yf.Ticker(sym).history(period="60d", interval="1d")
    df_w = yf.Ticker(sym).history(period="26wk", interval="1wk")

    # Calcula as médias
    df_d["ema_fast"] = df_d["Close"].ewm(span=EMA_FAST).mean()
    df_d["ema_mid"]  = df_d["Close"].ewm(span=EMA_MID).mean()
    df_d["sma_long"] = df_d["Close"].rolling(SMA_LONG).mean()

    df_w["ema_fast"] = df_w["Close"].ewm(span=EMA_FAST).mean()
    df_w["ema_mid"]  = df_w["Close"].ewm(span=EMA_MID).mean()
    df_w["sma_long"] = df_w["Close"].rolling(SMA_LONG).mean()

    last_d = df_d.iloc[-1]
    last_w = df_w.iloc[-1]

    # Condições de estar acima das 3 médias
    cond_d = last_d.Close > last_d.ema_fast and last_d.Close > last_d.ema_mid and last_d.Close > last_d.sma_long
    cond_w = last_w.Close > last_w.ema_fast and last_w.Close > last_w.ema_mid and last_w.Close > last_w.sma_long

    # Sequência de candles: última de baixa, depois 3 de alta
    # Índices: -4 = candle de baixa, -3,-2,-1 de alta
    seq_ok = (
        df_d["Close"].iloc[-4] < df_d["Open"].iloc[-4] and
        df_d["Close"].iloc[-3] > df_d["Open"].iloc[-3] and
        df_d["Close"].iloc[-2] > df_d["Open"].iloc[-2] and
        df_d["Close"].iloc[-1] > df_d["Open"].iloc[-1]
    )

    return cond_d and cond_w and seq_ok

def send_telegram(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT,
        "text": message
    }
    resp = requests.post(url, json=payload)
    print("Telegram response:", resp.status_code, resp.text)

def main():
    hits = []
    for sym in TICKERS:
        try:
            if check_symbol(sym):
                hits.append(sym)
        except Exception as e:
            print(f"Erro em {sym}:", e)

    if hits:
        msg = "Radar Diário Automático\n\n" + "\n".join(f"✅ {s}" for s in hits)
    else:
        msg = "Radar Diário Automático\n\n❌ Nenhum ativo bateu o filtro hoje."
    send_telegram(msg)

if __name__ == "__main__":
    main()
