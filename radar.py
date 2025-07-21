import os
import requests
import pandas as pd
import yfinance as yf

TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

EMA_FAST = 21
EMA_MID  = 120
SMA_LONG = 200

TICKERS = [
    "EXPE","AMZN","ORLY","LIN","BURL","FHN","L","STT","NTRS","DLR","AMT",
    "TRV","FLR","D","XEL","ETR","AEP","AWK"
    # ... coloque aqui sua lista completa ...
]

def check_symbol(sym: str) -> bool:
    # histórico diário de 1 ano (para ter pelo menos 252 barras úteis)
    df_d = yf.Ticker(sym).history(period="1y", interval="1d", actions=True)
    # histórico semanal de 5 anos (para robustez)
    df_w = yf.Ticker(sym).history(period="5y", interval="1wk", actions=True)

    if df_d.shape[0] < SMA_LONG or df_w.shape[0] < EMA_MID:
        print(f"{sym:6} → histórico insuficiente ({df_d.shape[0]}d / {df_w.shape[0]}w)")
        return False

    # cálculos das médias
    df_d["ema_fast"] = df_d["Close"].ewm(span=EMA_FAST).mean()
    df_d["ema_mid" ] = df_d["Close"].ewm(span=EMA_MID ).mean()
    df_d["sma_long"] = df_d["Close"].rolling(window=SMA_LONG).mean()

    df_w["ema_fast"] = df_w["Close"].ewm(span=EMA_FAST).mean()
    df_w["ema_mid" ] = df_w["Close"].ewm(span=EMA_MID ).mean()
    df_w["sma_long"] = df_w["Close"].rolling(window=SMA_LONG).mean()

    last_d = df_d.iloc[-1]
    last_w = df_w.iloc[-1]

    # padrão: 1ª barra de baixa, seguidas de 3 barras de alta
    recent = df_d.tail(4)
    bearish  = recent["Close"].iat[0] < recent["Open"].iat[0]
    three_up = all(recent["Close"].iat[i] > recent["Open"].iat[i] for i in [1,2,3])
    pattern  = bearish and three_up

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

    # debug
    print(f"{sym:6} | Pattern: {pattern!s:5} | D1>EMAs+SMA: {cond_d!s:5} | W1>EMAs+SMA: {cond_w!s:5}")
    return pattern and cond_d and cond_w

def send_telegram(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    resp = requests.post(url, json=payload)
    print("Telegram response:", resp.status_code, resp.text)

def main():
    hits = []
    for sym in TICKERS:
        if check_symbol(sym):
            hits.append(sym)

    header = "*🚀 Radar D1 US PDV*"
    if hits:
        body = "*Sinais de Compra:* (" + ", ".join(hits) + ")"
    else:
        body = "_Nenhum sinal encontrado hoje._"

    send_telegram(f"{header}\n\n{body}")

if __name__ == "__main__":
    main()
