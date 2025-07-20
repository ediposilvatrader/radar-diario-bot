import os
import yfinance as yf
import pandas as pd
import requests

# — Seu TOKEN e CHAT_ID via variáveis de ambiente
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT  = os.getenv("TELEGRAM_CHAT_ID")

# Parâmetros das médias
EMA_FAST = 21
EMA_MID  = 120
SMA_LONG = 200

# Tickers de teste (sua lista completa aqui)
TICKERS = ["BURL","FHN","L","STT","NTRS","DLR","AMT","TRV","FLR","D","XEL","ETR","AEP","AWK"]

def check_symbol(sym, debug=False):
    # Histórico diário e semanal já com ajustes
    df_d = yf.Ticker(sym).history(period="60d", interval="1d", auto_adjust=True)
    df_w = yf.Ticker(sym).history(period="26wk", interval="1wk", auto_adjust=True)

    # Calcula médias sobre Adj Close
    df_d["ema_fast"] = df_d["Close"].ewm(span=EMA_FAST, adjust=False).mean()
    df_d["ema_mid"]  = df_d["Close"].ewm(span=EMA_MID, adjust=False).mean()
    df_d["sma_long"] = df_d["Close"].rolling(window=SMA_LONG).mean()

    df_w["ema_fast"] = df_w["Close"].ewm(span=EMA_FAST, adjust=False).mean()
    df_w["ema_mid"]  = df_w["Close"].ewm(span=EMA_MID, adjust=False).mean()
    df_w["sma_long"] = df_w["Close"].rolling(window=SMA_LONG).mean()

    last_d = df_d.iloc[-1]
    last_w = df_w.iloc[-1]

    # condições de preço acima das médias
    cond_d = (last_d.Close > last_d.ema_fast
           and last_d.Close > last_d.ema_mid
           and last_d.Close > last_d.sma_long)
    cond_w = (last_w.Close > last_w.ema_fast
           and last_w.Close > last_w.ema_mid
           and last_w.Close > last_w.sma_long)

    # sequencia: quarta barra de baixa, seguida de 3 de alta
    three_bulls = (
        df_d["Close"].iloc[-1]  > df_d["Open"].iloc[-1] and
        df_d["Close"].iloc[-2]  > df_d["Open"].iloc[-2] and
        df_d["Close"].iloc[-3]  > df_d["Open"].iloc[-3] and
        df_d["Close"].iloc[-4]  < df_d["Open"].iloc[-4]
    )

    if debug:
        print(f"\n>>> {sym}")
        display(df_d[["Open","Close","ema_fast","ema_mid","sma_long"]].tail(5))
        print(f"Cond Diário (preço>EMAs+SMA): {cond_d}")
        print(f"Cond Semanal  (preço>EMAs+SMA): {cond_w}")
        print(f"Sinal barras (↓↑↑↑): {three_bulls}")

    return cond_d and cond_w and three_bulls

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT,
        "text": message,
        "parse_mode": "Markdown"
    }
    resp = requests.post(url, json=payload)
    print("Telegram response:", resp.status_code, resp.text)

def main():
    hits = []
    for sym in TICKERS:
        ok = check_symbol(sym, debug=True)  # ativa debug
        if ok:
            hits.append(sym)

    if hits:
        msg = "*Radar Diário Automático*\n\n" + "\n".join(f"✅ {s}" for s in hits)
    else:
        msg = "⚠️ Nenhum ativo bateu o filtro hoje."
    send_telegram(msg)

if __name__ == "__main__":
    main()
