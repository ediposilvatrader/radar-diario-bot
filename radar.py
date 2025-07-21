import os
import yfinance as yf
import pandas as pd
import requests

# — Seu TOKEN do Bot (obtido no BotFather)
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "seu_token_aqui")

# — O chat_id (ou ID de grupo) onde o bot vai enviar as mensagens
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "seu_chat_id_aqui")

# — Períodos das médias
EMA_FAST = 21
EMA_MID  = 120
SMA_LONG = 200

# — Lista de Tickers para checar (adicione aqui todos que desejar)
TICKERS = [
    "AA","AAPL","ABBV","ABNB","ACN","ADBE","ADI","ADP","AEP","AIG","AKAM","AMAT","AMD","AMGN","AMT","AMZN",
    "ANET","ANSS","APPN","APPS","ATR","ATVI","AVGO","AVY","AWK","AXON","AXP","AZO","BA","BAC","BALL","BAX",
    # ... etc (cole a sua lista completa aqui)
]

def check_symbol(sym):
    # diário: últimos 60 dias
    df_d = yf.Ticker(sym).history(period="60d", interval="1d")
    # semanal: último 5 anos (precisamos de 200+ semanas para a SMA de 200)
    df_w = yf.Ticker(sym).history(period="5y", interval="1wk")

    # cálculo das médias
    df_d["ema_fast"] = df_d["Close"].ewm(span=EMA_FAST).mean()
    df_d["ema_mid"]  = df_d["Close"].ewm(span=EMA_MID).mean()
    df_d["sma_long"] = df_d["Close"].rolling(SMA_LONG).mean()

    df_w["ema_fast"] = df_w["Close"].ewm(span=EMA_FAST).mean()
    df_w["ema_mid"]  = df_w["Close"].ewm(span=EMA_MID).mean()
    df_w["sma_long"] = df_w["Close"].rolling(SMA_LONG).mean()

    # últimas linhas
    last_d = df_d.iloc[-1]
    last_w = df_w.iloc[-1]

    # condições de estar acima das 3 médias
    cond_d = (last_d.Close > last_d.ema_fast
              and last_d.Close > last_d.ema_mid
              and last_d.Close > last_d.sma_long)
    cond_w = (last_w.Close > last_w.ema_fast
              and last_w.Close > last_w.ema_mid
              and last_w.Close > last_w.sma_long)

    # sinal de barras: uma de baixa seguida por 3 de alta
    closes = df_d["Close"].iloc[-4:]
    opens  = df_d["Open"].iloc[-4:]
    bars = list(zip(opens, closes))
    # primeiro deve ser baixa, depois três altas
    bar_signal = (bars[0][1] < bars[0][0]  # 1ª baixa
                  and bars[1][1] > bars[1][0]
                  and bars[2][1] > bars[2][0]
                  and bars[3][1] > bars[3][0])

    return cond_d and cond_w and bar_signal

def send_telegram(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    resp = requests.post(url, json=payload)
    print(f"Telegram response: {resp.status_code} {resp.text}")

def main():
    hits = []
    for sym in TICKERS:
        try:
            if check_symbol(sym):
                hits.append(sym)
        except Exception as e:
            print(f"[!] erro ao processar {sym}: {e}")

    header = "*🚀 Radar D1 US PDV*\n\n"
    if hits:
        body = "*Sinais de Compra:* " + ", ".join(hits)
    else:
        body = "_Nenhum sinal encontrado hoje._"
    send_telegram(header + body)

if __name__ == "__main__":
    main()
