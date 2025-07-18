import yfinance as yf
import pandas as pd
import requests
import os

# — Seu TOKEN do Bot (obtido no BotFather) via Secret ou variável de ambiente
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "80398308042:AAFbB8Dkrg_hpIL4rJurHm_HHV6YCM1Uknw")
# — O chat_id que você anotou via Secret ou variável de ambiente
TELEGRAM_CHAT  = os.environ.get("TELEGRAM_CHAT_ID", "1885160562")

# Períodos das médias
EMA_FAST = 21
EMA_MID  = 120
SMA_LONG = 200

# Lista de Tickers para checar
TICKERS = ["AAPL", "MSFT", "AMZN", "GOOGL", "TSLA", "META", "NVDA"]

def check_symbol(sym):
    # Histórico diário e semanal
    df_d = yf.Ticker(sym).history(period="60d",  interval="1d")
    df_w = yf.Ticker(sym).history(period="26wk", interval="1wk")

    # Cálculo das EMAs e SMA no Diário
    df_d["ema_fast"] = df_d["Close"].ewm(span=EMA_FAST, adjust=False).mean()
    df_d["ema_mid"]  = df_d["Close"].ewm(span=EMA_MID,  adjust=False).mean()
    df_d["sma_long"] = df_d["Close"].rolling(window=SMA_LONG).mean()

    # Cálculo das EMAs e SMA no Semanal
    df_w["ema_fast"] = df_w["Close"].ewm(span=EMA_FAST, adjust=False).mean()
    df_w["ema_mid"]  = df_w["Close"].ewm(span=EMA_MID,  adjust=False).mean()
    df_w["sma_long"] = df_w["Close"].rolling(window=SMA_LONG).mean()

    # Últimos fechamentos
    last_d = df_d.iloc[-1]
    last_w = df_w.iloc[-1]

    # Condições de viés e última barra de alta
    cond_d = (last_d.Close > last_d.ema_fast
              and last_d.Close > last_d.ema_mid
              and last_d.Close > last_d.sma_long)
    cond_w = (last_w.Close > last_w.ema_fast
              and last_w.Close > last_w.ema_mid
              and last_w.Close > last_w.sma_long)
    bull_d = last_d.Close > last_d.Open

    return cond_d and cond_w and bull_d

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id":    TELEGRAM_CHAT,
        "text":       message,
        "parse_mode": "Markdown"
    }
    resp = requests.post(url, json=payload)
    print("→ Telegram response:", resp.status_code, resp.text)

def main():
    hits = []
    for sym in TICKERS:
        try:
            if check_symbol(sym):
                hits.append(sym)
        except Exception as e:
            print(f"! Erro ao checar {sym}: {e}")

    # Monta e envia a mensagem sempre, mesmo sem hits
    if hits:
        body = "\n".join(f"✅ {s}" for s in hits)
    else:
        body = "❌ Nenhum ativo bateu o filtro hoje."

    msg = "*Radar Diário Automático*\n\n" + body
    send_telegram(msg)

if __name__ == "__main__":
    main()
