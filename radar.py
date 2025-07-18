import os
import yfinance as yf
import pandas as pd
import requests

# — Carrega TOKEN e CHAT_ID a partir das variáveis de ambiente (definidas nos Secrets do GitHub Actions)
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT  = os.getenv("TELEGRAM_CHAT_ID")

# Períodos das médias
EMA_FAST  = 21
EMA_MID   = 120
SMA_LONG  = 200

# Lista de Tickers para checar
TICKERS = ["AAPL","MSFT","AMZN","GOOGL","TSLA","META","NVDA"]

def check_symbol(sym):
    # Histórico diário (últimos 60 dias) e semanal (últimas 26 semanas)
    df_d = yf.Ticker(sym).history(period="60d", interval="1d")
    df_w = yf.Ticker(sym).history(period="26wk", interval="1wk")

    # Calcula médias móveis
    df_d["ema_fast"] = df_d["Close"].ewm(span=EMA_FAST).mean()
    df_d["ema_mid"]  = df_d["Close"].ewm(span=EMA_MID).mean()
    df_d["sma_long"] = df_d["Close"].rolling(window=SMA_LONG).mean()

    df_w["ema_fast"] = df_w["Close"].ewm(span=EMA_FAST).mean()
    df_w["ema_mid"]  = df_w["Close"].ewm(span=EMA_MID).mean()
    df_w["sma_long"] = df_w["Close"].rolling(window=SMA_LONG).mean()

    # Últimos valores fechados
    last_d = df_d.iloc[-1]
    last_w = df_w.iloc[-1]

    # Condições: fechamento acima das 3 médias no diário e no semanal, e última diária alta
    cond_d = (last_d.Close > last_d.ema_fast
              and last_d.Close > last_d.ema_mid
              and last_d.Close > last_d.sma_long)
    cond_w = (last_w.Close > last_w.ema_fast
              and last_w.Close > last_w.ema_mid
              and last_w.Close > last_w.sma_long)
    bull_d = last_d.Close > last_d.Open

    return cond_d and cond_w and bull_d

def send_telegram(message: str):
    """Envia uma mensagem para o Telegram via Bot API."""
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
        try:
            if check_symbol(sym):
                hits.append(sym)
        except Exception:
            # se der erro num ativo, ignora e segue
            pass

    if hits:
        msg = "*Radar Diário Automático*\n\n" + "\n".join(f"✅ {s}" for s in hits)
    else:
        msg = "❌ Nenhum ativo bateu o filtro hoje."

    send_telegram(msg)

if __name__ == "__main__":
    main()
