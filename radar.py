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

# Lista de Tickers para checar (inclui EME e RS)
TICKERS = [
    "AAPL", "MSFT", "AMZN", "GOOGL", "TSLA",
    "META", "NVDA", "EME", "RS"
]

def check_symbol(sym):
    # Histórico diário (60 dias) e semanal (400 semanas para SMA200)
    df_d = yf.Ticker(sym).history(period="60d",  interval="1d")
    df_w = yf.Ticker(sym).history(period="400wk", interval="1wk")

    # Calcula médias no diário
    df_d["ema_fast"] = df_d["Close"].ewm(span=EMA_FAST, adjust=False).mean()
    df_d["ema_mid"]  = df_d["Close"].ewm(span=EMA_MID,  adjust=False).mean()
    df_d["sma_long"] = df_d["Close"].rolling(window=SMA_LONG).mean()

    # Calcula médias no semanal
    df_w["ema_fast"] = df_w["Close"].ewm(span=EMA_FAST, adjust=False).mean()
    df_w["ema_mid"]  = df_w["Close"].ewm(span=EMA_MID,  adjust=False).mean()
    df_w["sma_long"] = df_w["Close"].rolling(window=SMA_LONG).mean()

    # Últimos fechamentos
    last_d = df_d.iloc[-1]
    last_w = df_w.iloc[-1]

    # Condições de estar acima das 3 médias
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

    # Padrão: 1 barra de baixa seguida de 3 barras de alta nas últimas 4 barras do diário
    last4 = df_d.iloc[-4:]
    bar1 = last4.iloc[0]   # baixa
    bar2 = last4.iloc[1]   # alta
    bar3 = last4.iloc[2]   # alta
    bar4 = last4.iloc[3]   # alta
    pattern = (
        (bar1.Close < bar1.Open) and
        (bar2.Close > bar2.Open) and
        (bar3.Close > bar3.Open) and
        (bar4.Close > bar4.Open)
    )

    return cond_d and cond_w and pattern

def send_telegram(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id":    TELEGRAM_CHAT,
        "text":       message,
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
        except Exception as e:
            print(f"Erro ao processar {sym}: {e}")

    if hits:
        text = "*Radar Diário Automático*\n\n" + "\n".join(f"✅ {s}" for s in hits)
    else:
        text = "*Radar Diário Automático*\n\n❌ Nenhum ativo bateu o filtro hoje.*"

    send_telegram(text)

if __name__ == "__main__":
    main()
