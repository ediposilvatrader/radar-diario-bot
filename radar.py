import os
import yfinance as yf
import pandas as pd
import requests

# — Seu TOKEN do Bot (via Secrets do GitHub Actions)
TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
# — O chat_id (via Secrets do GitHub Actions)
TELEGRAM_CHAT  = os.environ["TELEGRAM_CHAT_ID"]

# Períodos das médias
EMA_FAST = 21
EMA_MID  = 120
SMA_LONG = 200

# Lista de Tickers para checar
TICKERS = ["AAPL","MSFT","AMZN","GOOGL","TSLA","META","NVDA","EME"]

def check_symbol(sym):
    # Baixa 60 dias de diário e 400 semanas de semanal (para calcular SMA200)
    df_d = yf.Ticker(sym).history(period="60d",  interval="1d")
    df_w = yf.Ticker(sym).history(period="400wk", interval="1wk")

    # — calcula médias no diário
    df_d["ema_fast"] = df_d["Close"].ewm(span=EMA_FAST, adjust=False).mean()
    df_d["ema_mid"]  = df_d["Close"].ewm(span=EMA_MID,  adjust=False).mean()
    df_d["sma_long"] = df_d["Close"].rolling(window=SMA_LONG).mean()

    # — calcula médias no semanal
    df_w["ema_fast"] = df_w["Close"].ewm(span=EMA_FAST, adjust=False).mean()
    df_w["ema_mid"]  = df_w["Close"].ewm(span=EMA_MID,  adjust=False).mean()
    df_w["sma_long"] = df_w["Close"].rolling(window=SMA_LONG).mean()

    # pega a última barra semanal e diária
    last_d = df_d.iloc[-1]
    last_w = df_w.iloc[-1]

    # condições de estar acima das médias
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

    # extrai as últimas 4 barras diárias para o padrão 1 bear + 3 bulls
    last4 = df_d.iloc[-4:]
    bar1 = last4.iloc[0]
    bar2 = last4.iloc[1]
    bar3 = last4.iloc[2]
    bar4 = last4.iloc[3]

    is_bear = bar1.Close < bar1.Open
    is_bull2 = bar2.Close > bar2.Open
    is_bull3 = bar3.Close > bar3.Open
    is_bull4 = bar4.Close > bar4.Open

    pattern = is_bear and is_bull2 and is_bull3 and is_bull4

    return cond_d and cond_w and pattern

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id":    TELEGRAM_CHAT,
        "text":       message,
        "parse_mode": "Markdown"
    }
    resp = requests.post(url, json=payload)
    print("Telegram response:", resp.status_code, resp.text)

def main():
    sinais = []
    for sym in TICKERS:
        try:
            if check_symbol(sym):
                sinais.append(sym)
        except Exception as e:
            print(f"Erro ao processar {sym}: {e}")

    if sinais:
        texto = "*Radar Diário Automático*\n\n" + "\n".join(f"✅ {s}" for s in sinais)
    else:
        texto = "🚨 Nenhum ativo bateu o filtro hoje."

    send_telegram(texto)

if __name__ == "__main__":
    main()
