import os
import datetime
import pandas as pd
import yfinance as yf
import requests
import pandas_market_calendars as mcal

# — Seu TOKEN do Bot (obtido no BotFather)
TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
# — O chat_id que você anotou
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

# Períodos das médias
EMA_FAST = 21
EMA_MID  = 120
SMA_LONG = 200

# Lista de Tickers para checar (exemplo reduzido, coloque a sua lista completa aqui)
TICKERS = [
    "AAPL","MSFT","AMZN","GOOGL","TSLA","META","NVDA",
    # ... + todos os outros que você passou ...
]

def is_market_open(now_utc):
    """
    Retorna True se NYSE estiver aberta em now_utc.
    Usa pandas_market_calendars pra feriados e horário de verão.
    """
    nyse = mcal.get_calendar("NYSE")
    schedule = nyse.schedule(start_date=now_utc.date(), end_date=now_utc.date())
    if schedule.empty:
        return False
    open_time = schedule.iloc[0]["market_open"].tz_convert("UTC")
    close_time = schedule.iloc[0]["market_close"].tz_convert("UTC")
    return open_time <= now_utc <= close_time

def check_symbol(sym):
    """
    Retorna True se:
      - preço diário > EMAs (21,120) e SMA(200)
      - preço semanal > EMAs (21,120) e SMA(200)
      - padrão de barras: 1 de baixa + 3 de alta no diário
    """
    # histórico diário e semanal
    df_d = yf.Ticker(sym).history(period="60d", interval="1d", auto_adjust=True)
    df_w = yf.Ticker(sym).history(period="26wk", interval="1wk", auto_adjust=True)

    # cálculos de médias
    df_d["ema_fast"] = df_d["Close"].ewm(span=EMA_FAST).mean()
    df_d["ema_mid"]  = df_d["Close"].ewm(span=EMA_MID).mean()
    df_d["sma_long"] = df_d["Close"].rolling(SMA_LONG).mean()

    df_w["ema_fast"] = df_w["Close"].ewm(span=EMA_FAST).mean()
    df_w["ema_mid"]  = df_w["Close"].ewm(span=EMA_MID).mean()
    df_w["sma_long"] = df_w["Close"].rolling(SMA_LONG).mean()

    # pega últimas 4 barras diárias (1 de baixa + 3 de alta)
    last4 = df_d.tail(4)
    opens = last4["Open"].values
    closes = last4["Close"].values

    cond_pattern = (
        closes[0] < opens[0] and  # primeira de baixa
        closes[1] > opens[1] and  # 3 de alta
        closes[2] > opens[2] and
        closes[3] > opens[3]
    )

    # última barra diária e semanal
    last_d = df_d.iloc[-1]
    last_w = df_w.iloc[-1]

    cond_d = (
        last_d.Close > last_d.ema_fast and
        last_d.Close > last_d.ema_mid and
        last_d.Close > last_d.sma_long
    )
    cond_w = (
        last_w.Close > last_w.ema_fast and
        last_w.Close > last_w.ema_mid and
        last_w.Close > last_w.sma_long
    )

    return cond_pattern and cond_d and cond_w

def send_telegram(msg: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": msg,
        "parse_mode": "Markdown"
    }
    resp = requests.post(url, json=payload)
    print("Telegram response:", resp.status_code, resp.text)

def main():
    # pega data/hora UTC atual
    now = datetime.datetime.now(datetime.timezone.utc)

    # bloqueia em feriados/fim de semana **só** quando veio do schedule
    if os.environ.get("GITHUB_EVENT_NAME") == "schedule":
        if not is_market_open(now):
            print("Bolsa fechada ou feriado, pulando execução.")
            return

    hits = []
    for sym in TICKERS:
        try:
            if check_symbol(sym):
                hits.append(sym)
        except Exception as e:
            # só log de erro, continua
            print(f"Erro em {sym}: {e}")

    # monta mensagem
    if hits:
        msg  = "*Radar D1 US PDV*\n"
        msg += "\n*Sinais de Compra:* " + ", ".join(hits)
        send_telegram(msg)
    else:
        send_telegram("*Radar D1 US PDV*\n\nNenhum sinal encontrado hoje.")

if __name__ == "__main__":
    main()
