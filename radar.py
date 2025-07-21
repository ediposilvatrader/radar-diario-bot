import os
import yfinance as yf
import pandas as pd
import requests

# — Seu TOKEN do Bot (obtido no BotFather)
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "SEU_TOKEN_AQUI")

# — O chat_id que você anotou
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "SEU_CHAT_ID_AQUI")

# Períodos das médias
EMA_FAST = 21
EMA_MID  = 120
SMA_LONG = 200

# Lista de Tickers para checar
TICKERS = [
    # ... sua lista completa aqui ...
    "AAPL","MSFT","AMZN","GOOGL","TSLA","META","NVDA",
    # etc.
]

def check_symbol(sym: str, debug: bool = False) -> bool:
    """
    Retorna True se:
      - No gráfico diário, a última barra fechou acima das 3 médias
      - No gráfico semanal, idem
      - E ocorreu padrão: barra de baixa seguida por 3 barras de alta
    Se debug=True, imprime os últimos dados para inspeção.
    """
    # Busca 60 dias / 26 semanas
    df_d = yf.Ticker(sym).history(period="60d", interval="1d", auto_adjust=True)
    df_w = yf.Ticker(sym).history(period="26wk", interval="1wk", auto_adjust=True)

    # Calcula médias
    df_d["ema_fast"] = df_d["Close"].ewm(span=EMA_FAST, adjust=False).mean()
    df_d["ema_mid"]  = df_d["Close"].ewm(span=EMA_MID, adjust=False).mean()
    df_d["sma_long"] = df_d["Close"].rolling(window=SMA_LONG).mean()

    df_w["ema_fast"] = df_w["Close"].ewm(span=EMA_FAST, adjust=False).mean()
    df_w["ema_mid"]  = df_w["Close"].ewm(span=EMA_MID, adjust=False).mean()
    df_w["sma_long"] = df_w["Close"].rolling(window=SMA_LONG).mean()

    # Valores finais
    last_d = df_d.iloc[-1]
    last_w = df_w.iloc[-1]

    # Condições de média
    cond_d = (last_d.Close > last_d.ema_fast
              and last_d.Close > last_d.ema_mid
              and last_d.Close > last_d.sma_long)
    cond_w = (last_w.Close > last_w.ema_fast
              and last_w.Close > last_w.ema_mid
              and last_w.Close > last_w.sma_long)

    # Padrão de barras: 4 últimas barras no diário
    # [i-4]: baixa, [i-3],[i-2],[i-1]: altas
    closes = df_d["Close"].values
    opens  = df_d["Open"].values
    if len(closes) < 4:
        bar_signal = False
    else:
        is_bear = closes[-4] < opens[-4]
        is_bull3 = all(closes[-i] > opens[-i] for i in (3,2,1))
        bar_signal = is_bear and is_bull3

    if debug:
        print(f"\n>>> {sym}")
        print(df_d[["Open","Close","ema_fast","ema_mid","sma_long"]].tail(4))
        print(f"Cond Diário (preço>EMAs+SMA): {cond_d}")
        print(f"Cond Semanal(preço>EMAs+SMA): {cond_w}")
        print(f"Sinal barras (↓↑↑↑): {bar_signal}")

    return cond_d and cond_w and bar_signal

def send_telegram(message: str) -> None:
    """Envia mensagem formatada em Markdown para o seu chat."""
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
        try:
            ok = check_symbol(sym, debug=True)
            if ok:
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
