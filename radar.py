import os
import requests
import pandas as pd
import yfinance as yf

# — Variáveis de ambiente definidas no GitHub Actions
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# — Parâmetros das médias
EMA_FAST = 21
EMA_MID  = 120
SMA_LONG = 200

# — Lista completa de tickers
TICKERS = [
    "AA","AAPL","ABBV","ABNB","ACN","ADBE","ADI","ADP","AEP","AIG","AKAM","AMAT","AMD","AMGN",
    # ... toda a sua lista aqui ...
    "X","XEL","XOM","YELP","ZG","ZTS"
]

def check_symbol(sym: str) -> bool:
    """Retorna True se o ticker bate o padrão (barra baixa + 3 barras altas) e
       o preço estiver acima das 3 médias em D1 e W1."""
    try:
        # Baixa históricos com ajuste de dividendos
        df_d = yf.Ticker(sym).history(period="300d", interval="1d", actions=True)
        df_w = yf.Ticker(sym).history(period="10y", interval="1wk", actions=True)

        # Se não vier dado, pula
        if df_d.empty or df_w.empty:
            print(f"{sym}: sem dados (pode estar delisted)")
            return False

        # Calcula médias
        df_d["ema_fast"] = df_d["Close"].ewm(span=EMA_FAST).mean()
        df_d["ema_mid" ] = df_d["Close"].ewm(span=EMA_MID ).mean()
        df_d["sma_long"] = df_d["Close"].rolling(window=SMA_LONG).mean()

        df_w["ema_fast"] = df_w["Close"].ewm(span=EMA_FAST).mean()
        df_w["ema_mid" ] = df_w["Close"].ewm(span=EMA_MID ).mean()
        df_w["sma_long"] = df_w["Close"].rolling(window=SMA_LONG).mean()

        # Última linha de cada
        last_d = df_d.iloc[-1]
        last_w = df_w.iloc[-1]

        # 1 barra de baixa seguida de 3 barras de alta em D1
        recent = df_d.tail(4)
        toks = recent.reset_index()
        bearish = toks.loc[0, "Close"] < toks.loc[0, "Open"]
        three_bulls = all(
            toks.loc[i, "Close"] > toks.loc[i, "Open"]
            for i in [1,2,3]
        )
        pattern = bearish and three_bulls

        # Preço acima das 3 médias (D1 + W1)
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

        return pattern and cond_d and cond_w

    except Exception as e:
        print(f"{sym}: erro ao processar -> {e}")
        return False

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
    hits = [s for s in TICKERS if check_symbol(s)]

    header = "*🚀 Radar D1 US PDV*"
    if hits:
        body = "*Sinais de Compra:* (" + ", ".join(hits) + ")"
    else:
        body = "_Nenhum sinal encontrado hoje._"

    send_telegram(f"{header}\n\n{body}")

if __name__ == "__main__":
    main()
