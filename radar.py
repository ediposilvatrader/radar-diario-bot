import os
import yfinance as yf
import requests
import pandas as pd

# — Seu TOKEN e CHAT_ID
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT  = os.getenv("TELEGRAM_CHAT_ID")

EMA_FAST = 21
EMA_MID  = 120
SMA_LONG = 200

# Apenas os tickers que queremos depurar
TICKERS = ["EXPE", "AMZN", "ORLY", "LIN"]

def check_symbol(sym: str, debug: bool = False) -> bool:
    df_d = yf.Ticker(sym).history(period="60d", interval="1d", auto_adjust=True)
    df_w = yf.Ticker(sym).history(period="5y", interval="1wk", auto_adjust=True)

    df_d["ema_fast"] = df_d["Close"].ewm(span=EMA_FAST, adjust=False).mean()
    df_d["ema_mid"]  = df_d["Close"].ewm(span=EMA_MID,  adjust=False).mean()
    df_d["sma_long"] = df_d["Close"].rolling(window=SMA_LONG).mean()

    df_w["ema_fast"] = df_w["Close"].ewm(span=EMA_FAST, adjust=False).mean()
    df_w["ema_mid"]  = df_w["Close"].ewm(span=EMA_MID,  adjust=False).mean()
    df_w["sma_long"] = df_w["Close"].rolling(window=SMA_LONG).mean()

    last_d = df_d.iloc[-1]
    last_w = df_w.iloc[-1]

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

    bars = df_d.tail(4)
    bear  = bars["Close"].iloc[0] < bars["Open"].iloc[0]
    bull1 = bars["Close"].iloc[1] > bars["Open"].iloc[1]
    bull2 = bars["Close"].iloc[2] > bars["Open"].iloc[2]
    bull3 = bars["Close"].iloc[3] > bars["Open"].iloc[3]
    pattern = bear and bull1 and bull2 and bull3

    if debug:
        print(f"\n>>> DEBUG {sym} <<<")
        print("Últimas 4 barras (Open, Close):")
        print(bars[["Open","Close"]].to_string())
        print(f"EMA21: {last_d.ema_fast:.2f}, EMA120: {last_d.ema_mid:.2f}, SMA200: {last_d.sma_long:.2f}")
        print(f"Fechamento: {last_d.Close:.2f}")
        print(f"cond_d (D1 acima EMAs+SMA): {cond_d}")
        print(f"cond_w (W1 acima EMAs+SMA): {cond_w}")
        print(f"pattern (↓↑↑↑): {pattern}")

    return cond_d and cond_w and pattern

def main():
    for sym in TICKERS:
        try:
            check_symbol(sym, debug=True)
        except Exception as e:
            print(f"Erro ao processar {sym}: {e}")

if __name__ == "__main__":
    main()
