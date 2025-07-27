import os
import datetime
import pandas as pd
import yfinance as yf
import requests
import pandas_market_calendars as mcal

# — Seu TOKEN do Bot (mesmo que o D1) e novos chat/thread para H1
TELEGRAM_TOKEN        = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID_H1   = int(os.environ["TELEGRAM_CHAT_ID_H1"])
TELEGRAM_THREAD_ID_H1 = os.environ.get("TELEGRAM_THREAD_ID_H1")

# Parâmetros das médias
EMA_FAST = 21
EMA_MID  = 120
SMA_LONG = 200

# Lista de tickers (sem ATVI, COUP, EVBG, JWN)
TICKERS = [
    "AA","AAPL","ABBV","ABNB","ACN","ADBE","ADI","ADP","AEP","AIG","AKAM","AMAT","AMD",
    "AMGN","AMT","AMZN","ANET","ANSS","APPN","APPS","ATR","AVGO","AVY","AWK","AXON",
    "AXP","AZO","BA","BAC","BALL","BAX","BB","BBY","BDX","BEN","BF-B","BIDU","BIIB","BILI",
    "BK","BKNG","BLK","BMY","BNS","BRK-B","BSX","BURL","BX","BYD","BYND","BZUN","C","CAT",
    "CB","CBOE","CCI","CHD","CHGG","CHWY","CLX","CM","CMA","CMCSA","CME","CMG","CNC","COP",
    "COST","CP","CPB","CPRI","CPRT","CRM","CRWD","CSCO","CSX","CTRA","CVNA","CVS","CVX",
    "CYBR","D","DAL","DD","DE","DELL","DG","DHR","DIS","DKNG","DLR","DLTR",
    "DOCU","DT","DUK","DXC","DXCM","EA","EBAY","ECL","ED","EEFT","EIX","EL","ENB","ENPH","EPR",
    "ETR","ETSY","EXAS","EXPE","F","FANG","FCX","FDX","FHN","FITB","FIVE","FL","FLR",
    "FOX","FSLY","FTI","FTNT","GE","GILD","GM","GOOG","GPN","GRMN","GS","GT",
    "HD","HLT","HOG","HOLX","HON","HP","HPQ","HRL","IBKR","IBM","IDXX","ILMN","INCY","INTC","INTU",
    "ISRG","JNJ","JPM","KEY","KLAC","KMB","KMX","KO","LHX","LIN","LLY","LMT","LOW","LRCX","LULU",
    "LUMN","LUV","MA","MAR","MCD","MDB","MDLZ","MDT","META","MGM","MKC","MMM","MNST","MO","MRK",
    "MRVL","MS","MSCI","MSFT","MTCH","MU","NEE","NET","NFLX","NKE","NOW","NTAP","NTRS","NVDA","NVO","NVR",
    "NXPI","OKTA","OMC","ORCL","PANW","PDD","PEP","PFE","PG","PGR","PH","PINS","PLD","PLNT","PLTR","PM",
    "PNC","PODD","POOL","PSO","PXD","PYPL","QCOM","RAD","RBLX","RH","RNG","ROKU","RTX",
    "SBAC","SBUX","SE","SEDG","SFIX","SHOP","SIRI","SKX","SNAP","SNOW","SPLK","SQ","STT","SWK","SYK",
    "T","TAP","TDG","TDOC","TEAM","TFC","THO","TJX","TMO","TMUS","TRV","TSLA","TSN","TTD","TWLO","TXN",
    "UAL","UBER","UNH","UNP","UPS","URBN","USB","V","VMW","VZ","W","WBA","WDAY","WDC","WEN","WFC","WHR","WM","WTW","WYNN",
    "X","XEL","XOM","YELP","ZG","ZTS"
]

def is_market_open(now_utc):
    """Pula execução em feriados/fds."""
    nyse = mcal.get_calendar("NYSE")
    sched = nyse.schedule(start_date=now_utc.date(), end_date=now_utc.date())
    return not sched.empty

def check_symbol_h1(sym: str):
    """
    1) Verifica padrão 1↓+3↑ e preço > EMA/SMA no H1
    2) Verifica preço > EMA/SMA no D1 (tendência)
    """
    # H1
    df_h1 = yf.Ticker(sym).history(period="30d", interval="1h", auto_adjust=True)
    df_h1["ema_fast"] = df_h1["Close"].ewm(span=EMA_FAST).mean()
    df_h1["ema_mid"]  = df_h1["Close"].ewm(span=EMA_MID).mean()
    df_h1["sma_long"] = df_h1["Close"].rolling(window=SMA_LONG).mean()
    last4 = df_h1.tail(4)
    o_h1, c_h1 = last4["Open"].values, last4["Close"].values
    pattern_h1 = (c_h1[0]<o_h1[0] and c_h1[1]>o_h1[1] and c_h1[2]>o_h1[2] and c_h1[3]>o_h1[3])
    last_h1 = df_h1.iloc[-1]
    cond_h1 = (last_h1.Close>last_h1.ema_fast and last_h1.Close>last_h1.ema_mid and last_h1.Close>last_h1.sma_long)

    # D1
    df_d1 = yf.Ticker(sym).history(period="400d", interval="1d", auto_adjust=True)
    df_d1["ema_fast"] = df_d1["Close"].ewm(span=EMA_FAST).mean()
    df_d1["ema_mid"]  = df_d1["Close"].ewm(span=EMA_MID).mean()
    df_d1["sma_long"] = df_d1["Close"].rolling(window=SMA_LONG).mean()
    last_d1 = df_d1.iloc[-1]
    cond_d1 = (last_d1.Close>last_d1.ema_fast and last_d1.Close>last_d1.ema_mid and last_d1.Close>last_d1.sma_long)

    return pattern_h1 and cond_h1 and cond_d1

def send_telegram(msg: str):
    """Envia no chat H1 (e tópico, se definido)."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID_H1,
        **({"message_thread_id": int(TELEGRAM_THREAD_ID_H1)} if TELEGRAM_THREAD_ID_H1 else {}),
        "text": msg,
        "parse_mode": "Markdown"
    }
    requests.post(url, json=payload)

def main():
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    # se cron, pula feriado/fds
    if os.environ.get("GITHUB_EVENT_NAME")=="schedule" and not is_market_open(now_utc):
        print("Bolsa fechada ou feriado, pulando.")
        return

    timestamp = now_utc.astimezone(
        datetime.timezone(datetime.timedelta(hours=-3))
    ).strftime("%d/%m/%Y %H:%M")

    hits = []
    for sym in TICKERS:
        try:
            if check_symbol_h1(sym):
                hits.append(sym)
        except Exception as e:
            print(f"Erro {sym}: {e}")

    if hits:
        msg = f"*⏰ Radar H1 US PDV — {timestamp}*\n\n*Sinais de Compra:* {', '.join(hits)}"
    else:
        msg = f"*⏰ Radar H1 US PDV — {timestamp}*\n\nNenhum sinal encontrado agora."

    send_telegram(msg)

if __name__=="__main__":
    main()
