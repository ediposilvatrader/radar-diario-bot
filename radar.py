import os
import datetime
import pandas as pd
import yfinance as yf
import requests
import pandas_market_calendars as mcal

# — Seu TOKEN do Bot e chat_id via Secrets
TELEGRAM_TOKEN   = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = int(os.environ["TELEGRAM_CHAT_ID"])
# thread_id é opcional: pode não existir como secret
TELEGRAM_THREAD_ID = os.environ.get("TELEGRAM_THREAD_ID")

# Parâmetros das médias
EMA_FAST = 21
EMA_MID  = 120
SMA_LONG = 200

# Lista de tickers (sem ATVI, COUP, EVBG, JWN)
TICKERS = [
    "AA","AAPL","ABBV","ABNB","ACN","ADBE","ADI","ADP","AEP","AIG","AKAM","AMAT","AMD",
    "AMGN","AMT","AMZN","ANET","ANSS","APPN","APPS","ATR","ATVI","AVGO","AVY","AWK","AXON",
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
    "MRVL","MS","MSFT","MTCH","MU","NEE","NET","NFLX","NKE","NOW","NTAP","NTRS","NVDA","NVO","NVR",
    "NXPI","OKTA","OMC","ORCL","PANW","PDD","PEP","PFE","PG","PGR","PH","PINS","PLD","PLNT","PLTR",
    "PM","PNC","PODD","POOL","PSO","PXD","PYPL","QCOM","RAD","RBLX","RH","RNG","ROKU","RTX",
    "SBUX","SE","SEDG","SFIX","SHOP","SIRI","SKX","SNAP","SNOW","STT","SWK","SYK","T","TAP","TDG",
    "TDOC","TEAM","TMO","TRV","TSLA","TSN","TWLO","TXN","UAL","UBER","UNH","UNP","UPS","URBN",
    "USB","V","VMW","VZ","W","WBA","WDAY","WDC","WEN","WFC","WHR","WM","WYNN","X","XEL","XOM",
    "YELP","ZTS"
]

def is_market_open(now_utc):
    nyse = mcal.get_calendar("NYSE")
    sched = nyse.schedule(start_date=now_utc.date(), end_date=now_utc.date())
    if sched.empty:
        return False
    o = sched.iloc[0]["market_open"].tz_convert("UTC")
    c = sched.iloc[0]["market_close"].tz_convert("UTC")
    return o <= now_utc <= c

def check_symbol(sym: str):
    df_d = yf.Ticker(sym).history(period="400d", interval="1d", auto_adjust=True)
    df_w = yf.Ticker(sym).history(period="5y",  interval="1wk", auto_adjust=True)
    df_d["ema_fast"] = df_d["Close"].ewm(span=EMA_FAST).mean()
    df_d["ema_mid"]  = df_d["Close"].ewm(span=EMA_MID).mean()
    df_d["sma_long"] = df_d["Close"].rolling(window=SMA_LONG).mean()
    df_w["ema_fast"] = df_w["Close"].ewm(span=EMA_FAST).mean()
    df_w["ema_mid"]  = df_w["Close"].ewm(span=EMA_MID).mean()
    df_w["sma_long"] = df_w["Close"].rolling(window=SMA_LONG).mean()

    last4 = df_d.tail(4); o = last4["Open"].values; c = last4["Close"].values
    pattern = (c[0]<o[0] and c[1]>o[1] and c[2]>o[2] and c[3]>o[3])
    ld, lw = df_d.iloc[-1], df_w.iloc[-1]
    cond_d = ld.Close>ld.ema_fast and ld.Close>ld.ema_mid and ld.Close>ld.sma_long
    cond_w = lw.Close>lw.ema_fast and lw.Close>lw.ema_mid and lw.Close>lw.sma_long
    return pattern and cond_d and cond_w

def send_telegram(msg: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": msg,
        "parse_mode": "Markdown"
    }
    # só inclui thread_id se existir
    if TELEGRAM_THREAD_ID:
        payload["message_thread_id"] = int(TELEGRAM_THREAD_ID)
    requests.post(url, json=payload)

def main():
    now = datetime.datetime.now(datetime.timezone.utc)
    if os.environ.get("GITHUB_EVENT_NAME")=="schedule" and not is_market_open(now):
        print("Bolsa fechada ou feriado, pulando execução.")
        return

    hits = []
    for s in TICKERS:
        try:
            if check_symbol(s):
                hits.append(s)
        except Exception as e:
            print(f"Erro ao processar {s}: {e}")

    if hits:
        msg = "*🚀 Radar D1 US PDV*\n\n*Sinais de Compra:* " + ", ".join(hits)
    else:
        msg = "*🚀 Radar D1 US PDV*\n\nNenhum sinal encontrado hoje."

    send_telegram(msg)

if __name__=="__main__":
    main()
