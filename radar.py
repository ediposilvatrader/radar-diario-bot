import os
import datetime
import pandas as pd
import yfinance as yf
import requests
import pandas_market_calendars as mcal

# — Seu TOKEN do Bot e chat_id via Secrets
TELEGRAM_TOKEN   = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

# Parâmetros das médias
EMA_FAST = 21
EMA_MID  = 120
SMA_LONG = 200

# Lista completa de tickers, sem o "ATVI"
TICKERS = [
    "AA","AAPL","ABBV","ABNB","ACN","ADBE","ADI","ADP","AEP","AIG","AKAM","AMAT","AMD",
    "AMGN","AMT","AMZN","ANET","ANSS","APPN","APPS","ATR","ATVI_removed","AVGO","AVY","AWK","AXON",
    "AXP","AZO","BA","BAC","BALL","BAX","BB","BBY","BDX","BEN","BF-B","BIDU","BIIB","BILI",
    "BK","BKNG","BLK","BMY","BNS","BRK-B","BSX","BURL","BX","BYD","BYND","BZUN","C","CAT",
    "CB","CBOE","CCI","CHD","CHGG","CHWY","CLX","CM","CMA","CMCSA","CME","CMG","CNC","COP",
    "COST","COUP_removed","CP","CPB","CPRI","CPRT","CRM","CRWD","CSCO","CSX","CTRA","CVNA","CVS","CVX",
    "CYBR","D","DAL","DAN","DBX","DD","DE","DELL","DG","DHR","DIS","DK","DKNG","DLR","DLTR",
    "DOCU","DT","DUK","DXC","DXCM","EA","EBAY","ECL","ED","EEFT","EIX","EL","ENB","ENPH","EPR",
    "ETR","ETSY","EVBG_removed","EXAS","EXPE","F","FANG","FCX","FDX","FHN","FITB","FIVE","FL","FLR",
    "FOX","FSLY","FTI","FTNT","GDS","GE","GILD","GM","GOOG","GPN","GRMN","GS","GT",
    "HBAN","HD","HLT","HOG","HOLX","HON","HP","HPQ","HRL","HUYA","IAC","IBKR","IBM","IDXX","ILMN",
    "INCY","INO","INTC","INTU","IRBT","ISRG","J","JNJ","JPM","JWN_removed","KEY","KLAC","KMB","KMX","KO",
    "LHX","LIN","LLY","LMT","LOW","LRCX","LULU","LUMN","LUV","LYFT","MA","MAA","MAC","MAR",
    "MASI","MAT","MCD","MDB","MDLZ","MDT","MDXG","MELI","META","MGM","MKC","MKTX","MLM","MMM",
    "MNST","MO","MPC","MRK","MRVL","MS","MSCI","MSFT","MTCH","MTZ","MU","NEE","NEM","NET",
    "NFLX","NICE","NKE","NOW","NTAP","NTRS","NVDA","NVO","NVR","NXPI","NXST","OC","OKE","OKTA",
    "OMC","ORCL","PAAS","PANW","PDD","PEP","PFE","PG","PGR","PH","PINS","PLD","PLNT","PLTR","PM",
    "PNC","PNR","PODD","POOL","PSO","PYPL","QCOM","RAD","RBLX","RDFN","RH","RNG","ROKU","RTX",
    "SBAC","SBUX","SE","SEDG","SFIX","SHAK","SHOP","SIRI","SKX","SNAP","SNOW","SPLK","SQ","STT","SWK","SYK",
    "T","TAP","TDG","TDOC","TEAM","TFC","THO","TJX","TMO","TMUS","TRV","TSLA","TSN","TTD","TWLO","TXN",
    "UAL","UBER","UI","UNH","UNP","UPS","URBN","USB","V","VMW","VZ","W","WBA","WDAY","WDC","WEN","WFC","WHR","WM","WTW","WYNN",
    "X","XEL","XOM","YELP","ZG","ZTS"
]

def is_market_open(now_utc):
    nyse = mcal.get_calendar("NYSE")
    sched = nyse.schedule(start_date=now_utc.date(), end_date=now_utc.date())
    return not sched.empty

def check_symbol(sym: str):
    df_d = yf.Ticker(sym).history(period="400d", interval="1d", auto_adjust=True)
    df_w = yf.Ticker(sym).history(period="5y", interval="1wk", auto_adjust=True)

    df_d["ema_fast"] = df_d["Close"].ewm(span=EMA_FAST).mean()
    df_d["ema_mid"]  = df_d["Close"].ewm(span=EMA_MID).mean()
    df_d["sma_long"] = df_d["Close"].rolling(window=SMA_LONG).mean()

    df_w["ema_fast"] = df_w["Close"].ewm(span=EMA_FAST).mean()
    df_w["ema_mid"]  = df_w["Close"].ewm(span=EMA_MID).mean()
    df_w["sma_long"] = df_w["Close"].rolling(window=SMA_LONG).mean()

    last4 = df_d.tail(4)
    o, c = last4["Open"].values, last4["Close"].values
    pattern = (c[0]<o[0] and c[1]>o[1] and c[2]>o[2] and c[3]>o[3])

    ld, lw = df_d.iloc[-1], df_w.iloc[-1]
    cond_d = (ld.Close>ld.ema_fast and ld.Close>ld.ema_mid and ld.Close>ld.sma_long)
    cond_w = (lw.Close>lw.ema_fast and lw.Close>lw.ema_mid and lw.Close>lw.sma_long)

    return pattern and cond_d and cond_w

def send_telegram(msg: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"}
    requests.post(url, json=payload)

def main():
    hoje = datetime.datetime.now(datetime.timezone.utc).astimezone(
        datetime.timezone(datetime.timedelta(hours=-3))
    ).strftime("%d/%m/%Y")

    hits = []
    for sym in TICKERS:
        if "_removed" in sym:
            continue
        try:
            if check_symbol(sym):
                hits.append(sym)
        except:
            pass

    if hits:
        msg = f"*🚀 Radar D1 US PDV — Sinais do dia: {hoje}*\n\n*Sinais de Compra:* {', '.join(hits)}"
    else:
        msg = f"*🚀 Radar D1 US PDV — Sinais do dia: {hoje}*\n\nNenhum sinal encontrado hoje."

    send_telegram(msg)

if __name__ == "__main__":
    main()
