import os
import datetime
import pandas as pd
import yfinance as yf
import requests
import pandas_market_calendars as mcal

# — Seus Secrets do GitHub
TELEGRAM_TOKEN        = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID_H1   = int(os.environ["TELEGRAM_CHAT_ID_H1"])
TELEGRAM_THREAD_ID_H1 = os.environ.get("TELEGRAM_THREAD_ID_H1")

# Parâmetros das médias
EMA_FAST = 21
EMA_MID  = 120
SMA_LONG = 200

# Lista completa de tickers, sem ATVI, COUP, EVBG e JWN
TICKERS = [
    "AA","AAPL","ABBV","ABNB","ACN","ADBE","ADI","ADP","AEP","AIG","AKAM","AMAT","AMD",
    "AMGN","AMT","AMZN","ANET","ANSS","APPN","APPS","ATR","ATVI","AVGO","AVY","AWK","AXON",
    "AXP","AZO","BA","BAC","BALL","BAX","BB","BBY","BDX","BEN","BF-B","BIDU","BIIB","BILI",
    "BK","BKNG","BLK","BMY","BNS","BRK-B","BSX","BURL","BX","BYD","BYND","BZUN","C","CAT",
    "CB","CBOE","CCI","CHD","CHGG","CHWY","CLX","CM","CMA","CMCSA","CME","CMG","CNC","COP",
    "COST","COUP","CP","CPB","CPRI","CPRT","CRM","CRWD","CSCO","CSX","CTRA","CVNA","CVS","CVX",
    "CYBR","D","DAL","DAN","DBX","DD","DE","DELL","DG","DHR","DIS","DKNG","DLR","DLTR",
    "DOCU","DT","DUK","DXC","DXCM","EA","EBAY","ECL","ED","EEFT","EIX","EL","ENB","ENPH","EPR",
    "ETR","ETSY","EVBG","EXAS","EXPE","F","FANG","FCX","FDX","FHN","FITB","FIVE","FL","FLR",
    "FOX","FSLY","FTI","FTNT","GDS","GE","GILD","GM","GOOG","GPN","GRMN","GS","GT",
    "HBAN","HD","HLT","HOG","HOLX","HON","HP","HPQ","HRL","HUYA","IAC","IBKR","IBM","IDXX","ILMN",
    "INCY","INO","INTC","INTU","IRBT","ISRG","J","JNJ","JPM","JWN","KEY","KLAC","KMB","KMX","KO",
    "LHX","LIN","LLY","LMT","LOW","LRCX","LULU","LUMN","LUV","LYFT","MA","MAA","MAC","MAR",
    "MASI","MAT","MCD","MDB","MDLZ","MDT","MDXG","MELI","META","MGM","MKC","MKTX","MLM","MMM",
    "MNST","MO","MPC","MRK","MRVL","MS","MSCI","MSFT","MTCH","MTZ","MU","NEE","NEM","NET",
    "NFLX","NICE","NKE","NOW","NTAP","NTRS","NVDA","NVO","NVR","NXPI","NXST","OC","OKE","OKTA",
    "OMC","ORCL","PAAS","PANW","PDD","PEP","PFE","PG","PGR","PH","PINS","PLD","PLNT","PLTR","PM",
    "PNC","PODD","POOL","PSO","PYPL","QCOM","RAD","RBLX","RDFN","RH","RNG","ROKU","RTX",
    "SBAC","SBUX","SE","SEDG","SFIX","SHAK","SHOP","SIRI","SKX","SNAP","SNOW","SPLK","SQ","STT","SWK","SYK",
    "T","TAP","TDG","TDOC","TEAM","TFC","THO","TJX","TMO","TMUS","TRV","TSLA","TSN","TTD","TWLO","TXN",
    "UAL","UBER","UNH","UNP","UPS","URBN","USB","V","VMW","VZ","W","WBA","WDAY","WDC","WEN","WFC","WHR","WM","WTW","WYNN",
    "X","XEL","XOM","YELP","ZG","ZTS"
]

# Padrões de barras: False = Bear, True = Bull
PATTERNS = [
    [False, False, True,  True,  True],   # BEAR BEAR BULL BULL BULL
    [False, True,  False, True,  True],   # BEAR BULL BEAR BULL BULL
    [False, True,  True,  True,  True],   # BEAR BULL BULL BULL BULL
    [False, True,  True,  False, True],   # BEAR BULL BULL BEAR BULL
]

def is_market_open(now_utc):
    cal   = mcal.get_calendar("NYSE")
    sched = cal.schedule(start_date=now_utc.date(), end_date=now_utc.date())
    return not sched.empty

def check_symbol_h1(sym: str):
    # H1: fetch ~30 dias
    df_h1 = yf.Ticker(sym).history(period="30d", interval="1h", auto_adjust=True)
    df_h1["ema_fast"] = df_h1["Close"].ewm(span=EMA_FAST).mean()
    df_h1["ema_mid"]  = df_h1["Close"].ewm(span=EMA_MID).mean()
    df_h1["sma_long"] = df_h1["Close"].rolling(window=SMA_LONG).mean()

    last5 = df_h1.tail(5)
    bools = [(last5["Close"].iloc[i] > last5["Open"].iloc[i]) for i in range(5)]
    match_pattern = any(bools == p for p in PATTERNS)

    last_h1 = df_h1.iloc[-1]
    cond_h1  = (
        last_h1.Close > last_h1.ema_fast and
        last_h1.Close > last_h1.ema_mid  and
        last_h1.Close > last_h1.sma_long
    )

    # D1: fetch ~400d
    df_d1 = yf.Ticker(sym).history(period="400d", interval="1d", auto_adjust=True)
    df_d1["ema_fast"] = df_d1["Close"].ewm(span=EMA_FAST).mean()
    df_d1["ema_mid"]  = df_d1["Close"].ewm(span=EMA_MID).mean()
    df_d1["sma_long"] = df_d1["Close"].rolling(window=SMA_LONG).mean()

    last_d1 = df_d1.iloc[-1]
    cond_d1  = (
        last_d1.Close > last_d1.ema_fast and
        last_d1.Close > last_d1.ema_mid  and
        last_d1.Close > last_d1.sma_long
    )

    return match_pattern and cond_h1 and cond_d1

def send_telegram(msg: str):
    url     = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID_H1,
        "text":    msg,
        "parse_mode": "Markdown",
        **({"message_thread_id": int(TELEGRAM_THREAD_ID_H1)} 
           if TELEGRAM_THREAD_ID_H1 else {})
    }
    requests.post(url, json=payload)

def main():
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    if os.environ.get("GITHUB_EVENT_NAME")=="schedule" and not is_market_open(now_utc):
        print("Bolsa fechada ou feriado, pulando execução.")
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

if __name__ == "__main__":
    main()
