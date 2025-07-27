import os
import datetime
import pandas as pd
import yfinance as yf
import requests
import pandas_market_calendars as mcal

# — Seus Secrets do GitHub
TELEGRAM_TOKEN        = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID_S1   = int(os.environ["TELEGRAM_CHAT_ID_S1"])
TELEGRAM_THREAD_ID_S1 = os.environ.get("TELEGRAM_THREAD_ID_S1")

# Parâmetros das médias
EMA_FAST = 21
EMA_MID  = 120
SMA_LONG = 200

# Lista completa de tickers, sem ATVI, COUP, EVBG e JWN
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
    "MRVL","MS","MSCI","MSFT","MTCH","MU","NEE","NET","NFLX","NICE","NKE","NOW","NTAP","NTRS","NVDA","NVO","NVR",
    "NXPI","OKTA","OMC","ORCL","PANW","PDD","PEP","PFE","PG","PGR","PH","PINS","PLD","PLNT","PLTR","PM",
    "PNC","PODD","POOL","PSO","PXD","PYPL","QCOM","RAD","RBLX","RDFN","RH","RNG","ROKU","RTX",
    "SBAC","SBUX","SE","SEDG","SFIX","SHAK","SHOP","SIRI","SKX","SNAP","SNOW","SPLK","SQ","STT","SWK","SYK",
    "T","TAP","TDG","TDOC","TEAM","TFC","THO","TJX","TMO","TMUS","TRV","TSLA","TSN","TTD","TWLO","TXN",
    "UAL","UBER","UNH","UNP","UPS","URBN","USB","V","VMW","VZ","W","WBA","WDAY","WDC","WEN","WFC","WHR","WM","WTW","WYNN",
    "X","XEL","XOM","YELP","ZG","ZTS"
]

# Padrões de barras: False = Bear, True = Bull
# usa as mesmas sequências do H1, mas agora aplicadas a 6 barras semanais
PATTERNS = [
    [True,  False, False, True,  True,  True],   # BULL BEAR BEAR BULL BULL BULL
    [False, False, True,  False, True,  True],   # BEAR BEAR BULL BEAR BULL BULL
    [False, False, True,  True,  True,  True],   # BEAR BEAR BULL BULL BULL BULL
    [False, False, True,  True,  False, True],   # BEAR BEAR BULL BULL BEAR BULL
    [False, False, False, True,  True,  True],   # BEAR BEAR BEAR BULL BULL BULL
]

def is_market_open(now_utc):
    cal   = mcal.get_calendar("NYSE")
    sched = cal.schedule(start_date=now_utc.date(), end_date=now_utc.date())
    return not sched.empty

def check_symbol_s1(sym: str):
    # — Operacional: Semanal (últimos ~104 semanas para SMA200)
    df_w = yf.Ticker(sym).history(period="2y", interval="1wk", auto_adjust=True)
    df_w["ema_fast"] = df_w["Close"].ewm(span=EMA_FAST).mean()
    df_w["ema_mid"]  = df_w["Close"].ewm(span=EMA_MID).mean()
    df_w["sma_long"] = df_w["Close"].rolling(window=SMA_LONG).mean()

    # últimas 6 barras semanais
    last6 = df_w.tail(6)
    bools = [(last6["Close"].iloc[i] > last6["Open"].iloc[i]) for i in range(6)]
    match_op = any(bools == p for p in PATTERNS)

    last_w = df_w.iloc[-1]
    cond_w = (
        last_w.Close > last_w.ema_fast and
        last_w.Close > last_w.ema_mid  and
        last_w.Close > last_w.sma_long
    )

    # — Viés: Mensal (últimos 400 meses para SMA200 mensal)
    df_m = yf.Ticker(sym).history(period="400mo", interval="1mo", auto_adjust=True)
    df_m["ema_fast"] = df_m["Close"].ewm(span=EMA_FAST).mean()
    df_m["ema_mid"]  = df_m["Close"].ewm(span=EMA_MID).mean()
    df_m["sma_long"] = df_m["Close"].rolling(window=SMA_LONG).mean()

    last_m = df_m.iloc[-1]
    cond_m = (
        last_m.Close > last_m.ema_fast and
        last_m.Close > last_m.ema_mid  and
        last_m.Close > last_m.sma_long
    )

    return match_op and cond_w and cond_m

def send_telegram(msg: str):
    url     = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id":    TELEGRAM_CHAT_ID_S1,
        "text":       msg,
        "parse_mode": "Markdown",
        **({"message_thread_id": int(TELEGRAM_THREAD_ID_S1)}
           if TELEGRAM_THREAD_ID_S1 else {})
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
            if check_symbol_s1(sym):
                hits.append(sym)
        except Exception as e:
            print(f"Erro {sym}: {e}")

    if hits:
        msg = f"*📊 Radar S1 US PDV — {timestamp}*\n\n*Sinais de Compra:* {', '.join(hits)}"
    else:
        msg = f"*📊 Radar S1 US PDV — {timestamp}*\n\nNenhum sinal encontrado agora."

    send_telegram(msg)

if __name__=="__main__":
    main()
