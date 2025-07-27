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

# Lista completa sem ATVI, COUP, EVBG, JWN
TICKERS = [
    "AA","AAPL","ABBV","ABNB","ACN","ADBE","ADI","ADP","AEP","AIG","AKAM","AMAT","AMD",
    "AMGN","AMT","AMZN","ANET","ANSS","APPN","APPS","ATR","AVGO","AVY","AWK","AXON",
    "AXP","AZO","BA","BAC","BALL","BAX","BB","BBY","BDX","BEN","BF-B","BIDU","BIIB","BILI",
    "BK","BKNG","BLK","BMY","BNS","BRK-B","BSX","BURL","BX","BYD","BYND","BZUN","C","CAT",
    "CB","CBOE","CCI","CHD","CHGG","CHWY","CLX","CM","CMA","CMCSA","CME","CMG","CNC","COP",
    "COST","CP","CPB","CPRI","CPRT","CRM","CRWD","CSCO","CSX","CTRA","CVNA","CVS","CVX",
    "CYBR","D","DAL","DBX","DD","DE","DELL","DG","DHR","DIS","DKNG","DLR","DLTR","DOCU","DT",
    "DUK","DXC","DXCM","EA","EBAY","ECL","ED","EEFT","EIX","EL","ENB","ENPH","EPR","ETR",
    "ETSY","EXAS","EXPE","F","FANG","FCX","FDX","FHN","FITB","FIVE","FL","FLR","FOX","FSLY",
    "FTI","FTNT","GDS","GE","GILD","GM","GOOG","GPN","GRMN","GS","GT","HBAN","HD","HLT",
    "HOG","HOLX","HON","HP","HPQ","HRL","HUYA","IAC","IBKR","IBM","IDXX","ILMN","INCY","INO",
    "INTC","INTU","IRBT","ISRG","J","JNJ","JPM","KEY","KLAC","KMB","KMX","KO","LHX","LIN",
    "LLY","LMT","LOW","LRCX","LULU","LUMN","LUV","LYFT","MA","MAA","MAC","MAR","MASI","MAT",
    "MCD","MDB","MDLZ","MDT","META","MGM","MKC","MMM","MNST","MO","MPC","MRK","MRVL","MS",
    "MSCI","MSFT","MTCH","MU","NEE","NET","NFLX","NICE","NKE","NOW","NTAP","NTRS","NVDA","NVO",
    "NVR","NXPI","OKTA","OMC","ORCL","PAAS","PANW","PDD","PEP","PFE","PG","PGR","PH","PINS",
    "PLD","PLNT","PLTR","PM","PNC","PODD","POOL","PSO","PYPL","QCOM","RAD","RBLX","RDFN","RH",
    "RNG","ROKU","RTX","SBAC","SBUX","SE","SEDG","SFIX","SHAK","SHOP","SIRI","SKX","SNAP",
    "SNOW","SPLK","SQ","STT","SWK","SYK","T","TAP","TDG","TDOC","TEAM","TFC","THO","TJX",
    "TMO","TMUS","TRV","TSLA","TSN","TTD","TWLO","TXN","UAL","UBER","UI","UNH","UNP","UPS",
    "URBN","USB","V","VMW","VZ","W","WBA","WDAY","WDC","WEN","WFC","WHR","WM","WTW","WYNN",
    "X","XEL","XOM","YELP","ZG","ZTS"
]

# Padrões de 6 velas semanais (True=Bull, False=Bear)
BUY_PATTERNS = [
    [ True, False, False,  True,  True,  True],
    [False, False,  True, False,  True,  True],
    [False, False,  True,  True,  True,  True],
    [False, False,  True,  True, False,  True],
    [False, False, False,  True,  True,  True],
]
# Inverte cada padrão para venda
SELL_PATTERNS = [[not b for b in p] for p in BUY_PATTERNS]

def is_market_open(now_utc):
    sched = mcal.get_calendar("NYSE").schedule(start_date=now_utc.date(), end_date=now_utc.date())
    return not sched.empty

def check_symbol_s1(sym: str, patterns, above: bool):
    # Histórico semanal (5 anos)
    df_w = yf.Ticker(sym).history(period="5y", interval="1wk", auto_adjust=True)
    if len(df_w) < 6:
        return False
    df_w["ema_fast_w"] = df_w["Close"].ewm(span=EMA_FAST).mean()
    df_w["ema_mid_w"]  = df_w["Close"].ewm(span=EMA_MID).mean()
    df_w["sma_long_w"] = df_w["Close"].rolling(window=SMA_LONG).mean()

    # Histórico mensal (20 anos)
    df_m = yf.Ticker(sym).history(period="20y", interval="1mo", auto_adjust=True)
    if len(df_m) < SMA_LONG:
        return False
    df_m["ema_fast_m"] = df_m["Close"].ewm(span=EMA_FAST).mean()
    df_m["ema_mid_m"]  = df_m["Close"].ewm(span=EMA_MID).mean()
    df_m["sma_long_m"] = df_m["Close"].rolling(window=SMA_LONG).mean()

    # Extrai últimas 6 velas semanais
    last6 = df_w.tail(6)
    opens = last6["Open"].values
    closes = last6["Close"].values

    # Monta lista de bools com gap-check
    bools = []
    for i in range(6):
        if closes[i] > opens[i]:
            if i == 0 or closes[i] > closes[i-1]:
                bools.append(True)
            else:
                bools.append(False)
        else:
            bools.append(False)

    # Verifica padrão
    if not any(bools == p for p in patterns):
        return False

    # Condição preço acima/abaixo médias semanais
    lw = df_w.iloc[-1]
    if above:
        if not (lw.Close > lw.ema_fast_w and lw.Close > lw.ema_mid_w and lw.Close > lw.sma_long_w):
            return False
    else:
        if not (lw.Close < lw.ema_fast_w and lw.Close < lw.ema_mid_w and lw.Close < lw.sma_long_w):
            return False

    # Viés mensal: fechamento semanal vs médias mensais
    wm_close = lw.Close
    lm = df_m.iloc[-1]
    if above:
        return wm_close > lm.ema_fast_m and wm_close > lm.ema_mid_m and wm_close > lm.sma_long_m
    else:
        return wm_close < lm.ema_fast_m and wm_close < lm.ema_mid_m and wm_close < lm.sma_long_m

def send_telegram(msg: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID_S1,
        "text": msg,
        "parse_mode": "Markdown",
        **({"message_thread_id": int(TELEGRAM_THREAD_ID_S1)} if TELEGRAM_THREAD_ID_S1 else {})
    }
    requests.post(url, json=payload)

def main():
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    if os.environ.get("GITHUB_EVENT_NAME")=="schedule" and not is_market_open(now_utc):
        return

    ts = now_utc.astimezone(datetime.timezone(datetime.timedelta(hours=-3))).strftime("%d/%m/%Y %H:%M")

    buys, sells = [], []
    for sym in TICKERS:
        try:
            if check_symbol_s1(sym, BUY_PATTERNS, above=True):
                buys.append(sym)
            if check_symbol_s1(sym, SELL_PATTERNS, above=False):
                sells.append(sym)
        except Exception:
            continue

    header = f"*📊 Radar S1 US PDV — {ts}*\n\n"
    body = ""
    if buys:
        body += "*Sinais de Compra:* " + ", ".join(buys) + "\n\n"
    else:
        body += "Nenhum sinal de compra.\n\n"
    if sells:
        body += "*Sinais de Venda:* " + ", ".join(sells)
    else:
        body += "Nenhum sinal de venda."

    send_telegram(header + body)

if __name__=="__main__":
    main()
