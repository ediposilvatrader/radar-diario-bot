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
    "AMGN","AMT","AMZN","ANET","ANSS","APPN","APPS","ATR","ATVI","AVGO","AVY","AWK","AXON",
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
    "MCD","MDB","MDLZ","MDT","MDXG","MELI","META","MGM","MKC","MKTX","MLM","MMM","MNST","MO",
    "MPC","MRK","MRVL","MS","MSCI","MSFT","MTCH","MTZ","MU","NEE","NEM","NET","NFLX","NICE",
    "NKE","NOW","NTAP","NTRS","NVDA","NVO","NVR","NXPI","NXST","OKTA","OMC","ORCL","PAAS",
    "PANW","PDD","PEP","PFE","PG","PGR","PH","PINS","PLD","PLNT","PLTR","PM","PNC","PODD",
    "POOL","PSO","PYPL","QCOM","RAD","RBLX","RDFN","RH","RNG","ROKU","RTX","SBAC","SBUX",
    "SE","SEDG","SFIX","SHAK","SHOP","SIRI","SKX","SNAP","SNOW","SPLK","SQ","STT","SWK","SYK",
    "T","TAP","TDG","TDOC","TEAM","TFC","THO","TJX","TMO","TMUS","TRV","TSLA","TSN","TTD",
    "TWLO","TXN","UAL","UBER","UI","UNH","UNP","UPS","URBN","USB","V","VMW","VZ","W","WBA",
    "WDAY","WDC","WEN","WFC","WHR","WM","WTW","WYNN","X","XEL","XOM","YELP","ZG","ZTS"
]

# Debug apenas para estes 4 símbolos
DEBUG_SYMBOLS = ["EBAY","LHX","ED","ORLY"]

# Padrões de 6 barras semanal (True=Bull, False=Bear)
PATTERNS = [
    [ True, False, False,  True,  True,  True],
    [False, False,  True, False,  True,  True],
    [False, False,  True,  True,  True,  True],
    [False, False,  True,  True, False,  True],
    [False, False, False,  True,  True,  True],
]

def is_market_open(now_utc):
    cal   = mcal.get_calendar("NYSE")
    sched = cal.schedule(start_date=now_utc.date(), end_date=now_utc.date())
    return not sched.empty

def check_symbol_s1(sym: str, debug: bool=False):
    # Semanal
    df_w = yf.Ticker(sym).history(period="3y", interval="1wk", auto_adjust=True)
    if len(df_w) < 6:
        raise ValueError(f"{sym}: histórico semanal insuficiente ({len(df_w)} barras)")
    df_w["ema_fast_w"] = df_w["Close"].ewm(span=EMA_FAST).mean()
    df_w["ema_mid_w"]  = df_w["Close"].ewm(span=EMA_MID).mean()
    df_w["sma_long_w"] = df_w["Close"].rolling(window=SMA_LONG).mean()

    # Mensal
    df_m = yf.Ticker(sym).history(period="30y", interval="1mo", auto_adjust=True)
    if len(df_m) < SMA_LONG:
        raise ValueError(f"{sym}: histórico mensal insuficiente ({len(df_m)} barras)")
    df_m["ema_fast_m"] = df_m["Close"].ewm(span=EMA_FAST).mean()
    df_m["ema_mid_m"]  = df_m["Close"].ewm(span=EMA_MID).mean()
    df_m["sma_long_m"] = df_m["Close"].rolling(window=SMA_LONG).mean()

    # Pattern semanal
    last6 = df_w.tail(6)
    bools = [(last6["Close"].iloc[i] > last6["Open"].iloc[i]) for i in range(6)]
    match_pattern = any(bools == p for p in PATTERNS)

    # Condição fechamento semanal > médias semanais
    lw   = df_w.iloc[-1]
    cond_w = (
        lw.Close > lw.ema_fast_w and
        lw.Close > lw.ema_mid_w  and
        lw.Close > lw.sma_long_w
    )

    # Viés mensal: fechamento da última barra semanal vs médias mensais
    wm_close = lw.Close
    lm       = df_m.iloc[-1]
    cond_m   = (
        wm_close > lm.ema_fast_m and
        wm_close > lm.ema_mid_m  and
        wm_close > lm.sma_long_m
    )

    if debug:
        print(f"\n>>> DEBUG {sym} <<<")
        print("Semanal (últimas 6 barras O,C):")
        print(last6[["Open","Close"]])
        print("Bools:", bools)
        print("Casa padrão?", match_pattern)
        print(f"Semanal Close: {lw.Close:.2f} | EMA21w: {lw.ema_fast_w:.2f} EMA120w: {lw.ema_mid_w:.2f} SMA200w: {lw.sma_long_w:.2f}")
        print("Cond sem?", cond_w)
        print(f"Viés Mensal close={wm_close:.2f} | EMA21m: {lm.ema_fast_m:.2f} EMA120m: {lm.ema_mid_m:.2f} SMA200m: {lm.sma_long_m:.2f}")
        print("Cond mes?", cond_m, "\n")

    return match_pattern and cond_w and cond_m

def send_telegram(msg: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id":    TELEGRAM_CHAT_ID_S1,
        "text":       msg,
        "parse_mode": "Markdown",
        **({"message_thread_id": int(TELEGRAM_THREAD_ID_S1)} if TELEGRAM_THREAD_ID_S1 else {})
    }
    requests.post(url, json=payload)

def main():
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    if os.environ.get("GITHUB_EVENT_NAME") == "schedule" and not is_market_open(now_utc):
        print("Bolsa fechada ou feriado, pulando execução.")
        return

    ts  = now_utc.astimezone(datetime.timezone(datetime.timedelta(hours=-3))).strftime("%d/%m/%Y %H:%M")
    hits = []
    for sym in TICKERS:
        try:
            if check_symbol_s1(sym, debug=(sym in DEBUG_SYMBOLS)):
                hits.append(sym)
        except Exception as e:
            print(f"Erro {sym}: {e}")

    if hits:
        msg = f"*📊 Radar S1 US PDV — {ts}*\n\n*Sinais de Compra:* " + ", ".join(hits)
    else:
        msg = f"*📊 Radar S1 US PDV — {ts}*\n\nNenhum sinal encontrado agora."

    send_telegram(msg)

if __name__ == "__main__":
    main()
