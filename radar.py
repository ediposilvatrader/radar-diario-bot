import os
import yfinance as yf
import pandas as pd
import requests

# — Seu TOKEN e CHAT_ID via variáveis de ambiente
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT  = os.getenv("TELEGRAM_CHAT_ID")

# Parâmetros das médias
EMA_FAST = 21
EMA_MID  = 120
SMA_LONG = 200

# Lista completa de tickers
TICKERS = [
    "AA","AAPL","ABBV","ABNB","ACN","ADBE","ADI","ADP","AEP","AIG","AKAM",
    "AMAT","AMD","AMGN","AMT","AMZN","ANET","ANSS","APPN","APPS","ATR","ATVI",
    "AVGO","AVY","AWK","AXON","AXP","AZO","BA","BAC","BALL","BAX","BB","BBY",
    "BDX","BEN","BF-B","BIDU","BIIB","BILI","BK","BKNG","BLK","BMY","BNS",
    "BRK-B","BSX","BURL","BX","BYD","BYND","BZUN","C","CAT","CB","CBOE","CCI",
    "CHD","CHGG","CHWY","CLX","CM","CMA","CMCSA","CME","CMG","CNC","COP","COST",
    "COUP","CP","CPB","CPRI","CPRT","CRM","CRWD","CSCO","CSX","CTRA","CVNA","CVS",
    "CVX","CYBR","D","DAL","DAN","DBX","DD","DE","DELL","DG","DHR","DIS","DK",
    "DKNG","DLR","DLTR","DOCU","DT","DUK","DXC","DXCM","EA","EBAY","ECL","ED",
    "EEFT","EIX","EL","ENB","ENPH","EPR","ETR","ETSY","EVBG","EXAS","EXPE","F",
    "FANG","FCX","FDX","FHN","FITB","FIVE","FL","FLR","FLT","FOX","FSLY","FTI",
    "FTNT","GDS","GE","GILD","GM","GOLD","GOOG","GPN","GRMN","GS","GT","HBAN",
    "HD","HLT","HOG","HOLX","HON","HP","HPQ","HRL","HUYA","IAC","IBKR","IBM",
    "IDXX","ILMN","INCY","INO","INTC","INTU","IRBT","ISRG","J","JNJ","JPM","JWN",
    "KEY","KLAC","KMB","KMX","KO","LHX","LIN","LLY","LMT","LOW","LRCX","LTHM",
    "LULU","LUMN","LUV","LYFT","MA","MAA","MAC","MAR","MASI","MAT","MCD","MDB",
    "MDLZ","MDT","MDXG","MELI","META","MGM","MKC","MKTX","MLM","MMM","MNST","MO",
    "MPC","MRK","MRO","MRVL","MS","MSCI","MSFT","MTCH","MTZ","MU","NEE","NEM","NET",
    "NFLX","NICE","NKE","NOW","NTAP","NTRS","NVDA","NVO","NVR","NXPI","NXST","OC",
    "OKE","OKTA","OMC","ORCL","PAAS","PANW","PDD","PEP","PFE","PG","PGR","PH","PINS",
    "PLD","PLNT","PLTR","PM","PNC","PNR","PODD","POOL","PSO","PXD","PYPL","QCOM",
    "RAD","RBLX","RDFN","RH","RNG","ROKU","RTX","SBAC","SBUX","SE","SEDG","SFIX",
    "SGEN","SHAK","SHOP","SIRI","SKX","SMAR","SNAP","SNOW","SPLK","SQ","STT","SWK",
    "SYK","T","TAP","TDG","TDOC","TEAM","TFC","THO","TJX","TMO","TMUS","TRV","TSLA",
    "TSN","TTD","TWLO","TXN","UAL","UBER","UI","UNH","UNP","UPS","URBN","USB","V",
    "VMW","VZ","W","WBA","WDAY","WDC","WEN","WFC","WHR","WM","WTW","WWE","WYNN","X",
    "XEL","XOM","YELP","ZG","ZTS"
]

def check_symbol(sym):
    # usa auto_adjust para ajuste por dividendos e escala logarítmica não afeta cálculo
    df_d = yf.Ticker(sym).history(period="250d", interval="1d", auto_adjust=True)
    df_w = yf.Ticker(sym).history(period="250wk", interval="1wk", auto_adjust=True)

    df_d["ema_fast"] = df_d["Close"].ewm(span=EMA_FAST, adjust=False).mean()
    df_d["ema_mid"]  = df_d["Close"].ewm(span=EMA_MID, adjust=False).mean()
    df_d["sma_long"] = df_d["Close"].rolling(window=SMA_LONG).mean()

    df_w["ema_fast"] = df_w["Close"].ewm(span=EMA_FAST, adjust=False).mean()
    df_w["ema_mid"]  = df_w["Close"].ewm(span=EMA_MID, adjust=False).mean()
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
    bull_d = last_d.Close > last_d.Open

    return cond_d and cond_w and bull_d

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT, "text": message, "parse_mode": "Markdown"}
    resp = requests.post(url, json=payload)
    print("Telegram response:", resp.status_code, resp.text)

def main():
    hits = []
    for sym in TICKERS:
        try:
            if check_symbol(sym):
                hits.append(sym)
        except:
            continue

    if hits:
        msg = (
            "*Radar D1 US PDV*\n\n"
            f"Sinais de Compra: ({', '.join(hits)})"
        )
    else:
        msg = "*Radar D1 US PDV*\n\nNenhum sinal de compra hoje."

    send_telegram(msg)

if __name__ == "__main__":
    main()
