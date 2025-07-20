import os
import yfinance as yf
import pandas as pd
import requests
from datetime import datetime
import pytz

# — Seu TOKEN do Bot (obtido no BotFather)
TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]

# — O chat_id do seu grupo ou canal
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

# Períodos das médias
EMA_FAST = 21
EMA_MID  = 120
SMA_LONG = 200

# Lista completa de Tickers para checar
TICKERS = [
    "AA","AAPL","ABBV","ABNB","ACN","ADBE","ADI","ADP","AEP","AIG","AKAM","AMAT","AMD","AMGN","AMT",
    "AMZN","ANET","ANSS","APPN","APPS","ATR","ATVI","AVGO","AVY","AWK","AXON","AXP","AZO","BA","BAC",
    "BALL","BAX","BB","BBY","BDX","BEN","BF-B","BIDU","BIIB","BILI","BK","BKNG","BLK","BMY","BNS",
    "BRK-B","BSX","BURL","BX","BYD","BYND","BZUN","C","CAT","CB","CBOE","CCI","CHD","CHGG","CHWY",
    "CLX","CM","CMA","CMCSA","CME","CMG","CNC","COP","COST","COUP","CP","CPB","CPRI","CPRT","CRM",
    "CRWD","CSCO","CSX","CTRA","CVNA","CVS","CVX","CYBR","D","DAL","DAN","DBX","DD","DE","DELL","DG",
    "DHR","DIS","DK","DKNG","DLR","DLTR","DOCU","DT","DUK","DXC","DXCM","EA","EBAY","ECL","ED","EEFT",
    "EIX","EL","ENB","ENPH","EPR","ETR","ETSY","EVBG","EXAS","EXPE","F","FANG","FCX","FDX","FHN","FITB",
    "FIVE","FL","FLR","FLT","FOX","FSLY","FTI","FTNT","GDS","GE","GILD","GM","GOLD","GOOG","GPN","GRMN",
    "GS","GT","HBAN","HD","HLT","HOG","HOLX","HON","HP","HPQ","HRL","HUYA","IAC","IBKR","IBM","IDXX",
    "ILMN","INCY","INO","INTC","INTU","IRBT","ISRG","J","JNJ","JPM","JWN","KEY","KLAC","KMB","KMX","KO",
    "LHX","LIN","LLY","LMT","LOW","LRCX","LTHM","LULU","LUMN","LUV","LYFT","MA","MAA","MAC","MAR","MASI",
    "MAT","MCD","MDB","MDLZ","MDT","MDXG","MELI","META","MGM","MKC","MKTX","MLM","MMM","MNST","MO","MPC",
    "MRK","MRO","MRVL","MS","MSCI","MSFT","MTCH","MTZ","MU","NEE","NEM","NET","NFLX","NICE","NKE","NOW",
    "NTAP","NTRS","NVDA","NVO","NVR","NXPI","NXST","OC","OKE","OKTA","OMC","ORCL","PAAS","PANW","PDD",
    "PEP","PFE","PG","PGR","PH","PINS","PLD","PLNT","PLTR","PM","PNC","PNR","PODD","POOL","PSO","PXD",
    "PYPL","QCOM","RAD","RBLX","RDFN","RH","RNG","ROKU","RTX","SBAC","SBUX","SE","SEDG","SFIX","SGEN",
    "SHAK","SHOP","SIRI","SKX","SMAR","SNAP","SNOW","SPLK","SQ","STT","SWK","SYK","T","TAP","TDG","TDOC",
    "TEAM","TFC","THO","TJX","TMO","TMUS","TRV","TSLA","TSN","TTD","TWLO","TXN","UAL","UBER","UI","UNH",
    "UNP","UPS","URBN","USB","V","VMW","VZ","W","WBA","WDAY","WDC","WEN","WFC","WHR","WM","WTW","WWE",
    "WYNN","X","XEL","XOM","YELP","ZG","ZTS"
]

def check_symbol(sym):
    # Histórico diário e semanal
    d = yf.Ticker(sym).history(period="60d", interval="1d")
    w = yf.Ticker(sym).history(period="26wk", interval="1wk")

    # Calcula as médias
    d["ema_fast"] = d["Close"].ewm(span=EMA_FAST).mean()
    d["ema_mid" ] = d["Close"].ewm(span=EMA_MID).mean()
    d["sma_long"] = d["Close"].rolling(window=SMA_LONG).mean()

    w["ema_fast"] = w["Close"].ewm(span=EMA_FAST).mean()
    w["ema_mid" ] = w["Close"].ewm(span=EMA_MID).mean()
    w["sma_long"] = w["Close"].rolling(window=SMA_LONG).mean()

    # Últimas barras
    last_d = d.iloc[-1]
    last_w = w.iloc[-1]

    # Condições de filtro
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
    # Últimas três barras de alta após uma de baixa
    seq = d["Close"] > d["Open"]
    last3 = seq.iloc[-3:].all()           # três últimas são alta
    prev1 = not seq.iloc[-4]              # a quarta antes é baixa

    return cond_d and cond_w and prev1 and last3

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
    hits = []
    for sym in TICKERS:
        try:
            if check_symbol(sym):
                hits.append(sym)
        except Exception as e:
            print(f"Erro em {sym}: {e}")

    now = datetime.now(pytz.timezone("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M")
    if hits:
        msg = f"*Radar Diário* — {now}\n\n" + "\n".join(f"✅ {s}" for s in hits)
    else:
        msg = f"*Radar Diário* — {now}\n\nNenhum ativo bateu o filtro hoje."
    send_telegram(msg)

if __name__ == "__main__":
    main()
