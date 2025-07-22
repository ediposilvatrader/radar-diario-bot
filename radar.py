import os
import requests
import pandas as pd
import yfinance as yf
import pandas_market_calendars as mcal
from datetime import datetime

# — Seu TOKEN do Bot (obtido no BotFather) e chat_id vindos das Secrets
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT  = os.getenv("TELEGRAM_CHAT_ID")

# Períodos das médias
EMA_FAST = 21
EMA_MID  = 120
SMA_LONG = 200

# Lista de tickers (sem cifrão)
TICKERS = [
    "AA","AAPL","ABBV","ABNB","ACN","ADBE","ADI","ADP","AEP","AIG","AKAM","AMAT","AMD",
    "AMGN","AMT","AMZN","ANET","ANSS","APPN","APPS","ATR","ATVI","AVGO","AVY","AWK",
    "AXON","AXP","AZO","BA","BAC","BALL","BAX","BB","BBY","BDX","BEN","BF-B","BIDU",
    "BIIB","BILI","BK","BKNG","BLK","BMY","BNS","BRK-B","BSX","BURL","BX","BYD","BYND",
    "BZUN","C","CAT","CB","CBOE","CCI","CHD","CHGG","CHWY","CLX","CM","CMA","CMCSA",
    "CME","CMG","CNC","COP","COST","COUP","CP","CPB","CPRI","CPRT","CRM","CRWD","CSCO",
    "CSX","CTRA","CVNA","CVS","CVX","CYBR","D","DAL","DAN","DBX","DD","DE","DELL","DG",
    "DHR","DIS","DK","DKNG","DLR","DLTR","DOCU","DT","DUK","DXC","DXCM","EA","EBAY",
    "ECL","ED","EEFT","EIX","EL","ENB","ENPH","EPR","ETR","ETSY","EVBG","EXAS","EXPE",
    "F","FANG","FCX","FDX","FHN","FITB","FIVE","FL","FLR","FLT","FOX","FSLY","FTI","FTNT",
    "GDS","GE","GILD","GM","GOLD","GOOG","GPN","GRMN","GS","GT","HBAN","HD","HLT","HOG",
    "HOLX","HON","HP","HPQ","HRL","HUYA","IAC","IBKR","IBM","IDXX","ILMN","INCY","INO",
    "INTC","INTU","IRBT","ISRG","J","JNJ","JPM","JWN","KEY","KLAC","KMB","KMX","KO","LHX",
    "LIN","LLY","LMT","LOW","LRCX","LTHM","LULU","LUMN","LUV","LYFT","MA","MAA","MAC","MAR",
    "MASI","MAT","MCD","MDB","MDLZ","MDT","MDXG","MELI","META","MGM","MKC","MKTX","MLM","MMM",
    "MNST","MO","MPC","MRK","MRO","MRVL","MS","MSCI","MSFT","MTCH","MTZ","MU","NEE","NEM","NET",
    "NFLX","NICE","NKE","NOW","NTAP","NTRS","NVDA","NVO","NVR","NXPI","NXST","OC","OKE","OKTA",
    "OMC","ORCL","PAAS","PANW","PDD","PEP","PFE","PG","PGR","PH","PINS","PLD","PLNT","PLTR","PM",
    "PNC","PNR","PODD","POOL","PSO","PXD","PYPL","QCOM","RAD","RBLX","RDFN","RH","RNG","ROKU",
    "RTX","SBAC","SBUX","SE","SEDG","SFIX","SGEN","SHAK","SHOP","SIRI","SKX","SMAR","SNAP","SNOW",
    "SPLK","SQ","STT","SWK","SYK","T","TAP","TDG","TDOC","TEAM","TFC","THO","TJX","TMO","TMUS",
    "TRV","TSLA","TSN","TTD","TWLO","TXN","UAL","UBER","UI","UNH","UNP","UPS","URBN","USB","V",
    "VMW","VZ","W","WBA","WDAY","WDC","WEN","WFC","WHR","WM","WTW","WWE","WYNN","X","XEL","XOM",
    "YELP","ZG","ZTS",
]

def is_market_open(date: datetime) -> bool:
    # Usa o calendário da NYSE para pular feriados
    nyse = mcal.get_calendar("NYSE")
    schedule = nyse.schedule(start_date=date.date(), end_date=date.date())
    return not schedule.empty and date.time() >= schedule.iloc[0].market_close.time()

def check_symbol(sym: str) -> bool:
    # histórico diário e semanal
    df_d = yf.Ticker(sym).history(period="60d", interval="1d", auto_adjust=True)
    df_w = yf.Ticker(sym).history(period="26wk", interval="1wk", auto_adjust=True)

    # calcula médias
    df_d["ema_fast"] = df_d["Close"].ewm(span=EMA_FAST).mean()
    df_d["ema_mid"]  = df_d["Close"].ewm(span=EMA_MID ).mean()
    df_d["sma_long"] = df_d["Close"].rolling(window=SMA_LONG).mean()

    df_w["ema_fast"] = df_w["Close"].ewm(span=EMA_FAST).mean()
    df_w["ema_mid"]  = df_w["Close"].ewm(span=EMA_MID ).mean()
    df_w["sma_long"] = df_w["Close"].rolling(window=SMA_LONG).mean()

    # últimos fechamentos
    last_d = df_d.iloc[-1]
    last_w = df_w.iloc[-1]

    # condições de tendência
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

    # padrão de barras: baixa seguida de 3 de alta
    closes = df_d["Close"].iloc[-4:].values
    opens  = df_d["Open"].iloc[-4:].values
    pattern = (
        closes[0] < opens[0]    and
        all(closes[i] > opens[i] for i in [1,2,3])
    )

    return cond_d and cond_w and pattern

def send_telegram(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT,
        "text": message,
        "parse_mode": "Markdown"
    }
    resp = requests.post(url, json=payload)
    print("Telegram response:", resp.status_code, resp.text)

def main():
    now = datetime.utcnow()
    # só roda 30 min após fechamento e em dia de bolsa
    if not is_market_open(now):
        print("Bolsa fechada ou feriado, pulando execução.")
        return

    hits = [s for s in TICKERS if check_symbol(s)]
    if hits:
        msg = (
            "*Radar D1 US PDV*\n\n"
            "*Sinais de Compra:* " + ", ".join(hits)
        )
        send_telegram(msg)
    else:
        # envia também quando não há sinais
        send_telegram("*Radar D1 US PDV*\n\nNenhum sinal de compra hoje.")

if __name__ == "__main__":
    main()
