import os
import yfinance as yf
import pandas as pd
import requests

# — Seu TOKEN do Bot (obtido no BotFather) — agora vindo de variável de ambiente
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
# — O chat_id que você anotou — também via variável de ambiente
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Períodos das médias
EMA_FAST = 21      # exponencial curta
EMA_MID  = 120     # exponencial média
SMA_LONG = 200     # simples longa

# Lista completa de tickers (NASDAQ e NYSE)
TICKERS = [
    "AA","AAPL","ABBV","ABNB","ACN","ADBE","ADI","ADP","AEP","AIG","AKAM",
    "AMAT","AMD","AMGN","AMT","AMZN","ANET","ANSS","APPN","APPS","ATR","ATVI",
    "AVGO","AVY","AWK","AXON","AXP","AZO","BA","BAC","BALL","BAX","BB","BBY",
    "BDX","BEN","BF-B","BIDU","BIIB","BILI","BK","BKNG","BLK","BMY","BNS","BRK-B",
    "BSX","BURL","BX","BYD","BYND","BZUN","C","CAT","CB","CBOE","CCI","CHD","CHGG",
    "CHWY","CLX","CM","CMA","CMCSA","CME","CMG","CNC","COP","COST","COUP","CP","CPB",
    "CPRI","CPRT","CRM","CRWD","CSCO","CSX","CTRA","CVNA","CVS","CVX","CYBR","D","DAL",
    "DAN","DBX","DD","DE","DELL","DG","DHR","DIS","DK","DKNG","DLR","DLTR","DOCU","DT",
    "DUK","DXC","DXCM","EA","EBAY","ECL","ED","EEFT","EIX","EL","ENB","ENPH","EPR","ETR",
    "ETSY","EVBG","EXAS","EXPE","F","FANG","FCX","FDX","FHN","FITB","FIVE","FL","FLR",
    "FLT","FOX","FSLY","FTI","FTNT","GDS","GE","GILD","GM","GOLD","GOOG","GPN","GRMN",
    "GS","GT","HBAN","HD","HLT","HOG","HOLX","HON","HP","HPQ","HRL","HUYA","IAC","IBKR",
    "IBM","IDXX","ILMN","INCY","INO","INTC","INTU","IRBT","ISRG","J","JNJ","JPM","JWN",
    "KEY","KLAC","KMB","KMX","KO","LHX","LIN","LLY","LMT","LOW","LRCX","LTHM","LULU","LUMN",
    "LUV","LYFT","MA","MAA","MAC","MAR","MASI","MAT","MCD","MDB","MDLZ","MDT","MDXG","MELI",
    "META","MGM","MKC","MKTX","MLM","MMM","MNST","MO","MPC","MRK","MRO","MRVL","MS","MSCI",
    "MSFT","MTCH","MTZ","MU","NEE","NEM","NET","NFLX","NICE","NKE","NOW","NTAP","NTRS","NVDA",
    "NVO","NVR","NXPI","NXST","OC","OKE","OKTA","OMC","ORCL","PAAS","PANW","PDD","PEP","PFE",
    "PG","PGR","PH","PINS","PLD","PLNT","PLTR","PM","PNC","PNR","PODD","POOL","PSO","PXD","PYPL",
    "QCOM","RAD","RBLX","RDFN","RH","RNG","ROKU","RTX","SBAC","SBUX","SE","SEDG","SFIX","SGEN",
    "SHAK","SHOP","SIRI","SKX","SMAR","SNAP","SNOW","SPLK","SQ","STT","SWK","SYK","T","TAP",
    "TDG","TDOC","TEAM","TFC","THO","TJX","TMO","TMUS","TRV","TSLA","TSN","TTD","TWLO","TXN",
    "UAL","UBER","UI","UNH","UNP","UPS","URBN","USB","V","VMW","VZ","W","WBA","WDAY","WDC",
    "WEN","WFC","WHR","WM","WTW","WWE","WYNN","X","XEL","XOM","YELP","ZG","ZTS"
]

def check_symbol(sym: str) -> bool:
    # busca diário e semanal com histórico suficiente
    df_d = yf.Ticker(sym).history(period="300d", interval="1d", auto_adjust=True)
    df_w = yf.Ticker(sym).history(period="260wk", interval="1wk", auto_adjust=True)

    # se não tiver bars suficientes não processa
    if len(df_d) < SMA_LONG or len(df_w) < EMA_MID:
        return False

    # calculo das médias
    df_d["ema_fast"] = df_d["Close"].ewm(span=EMA_FAST, adjust=False).mean()
    df_d["ema_mid"]  = df_d["Close"].ewm(span=EMA_MID, adjust=False).mean()
    df_d["sma_long"] = df_d["Close"].rolling(window=SMA_LONG).mean()

    df_w["ema_fast"] = df_w["Close"].ewm(span=EMA_FAST, adjust=False).mean()
    df_w["ema_mid"]  = df_w["Close"].ewm(span=EMA_MID, adjust=False).mean()
    df_w["sma_long"] = df_w["Close"].rolling(window=SMA_LONG).mean()

    # últimos valores
    ld = df_d.iloc[-1]
    lw = df_w.iloc[-1]

    # condições de posição acima das médias
    cond_d = ld.Close > ld.ema_fast and ld.Close > ld.ema_mid and ld.Close > ld.sma_long
    cond_w = lw.Close > lw.ema_fast and lw.Close > lw.ema_mid and lw.Close > lw.sma_long

    # padrão de barras: baixa seguida de três altas
    last4 = df_d[["Open","Close"]].tail(4)
    pattern = (
        (last4["Close"].iloc[0] < last4["Open"].iloc[0]) and    # barra de baixa
        (last4["Close"].iloc[1] > last4["Open"].iloc[1]) and    # 1ª alta
        (last4["Close"].iloc[2] > last4["Open"].iloc[2]) and    # 2ª alta
        (last4["Close"].iloc[3] > last4["Open"].iloc[3])        # 3ª alta
    )

    return cond_d and cond_w and pattern

def send_telegram(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    resp = requests.post(url, json=payload)
    if not resp.ok:
        print("Telegram response:", resp.status_code, resp.text)

def main():
    hits = []
    for sym in TICKERS:
        try:
            if check_symbol(sym):
                hits.append(sym)
        except Exception as e:
            # ignora símbolos inválidos ou erros de download
            continue

    if hits:
        msg = (
            "*📊 Radar D1 US PDV*\n\n"
            f"*Sinais de Compra:* {', '.join(hits)}"
        )
        send_telegram(msg)
    else:
        # sempre envia, mesmo sem sinais
        send_telegram("*📊 Radar D1 US PDV*\n\nNenhum sinal encontrado hoje.")

if __name__ == "__main__":
    main()
