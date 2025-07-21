import os
import yfinance as yf
import pandas as pd
import requests

# — Seu TOKEN do Bot (obtido no BotFather)
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")

# — O chat_id que você anotou
TELEGRAM_CHAT = os.environ.get("TELEGRAM_CHAT_ID")

# Períodos das médias
EMA_FAST = 21
EMA_MID  = 120
SMA_LONG = 200

# Lista de Tickers para checar (sem cifrões!)
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
    "PYPL","QCOM","RAD","RBLX","RDFN","RH","RNG","ROK","ROKU","RTX","SBAC","SBUX","SE","SEDG","SFIX",
    "SGEN","SHAK","SHOP","SIRI","SKX","SMAR","SNAP","SNOW","SPLK","SQ","STT","SWK","SYK","T","TAP","TDG",
    "TDOC","TEAM","TFC","THO","TJX","TMO","TMUS","TRV","TSLA","TSN","TTD","TWLO","TXN","UAL","UBER","UI",
    "UNH","UNP","UPS","URBN","USB","V","VMW","VZ","WBA","WDAY","WDC","WEN","WFC","WHR","WM","WTW","WWE",
    "WYNN","X","XEL","XOM","YELP","ZG","ZTS"
]

def clean_symbol(sym: str) -> str:
    """Remove cifrão e espaços extras."""
    return sym.strip().lstrip('$').upper()

def check_symbol(sym: str) -> bool:
    """Retorna True se o ativo bateu o filtro em D1 e W1."""
    sym = clean_symbol(sym)
    # Carrega histórico diário e semanal
    df_d = yf.Ticker(sym).history(period="300d", interval="1d", auto_adjust=True)
    df_w = yf.Ticker(sym).history(period="60wk", interval="1wk", auto_adjust=True)

    # Se não vier dados suficientes, pula
    if len(df_d) < SMA_LONG or len(df_w) < EMA_MID:
        return False

    # Calcula médias
    df_d["ema_fast"] = df_d["Close"].ewm(span=EMA_FAST, adjust=False).mean()
    df_d["ema_mid"]  = df_d["Close"].ewm(span=EMA_MID,  adjust=False).mean()
    df_d["sma_long"] = df_d["Close"].rolling(window=SMA_LONG).mean()

    df_w["ema_fast"] = df_w["Close"].ewm(span=EMA_FAST, adjust=False).mean()
    df_w["ema_mid"]  = df_w["Close"].ewm(span=EMA_MID,  adjust=False).mean()
    df_w["sma_long"] = df_w["Close"].rolling(window=SMA_LONG).mean()

    # Pega últimas 4 barras (1 de baixa + 3 de alta)
    last4 = df_d.tail(4)
    opens = last4["Open"].values
    closes = last4["Close"].values

    pattern = (
        closes[-4] < opens[-4] and  # barra de baixa
        closes[-3] > opens[-3] and  # 1ª alta
        closes[-2] > opens[-2] and  # 2ª alta
        closes[-1] > opens[-1]      # 3ª alta
    )

    # Condição preço > médias no D1 e W1
    ld = df_d.iloc[-1]
    lw = df_w.iloc[-1]
    cond_d = (ld.Close > ld.ema_fast) and (ld.Close > ld.ema_mid) and (ld.Close > ld.sma_long)
    cond_w = (lw.Close > lw.ema_fast) and (lw.Close > lw.ema_mid) and (lw.Close > lw.sma_long)

    return pattern and cond_d and cond_w

def send_telegram(msg: str):
    url     = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT, "text": msg, "parse_mode": "Markdown"}
    resp    = requests.post(url, json=payload)
    print("Telegram response:", resp.status_code, resp.text)

def main():
    hits = []
    for sym in TICKERS:
        try:
            if check_symbol(sym):
                hits.append(clean_symbol(sym))
        except Exception as e:
            print(f"{sym}: erro -> {e}")

    if hits:
        msg = "🚀 *Radar D1 US PDV*\n\n" + "*Sinais de Compra:* " + ", ".join(hits)
        send_telegram(msg)
    else:
        # sempre avisa, mesmo sem sinais
        send_telegram("🚀 *Radar D1 US PDV*\n\n_Nenhum sinal de compra encontrado hoje._")

if __name__ == "__main__":
    main()
