import os
import datetime
import pandas as pd
import yfinance as yf
import requests
import pandas_market_calendars as mcal

# — Seu TOKEN do Bot e chat_id via Secrets
TELEGRAM_TOKEN   = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
# Thread (forum topic) opcional no Telegram
TELEGRAM_THREAD_ID = os.getenv("TELEGRAM_THREAD_ID")

# Parâmetros das médias
EMA_FAST = 21
EMA_MID  = 120
SMA_LONG = 200

# Lista completa de tickers (sem cifrão)
TICKERS = [
    "AA","AAPL","ABBV","ABNB","ACN","ADBE","ADI","ADP","AEP","AIG","AKAM","AMAT","AMD",
    "AMGN","AMT","AMZN","ANET","ANSS","APPN","APPS","ATR","ATVI","AVGO","AVY","AWK","AXON",
    "AXP","AZO","BA","BAC","BALL","BAX","BB","BBY","BDX","BEN","BF-B","BIDU","BIIB","BILI",
    "BK","BKNG","BLK","BMY","BNS","BRK-B","BSX","BURL","BX","BYD","BYND","BZUN","C","CAT",
    "CB","CBOE","CCI","CHD","CHGG","CHWY","CLX","CM","CMA","CMCSA","CME","CMG","CNC","COP",
    "COST","COUP","CP","CPB","CPRI","CPRT","CRM","CRWD","CSCO","CSX","CTRA","CVNA","CVS","CVX",
    "CYBR","D","DAL","DAN","DBX","DD","DE","DELL","DG","DHR","DIS","DK","DKNG","DLR","DLTR",
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
    "PNC","PNR","PODD","POOL","PSO","PYPL","QCOM","RAD","RBLX","RDFN","RH","RNG","ROKU","RTX",
    "SBAC","SBUX","SE","SEDG","SFIX","SHAK","SHOP","SIRI","SKX","SNAP","SNOW",
    "STT","SWK","SYK","T","TAP","TDG","TDOC","TEAM","TFC","THO","TJX","TMO","TMUS","TRV","TSLA",
    "TSN","TTD","TWLO","TXN","UAL","UBER","UI","UNH","UNP","UPS","URBN","USB","V","VMW","VZ","W",
    "WBA","WDAY","WDC","WEN","WFC","WHR","WM","WTW","WYNN","X","XEL","XOM","YELP","ZG","ZTS"
]

def is_market_open(now_utc):
    """Verifica se NYSE está aberta no momento UTC dado."""
    nyse = mcal.get_calendar("NYSE")
    sched = nyse.schedule(start_date=now_utc.date(), end_date=now_utc.date())
    if sched.empty:
        return False
    open_utc  = sched.iloc[0]["market_open"].tz_convert("UTC")
    close_utc = sched.iloc[0]["market_close"].tz_convert("UTC")
    return open_utc <= now_utc <= close_utc

def check_symbol(sym: str):
    """
    Retorna True se o símbolo bate o padrão:
     - 1 barra de baixa + 3 barras de alta no diário
     - Preço do último candle acima de EMA21, EMA120 e SMA200 (D1 e W1).
    """
    # histórico diário: pelo menos 400 dias para SMA200
    df_d = yf.Ticker(sym).history(period="400d", interval="1d", auto_adjust=True)
    # histórico semanal: pelo menos 5 anos para SMA200 semanal
    df_w = yf.Ticker(sym).history(period="5y", interval="1wk", auto_adjust=True)

    # calcula médias no diário
    df_d["ema_fast"] = df_d["Close"].ewm(span=EMA_FAST).mean()
    df_d["ema_mid"]  = df_d["Close"].ewm(span=EMA_MID).mean()
    df_d["sma_long"] = df_d["Close"].rolling(window=SMA_LONG).mean()

    # calcula médias no semanal
    df_w["ema_fast"] = df_w["Close"].ewm(span=EMA_FAST).mean()
    df_w["ema_mid"]  = df_w["Close"].ewm(span=EMA_MID).mean()
    df_w["sma_long"] = df_w["Close"].rolling(window=SMA_LONG).mean()

    # padrão de barras no diário: 1 de baixa seguida de 3 de alta
    last4 = df_d.tail(4)
    opens  = last4["Open"].values
    closes = last4["Close"].values
    pattern = (
        closes[0] < opens[0] and
        closes[1] > opens[1] and
        closes[2] > opens[2] and
        closes[3] > opens[3]
    )

    # condições de preço > médias (último candle)
    ld = df_d.iloc[-1]
    lw = df_w.iloc[-1]
    cond_d = (ld.Close > ld.ema_fast and ld.Close > ld.ema_mid and ld.Close > ld.sma_long)
    cond_w = (lw.Close > lw.ema_fast and lw.Close > lw.ema_mid and lw.Close > lw.sma_long)

    return pattern and cond_d and cond_w

def send_telegram(msg: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": msg,
        "parse_mode": "Markdown"
    }
    # se definiram THREAD_ID (forum topic), anexa ao payload
    if TELEGRAM_THREAD_ID:
        payload["message_thread_id"] = int(TELEGRAM_THREAD_ID)
    requests.post(url, json=payload)

def main():
    now = datetime.datetime.now(datetime.timezone.utc)

    # pular fim de semana/feriado em schedule
    if os.environ.get("GITHUB_EVENT_NAME") == "schedule":
        if not is_market_open(now):
            print("Bolsa fechada ou feriado, pulando execução.")
            return

    hits = []
    for sym in TICKERS:
        try:
            if check_symbol(sym):
                hits.append(sym)
        except Exception as e:
            print(f"Erro ao processar {sym}: {e}")

    # monta e envia mensagem
    if hits:
        msg = "*🚀 Radar D1 US PDV*\n\n*Sinais de Compra:* " + ", ".join(hits)
    else:
        msg = "*🚀 Radar D1 US PDV*\n\nNenhum sinal encontrado hoje."

    send_telegram(msg)

if __name__ == "__main__":
    main()
