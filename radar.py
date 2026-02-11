import os
import datetime
import yfinance as yf
import requests
import pandas as pd

# — Seu TOKEN do Bot e chat_id via Secrets
TELEGRAM_TOKEN   = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

# =========================
# CONFIGURAÇÕES
# =========================
EMA_FAST = 20     # EMA 20
SMA_LONG = 200    # SMA 200

# Filtro de earnings
EXCLUIR_EARNINGS = True
EARNINGS_LOOKAHEAD_DIAS = 15  # próximos 15 dias

# Filtro de preço mínimo (USD)
PRECO_MIN_USD = 50.0

# Lista completa de tickers, sem o "ATVI"
TICKERS = [
    "AA","AAPL","ABBV","ABNB","ACN","ADBE","ADI","ADP","AEP","AIG","AKAM","AMAT","AMD",
    "AMGN","AMT","AMZN","ANET","ANSS","APPN","APPS","ATR","ATVI_removed","AVGO","AVY","AWK","AXON",
    "AXP","AZO","BA","BAC","BALL","BAX","BB","BBY","BDX","BEN","BF-B","BIDU","BIIB","BILI",
    "BK","BKNG","BLK","BMY","BNS","BRK-B","BSX","BURL","BX","BYD","BYND","BZUN","C","CAT",
    "CB","CBOE","CCI","CHD","CHGG","CHWY","CLX","CM","CMA","CMCSA","CME","CMG","CNC","COP",
    "COST","COUP_removed","CP","CPB","CPRI","CPRT","CRM","CRWD","CSCO","CSX","CTRA","CVNA","CVS","CVX",
    "CYBR","D","DAL","DAN","DBX","DD","DE","DELL","DG","DHR","DIS","DK","DKNG","DLR","DLTR",
    "DOCU","DT","DUK","DXC","DXCM","EA","EBAY","ECL","ED","EEFT","EIX","EL","ENB","ENPH","EPR",
    "ETR","ETSY","EVBG_removed","EXAS","EXPE","F","FANG","FCX","FDX","FHN","FITB","FIVE","FL","FLR",
    "FOX","FSLY","FTI","FTNT","GDS","GE","GILD","GM","GOOG","GPN","GRMN","GS","GT",
    "HBAN","HD","HLT","HOG","HOLX","HON","HP","HPQ","HRL","HUYA","IAC","IBKR","IBM","IDXX","ILMN",
    "INCY","INO","INTC","INTU","IRBT","ISRG","J","JNJ","JPM","JWN_removed","KEY","KLAC","KMB","KMX","KO",
    "LHX","LIN","LLY","LMT","LOW","LRCX","LULU","LUMN","LUV","LYFT","MA","MAA","MAC","MAR",
    "MASI","MAT","MCD","MDB","MDLZ","MDT","MDXG","MELI","META","MGM","MKC","MKTX","MLM","MMM",
    "MNST","MO","MPC","MRK","MRVL","MS","MSCI","MSFT","MTCH","MTZ","MU","NEE","NEM","NET",
    "NFLX","NICE","NKE","NOW","NTAP","NTRS","NVDA","NVO","NVR","NXPI","NXST","OC","OKE","OKTA",
    "OMC","ORCL","PAAS","PANW","PDD","PEP","PFE","PG","PGR","PH","PINS","PLD","PLNT","PLTR","PM",
    "PNC","PNR","PODD","POOL","PSO","PYPL","QCOM","RAD","RBLX","RDFN","RH","RNG","ROKU","RTX",
    "SBAC","SBUX","SE","SEDG","SFIX","SHAK","SHOP","SIRI","SKX","SNAP","SNOW","SPLK","SQ","STT","SWK","SYK",
    "T","TAP","TDG","TDOC","TEAM","TFC","THO","TJX","TMO","TMUS","TRV","TSLA","TSN","TTD","TWLO","TXN",
    "UAL","UBER","UI","UNH","UNP","UPS","URBN","USB","V","VMW","VZ","W","WBA","WDAY","WDC","WEN","WFC","WHR","WM","WTW","WYNN",
    "X","XEL","XOM","YELP","ZG","ZTS"
]

def send_telegram(msg: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"}
    requests.post(url, json=payload, timeout=20)

def safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

def get_next_earnings_date(ticker: yf.Ticker):
    """
    Tenta obter a próxima data de earnings via yfinance.
    Retorna um datetime.date ou None se não encontrar.
    """
    try:
        edf = ticker.get_earnings_dates(limit=8)
        if edf is None or edf.empty:
            return None

        dates = []
        for idx in edf.index:
            try:
                dates.append(pd.Timestamp(idx).date())
            except Exception:
                pass

        if not dates:
            return None

        today = datetime.date.today()
        future = sorted([d for d in dates if d >= today])
        return future[0] if future else None
    except Exception:
        return None

def should_exclude_by_earnings(ticker: yf.Ticker, lookahead_days: int) -> bool:
    next_e = get_next_earnings_date(ticker)
    if next_e is None:
        return False  # sem dado => não exclui

    today = datetime.date.today()
    limit = today + datetime.timedelta(days=lookahead_days)
    return today <= next_e <= limit

def get_last_price_usd(ticker: yf.Ticker):
    """
    Pega o último preço (regularMarketPrice) quando disponível.
    Fallback: último Close do histórico diário.
    """
    try:
        info = ticker.fast_info if hasattr(ticker, "fast_info") else None
        if info and "last_price" in info and info["last_price"] is not None:
            return safe_float(info["last_price"])
    except Exception:
        pass

    try:
        info2 = ticker.info
        p = info2.get("regularMarketPrice")
        if p is not None:
            return safe_float(p)
    except Exception:
        pass

    try:
        df = ticker.history(period="10d", interval="1d", auto_adjust=True)
        if df is not None and not df.empty:
            return safe_float(df["Close"].iloc[-1])
    except Exception:
        pass

    return None

def check_symbol(sym: str) -> bool:
    ticker = yf.Ticker(sym)

    # 0) FILTRO DE PREÇO MÍNIMO
    last_price = get_last_price_usd(ticker)
    if last_price is None:
        return False  # sem preço confiável, ignora
    if last_price < PRECO_MIN_USD:
        return False

    # 1) FILTRO DE EARNINGS (antes do histórico pesado)
    if EXCLUIR_EARNINGS and should_exclude_by_earnings(ticker, EARNINGS_LOOKAHEAD_DIAS):
        return False

    # 2) HISTÓRICO D1 / W1
    df_d = ticker.history(period="450d", interval="1d", auto_adjust=True)
    df_w = ticker.history(period="7y", interval="1wk", auto_adjust=True)

    if df_d.empty or df_w.empty:
        return False

    # Médias no D1
    df_d["ema20"]  = df_d["Close"].ewm(span=EMA_FAST, adjust=False).mean()
    df_d["sma200"] = df_d["Close"].rolling(window=SMA_LONG).mean()

    # Médias no W1
    df_w["ema20"]  = df_w["Close"].ewm(span=EMA_FAST, adjust=False).mean()
    df_w["sma200"] = df_w["Close"].rolling(window=SMA_LONG).mean()

    ld = df_d.iloc[-1]
    lw = df_w.iloc[-1]

    # Condições: acima das médias no D1 e W1
    cond_d = (ld["Close"] > ld["ema20"]) and (ld["Close"] > ld["sma200"])
    cond_w = (lw["Close"] > lw["ema20"]) and (lw["Close"] > lw["sma200"])

    # Condição extra: último candle diário positivo
    cond_d_pos = (ld["Close"] > ld["Open"])

    return bool(cond_d and cond_w and cond_d_pos)

def main():
    hoje = datetime.datetime.now(datetime.timezone.utc).astimezone(
        datetime.timezone(datetime.timedelta(hours=-3))
    ).strftime("%d/%m/%Y")

    hits = []
    for sym in TICKERS:
        if "_removed" in sym:
            continue
        try:
            if check_symbol(sym):
                hits.append(sym)
        except Exception:
            pass

    tags = []
    tags.append(f"Preço≥${PRECO_MIN_USD:g}")
    if EXCLUIR_EARNINGS:
        tags.append(f"Sem earnings ≤{EARNINGS_LOOKAHEAD_DIAS}d")
    tag_txt = " | ".join(tags)

    titulo = f"*🚀 Radar D1/W1 — EMA20+SMA200 & D1 Positivo ({hoje})*\n_{tag_txt}_\n\n"

    if hits:
        msg = titulo + f"*Ativos filtrados:* {', '.join(hits)}"
    else:
        msg = titulo + "Nenhum ativo no filtro hoje."

    send_telegram(msg)

if __name__ == "__main__":
    main()
