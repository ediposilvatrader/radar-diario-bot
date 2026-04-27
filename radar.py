import os
import datetime
import zoneinfo
import schedule
import time
import yfinance as yf
import requests
import pandas as pd

# — Seu TOKEN do Bot e chat_id via Secrets
TELEGRAM_TOKEN   = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

# =========================
# CONFIGURAÇÕES
# =========================
EMA_FAST  = 21    # EMA 21
EMA_MID   = 120   # EMA 120
SMA_LONG  = 200   # SMA 200

# Filtro de earnings
EXCLUIR_EARNINGS       = True
EARNINGS_LOOKAHEAD_DIAS = 15

# Filtro de preço mínimo (USD)
PRECO_MIN_USD = 50.0

# Padrão de barras das últimas 5 candles fechadas no D1
# True = bull (close > open), False = bear (close < open)
PADRAO_BARRAS = [False, False, True, True, True]  # bear, bear, bull, bull, bull

TICKERS = [
    "AA","AAPL","ABBV","ABNB","ACN","ADBE","ADI","ADP","AEP","AIG","AKAM","AMAT","AMD",
    "AMGN","AMT","AMZN","ANET","ANSS","APPN","APPS","ATR","AVGO","AVY","AWK","AXON",
    "AXP","AZO","BA","BAC","BALL","BAX","BB","BBY","BDX","BEN","BF-B","BIDU","BIIB","BILI",
    "BK","BKNG","BLK","BMY","BNS","BRK-B","BSX","BURL","BX","BYD","BYND","BZUN","C","CAT",
    "CB","CBOE","CCI","CHD","CHGG","CHWY","CLX","CM","CMA","CMCSA","CME","CMG","CNC","COP",
    "COST","CP","CPB","CPRI","CPRT","CRM","CRWD","CSCO","CSX","CTRA","CVNA","CVS","CVX",
    "CYBR","D","DAL","DAN","DBX","DD","DE","DELL","DG","DHR","DIS","DK","DKNG","DLR","DLTR",
    "DOCU","DT","DUK","DXC","DXCM","EA","EBAY","ECL","ED","EEFT","EIX","EL","ENB","ENPH","EPR",
    "ETR","ETSY","EXAS","EXPE","F","FANG","FCX","FDX","FHN","FITB","FIVE","FL","FLR",
    "FOX","FSLY","FTI","FTNT","GDS","GE","GILD","GM","GOOG","GPN","GRMN","GS","GT",
    "HBAN","HD","HLT","HOG","HOLX","HON","HP","HPQ","HRL","HUYA","IAC","IBKR","IBM","IDXX","ILMN",
    "INCY","INO","INTC","INTU","IRBT","ISRG","J","JNJ","JPM","KEY","KLAC","KMB","KMX","KO",
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

# =======================
# HELPERS
# =======================

def send_telegram(msg: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"}
    try:
        requests.post(url, json=payload, timeout=20)
    except Exception as e:
        print(f"Erro ao enviar Telegram: {e}")

def safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

def get_next_earnings_date(ticker: yf.Ticker):
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
        return False
    today = datetime.date.today()
    limit = today + datetime.timedelta(days=lookahead_days)
    return today <= next_e <= limit

def get_last_price_usd(ticker: yf.Ticker):
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

    # 0) Preço mínimo
    last_price = get_last_price_usd(ticker)
    if last_price is None or last_price < PRECO_MIN_USD:
        return False

    # 1) Earnings filter
    if EXCLUIR_EARNINGS and should_exclude_by_earnings(ticker, EARNINGS_LOOKAHEAD_DIAS):
        return False

    # 2) Histórico semanal e diário
    df_d = ticker.history(period="600d", interval="1d", auto_adjust=True)
    df_w = ticker.history(period="7y",   interval="1wk", auto_adjust=True)

    if df_d is None or df_w is None or df_d.empty or df_w.empty:
        return False

    # Mínimo de barras para calcular SMA200
    if len(df_d) < 205 or len(df_w) < 205:
        return False

    # --- Médias no D1 ---
    df_d["ema21"]  = df_d["Close"].ewm(span=EMA_FAST, adjust=False).mean()
    df_d["ema120"] = df_d["Close"].ewm(span=EMA_MID,  adjust=False).mean()
    df_d["sma200"] = df_d["Close"].rolling(window=SMA_LONG).mean()

    # --- Médias no W1 ---
    df_w["ema21"]  = df_w["Close"].ewm(span=EMA_FAST, adjust=False).mean()
    df_w["ema120"] = df_w["Close"].ewm(span=EMA_MID,  adjust=False).mean()
    df_w["sma200"] = df_w["Close"].rolling(window=SMA_LONG).mean()

    ld = df_d.iloc[-1]
    lw = df_w.iloc[-1]

    # Condição: preço acima das 3 médias no D1
    cond_d = (
        ld["Close"] > ld["ema21"]  and
        ld["Close"] > ld["ema120"] and
        ld["Close"] > ld["sma200"]
    )

    # Condição: preço acima das 3 médias no W1
    cond_w = (
        lw["Close"] > lw["ema21"]  and
        lw["Close"] > lw["ema120"] and
        lw["Close"] > lw["sma200"]
    )

    if not (cond_d and cond_w):
        return False

    # --- Padrão das últimas 5 barras fechadas no D1 ---
    # Barras fechadas = excluindo a última candle se ainda em formação
    # Usamos as últimas 6 barras e pegamos as 5 mais antigas (as fechadas)
    if len(df_d) < 6:
        return False

    # As 5 barras FECHADAS mais recentes (posições -6 a -2, excluindo a última/atual)
    ultimas_5 = df_d.iloc[-6:-1]

    padrao_ok = True
    for i, (_, row) in enumerate(ultimas_5.iterrows()):
        esperado_bull = PADRAO_BARRAS[i]
        real_bull     = row["Close"] > row["Open"]
        if real_bull != esperado_bull:
            padrao_ok = False
            break

    return padrao_ok

# =======================
# MAIN JOB
# =======================

def run_radar():
    tz_brt = zoneinfo.ZoneInfo("America/Sao_Paulo")
    agora  = datetime.datetime.now(tz_brt)
    hoje   = agora.strftime("%d/%m/%Y %H:%M")

    print(f"[{hoje}] Iniciando radar...")

    hits = []
    for sym in TICKERS:
        try:
            if check_symbol(sym):
                hits.append(sym)
                print(f"  ✅ {sym}")
        except Exception as e:
            print(f"  ⚠️  {sym}: {e}")

    tags = [
        f"Preço≥${PRECO_MIN_USD:g}",
        "EMA21 + EMA120 + SMA200 (D1 e W1)",
        "Padrão: 🔴🔴🟢🟢🟢",
    ]
    if EXCLUIR_EARNINGS:
        tags.append(f"Sem earnings ≤{EARNINGS_LOOKAHEAD_DIAS}d")
    tag_txt = " | ".join(tags)

    titulo = (
        f"*🚀 Radar D1/W1 — 3 Médias + Padrão Bear/Bear/Bull/Bull/Bull*\n"
        f"_{hoje}_\n"
        f"_{tag_txt}_\n\n"
    )

    if hits:
        msg = titulo + f"*Ativos com sinal:* {', '.join(hits)}"
    else:
        msg = titulo + "_Nenhum ativo com sinal hoje._"

    send_telegram(msg)
    print(f"[{hoje}] Radar finalizado. {len(hits)} sinal(is) enviado(s).")

# =======================
# AGENDAMENTO
# =======================

if __name__ == "__main__":
    # Agenda todos os dias às 18:30 no horário de Brasília
    schedule.every().day.at("18:30").do(run_radar)

    print("Radar agendado para 18:30 (horário de Brasília) todos os dias.")
    print("Pressione Ctrl+C para parar.\n")

    # Executa imediatamente na primeira vez se quiser testar:
    # run_radar()

    while True:
        schedule.run_pending()
        time.sleep(30)
