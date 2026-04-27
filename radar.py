import os
import datetime
import zoneinfo
import yfinance as yf
import requests
import pandas as pd

# — Secrets do GitHub Actions
TELEGRAM_TOKEN   = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

# =========================
# CONFIGURAÇÕES
# =========================
EMA_FAST  = 21
EMA_MID   = 120
SMA_LONG  = 200

EXCLUIR_EARNINGS        = True
EARNINGS_LOOKAHEAD_DIAS = 15

PRECO_MIN_USD = 50.0

# Padrão das últimas 4 barras FECHADAS no D1
# False = bear (close < open) | True = bull (close > open)
PADRAO_BARRAS = [False, True, True, True]  # bear, bull, bull, bull

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
        print(f"Erro Telegram: {e}")

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
        today = datetime.date.today()
        dates = []
        for idx in edf.index:
            try:
                dates.append(pd.Timestamp(idx).date())
            except Exception:
                pass
        future = sorted([d for d in dates if d >= today])
        return future[0] if future else None
    except Exception:
        return None

def should_exclude_by_earnings(ticker: yf.Ticker, lookahead_days: int) -> bool:
    next_e = get_next_earnings_date(ticker)
    if next_e is None:
        return False
    today = datetime.date.today()
    return today <= next_e <= today + datetime.timedelta(days=lookahead_days)

def get_last_price_usd(ticker: yf.Ticker):
    try:
        info = ticker.fast_info
        if hasattr(info, "last_price") and info.last_price is not None:
            return safe_float(info.last_price)
    except Exception:
        pass
    try:
        p = ticker.info.get("regularMarketPrice")
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

    # 1) Earnings
    if EXCLUIR_EARNINGS and should_exclude_by_earnings(ticker, EARNINGS_LOOKAHEAD_DIAS):
        return False

    # 2) Histórico
    df_d = ticker.history(period="600d", interval="1d",  auto_adjust=True)
    df_w = ticker.history(period="7y",   interval="1wk", auto_adjust=True)

    if df_d is None or df_w is None or df_d.empty or df_w.empty:
        return False
    if len(df_d) < 205 or len(df_w) < 205:
        return False

    # Médias D1
    df_d["ema21"]  = df_d["Close"].ewm(span=EMA_FAST, adjust=False).mean()
    df_d["ema120"] = df_d["Close"].ewm(span=EMA_MID,  adjust=False).mean()
    df_d["sma200"] = df_d["Close"].rolling(window=SMA_LONG).mean()

    # Médias W1
    df_w["ema21"]  = df_w["Close"].ewm(span=EMA_FAST, adjust=False).mean()
    df_w["ema120"] = df_w["Close"].ewm(span=EMA_MID,  adjust=False).mean()
    df_w["sma200"] = df_w["Close"].rolling(window=SMA_LONG).mean()

    ld = df_d.iloc[-1]
    lw = df_w.iloc[-1]

    # Preço acima das 3 médias no D1
    cond_d = (
        ld["Close"] > ld["ema21"] and
        ld["Close"] > ld["ema120"] and
        ld["Close"] > ld["sma200"]
    )
    # Preço acima das 3 médias no W1
    cond_w = (
        lw["Close"] > lw["ema21"] and
        lw["Close"] > lw["ema120"] and
        lw["Close"] > lw["sma200"]
    )

    if not (cond_d and cond_w):
        return False

    # Padrão das últimas 4 barras FECHADAS (exclui candle atual)
    if len(df_d) < 5:
        return False

    ultimas_4 = df_d.iloc[-5:-1]  # 4 barras fechadas mais recentes

    # 1) Verificar direção de cada barra (bear/bull)
    for i, (_, row) in enumerate(ultimas_4.iterrows()):
        esperado_bull = PADRAO_BARRAS[i]
        real_bull     = row["Close"] > row["Open"]
        if real_bull != esperado_bull:
            return False

    # 2) Verificar fechamentos crescentes (cada close > close da barra anterior)
    closes = ultimas_4["Close"].values  # [bar1, bar2, bar3, bar4]
    for i in range(1, len(closes)):
        if closes[i] <= closes[i - 1]:
            return False

    return True

# =======================
# EXECUÇÃO DIRETA
# =======================

def main():
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
            else:
                print(f"  — {sym}")
        except Exception as e:
            print(f"  ⚠️  {sym}: {e}")

    tag_txt = (
        f"Preço≥${PRECO_MIN_USD:g} | "
        f"EMA21 + EMA120 + SMA200 (D1 e W1) | "
        f"Padrão: 🔴🟢🟢🟢 + closes crescentes"
        + (f" | Sem earnings ≤{EARNINGS_LOOKAHEAD_DIAS}d" if EXCLUIR_EARNINGS else "")
    )

    titulo = (
        f"*🚀 Radar D1/W1 — 3 Médias + Bear/Bull/Bull/Bull (closes crescentes)*\n"
        f"_{hoje}_\n"
        f"_{tag_txt}_\n\n"
    )

    msg = titulo + (f"*Ativos com sinal:* {', '.join(hits)}" if hits else "_Nenhum ativo com sinal hoje._")
    send_telegram(msg)
    print(f"\n[{hoje}] Finalizado. {len(hits)} sinal(is) enviado(s).")

if __name__ == "__main__":
    main()
