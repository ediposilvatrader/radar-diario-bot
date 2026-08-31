import os
import datetime
import zoneinfo
import yfinance as yf
import requests
import pandas as pd

# — Secrets do GitHub Actions
TELEGRAM_TOKEN         = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID_S1    = os.environ["TELEGRAM_CHAT_ID_S1"]
TELEGRAM_THREAD_ID_S1  = os.environ.get("TELEGRAM_THREAD_ID_S1")  # opcional — tópico do grupo

# =========================
# CONFIGURAÇÕES
# =========================
EMA_FAST  = 21
EMA_MID   = 120
SMA_LONG  = 200

PRECO_MIN_USD = 50.0

# Modo debug: imprime diagnóstico detalhado de cada ticker no log do GitHub Actions
# (NÃO afeta a mensagem do Telegram, só aparece nos logs da execução)
DEBUG = True

# Padrão das últimas 4 barras FECHADAS no W1 (mesmo padrão do H1/D1)
# False = bear (close < open) | True = bull (close > open)
PADRAO_BARRAS_COMPRA = [False, True, True, True]   # bear, bull, bull, bull
PADRAO_BARRAS_VENDA  = [True, False, False, False]  # espelho exato: bull, bear, bear, bear

TICKERS = [
    "AA","AAPL","ABBV","ABNB","ACN","ADBE","ADI","ADP","AEP","AIG","AKAM","AMAT","AMD",
    "AMGN","AMT","AMZN","ANET","APPN","APPS","ATR","AVGO","AVY","AWK","AXON",
    "AXP","AZO","BA","BAC","BALL","BAX","BB","BBY","BDX","BEN","BF-B","BIDU","BIIB","BILI",
    "BK","BKNG","BLK","BMY","BNS","BRK-B","BSX","BURL","BX","BYD","BYND","BZUN","C","CAT",
    "CB","CBOE","CCI","CHD","CHGG","CHWY","CLX","CM","CMCSA","CME","CMG","CNC","COP",
    "COST","CP","CPB","CPRI","CPRT","CRM","CRWD","CSCO","CSX","CTRA","CVNA","CVS","CVX",
    "D","DAL","DAN","DBX","DD","DE","DELL","DG","DHR","DIS","DK","DKNG","DLR","DLTR",
    "DOCU","DT","DUK","DXC","DXCM","EA","EBAY","ECL","ED","EEFT","EIX","EL","ENB","ENPH","EPR",
    "ETR","ETSY","EXPE","F","FANG","FCX","FDX","FHN","FITB","FIVE","FLR",
    "FOX","FSLY","FTI","FTNT","GDS","GE","GILD","GM","GOOG","GPN","GRMN","GS","GT",
    "HBAN","HD","HLT","HOG","HON","HP","HPQ","HRL","HUYA","IAC","IBKR","IBM","IDXX","ILMN",
    "INCY","INO","INTC","INTU","ISRG","J","JNJ","JPM","KEY","KLAC","KMB","KMX","KO",
    "LHX","LIN","LLY","LMT","LOW","LRCX","LULU","LUMN","LUV","LYFT","MA","MAA","MAC","MAR",
    "MASI","MAT","MCD","MDB","MDLZ","MDT","MDXG","MELI","META","MGM","MKC","MKTX","MLM","MMM",
    "MNST","MO","MPC","MRK","MRVL","MS","MSCI","MSFT","MTCH","MTZ","MU","NEE","NEM","NET",
    "NFLX","NICE","NKE","NOW","NTAP","NTRS","NVDA","NVO","NVR","NXPI","NXST","OC","OKE","OKTA",
    "OMC","ORCL","PAAS","PANW","PDD","PEP","PFE","PG","PGR","PH","PINS","PLD","PLNT","PLTR","PM",
    "PNC","PNR","PODD","POOL","PSO","PYPL","QCOM","RBLX","RH","RNG","ROKU","RTX",
    "SBAC","SBUX","SE","SEDG","SFIX","SHAK","SHOP","SIRI","SNAP","SNOW","XYZ","STT","SWK","SYK",
    "T","TAP","TDG","TDOC","TEAM","TFC","THO","TJX","TMO","TMUS","TRV","TSLA","TSN","TTD","TWLO","TXN",
    "UAL","UBER","UI","UNH","UNP","UPS","URBN","USB","V","VZ","W","WDAY","WDC","WEN","WFC","WHR","WM","WTW","WYNN",
    "XEL","XOM","YELP","ZG","ZTS",

    # ETFs setoriais SPDR — cobrem os 11 setores GICS do mercado americano
    "XLC","XLY","XLP","XLE","XLF","XLV","XLI","XLB","XLRE","XLK","XLU"
]

# =======================
# HELPERS
# =======================

def send_telegram(msg: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID_S1, "text": msg, "parse_mode": "Markdown"}
    if TELEGRAM_THREAD_ID_S1:
        payload["message_thread_id"] = TELEGRAM_THREAD_ID_S1
    try:
        requests.post(url, json=payload, timeout=20)
    except Exception as e:
        print(f"Erro Telegram: {e}")

def safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

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

def check_symbol(sym: str, direcao: str) -> bool:
    """
    direcao: "compra" (preço acima das médias, padrão bear->bull->bull->bull)
             ou "venda" (espelho exato: preço abaixo das médias, padrão bull->bear->bear->bear)
    """
    ticker = yf.Ticker(sym)

    def dbg(msg):
        if DEBUG:
            print(f"    [{sym}/{direcao}] {msg}")

    # 0) Preço mínimo
    last_price = get_last_price_usd(ticker)
    if last_price is None or last_price < PRECO_MIN_USD:
        dbg(f"REPROVADO — preço ({last_price}) abaixo de {PRECO_MIN_USD}")
        return False

    # 1) Histórico
    # W1 (sinal)
    df_w = ticker.history(period="5y",  interval="1wk", auto_adjust=True)
    # MN1 (viés)
    df_m = ticker.history(period="20y", interval="1mo", auto_adjust=True)

    if df_w is None or df_m is None or df_w.empty or df_m.empty:
        dbg("REPROVADO — histórico vazio")
        return False
    if len(df_w) < 205 or len(df_m) < 205:
        dbg(f"REPROVADO — histórico insuficiente (W1={len(df_w)}, MN1={len(df_m)})")
        return False

    # Médias W1
    df_w["ema21"]  = df_w["Close"].ewm(span=EMA_FAST, adjust=False).mean()
    df_w["ema120"] = df_w["Close"].ewm(span=EMA_MID,  adjust=False).mean()
    df_w["sma200"] = df_w["Close"].rolling(window=SMA_LONG).mean()

    # Médias MN1
    df_m["ema21"]  = df_m["Close"].ewm(span=EMA_FAST, adjust=False).mean()
    df_m["ema120"] = df_m["Close"].ewm(span=EMA_MID,  adjust=False).mean()
    df_m["sma200"] = df_m["Close"].rolling(window=SMA_LONG).mean()

    lw = df_w.iloc[-1]
    lm = df_m.iloc[-1]

    if DEBUG:
        dbg(f"última barra W1: {df_w.index[-1].date()}  Close={lw['Close']:.2f}")
        dbg(f"última barra MN1: {df_m.index[-1].date()}  Close={lm['Close']:.2f}")

    if direcao == "compra":
        cond_w = lw["Close"] > lw["ema21"] and lw["Close"] > lw["ema120"] and lw["Close"] > lw["sma200"]
        cond_m = lm["Close"] > lm["ema21"] and lm["Close"] > lm["ema120"] and lm["Close"] > lm["sma200"]
        padrao_barras = PADRAO_BARRAS_COMPRA
    else:
        cond_w = lw["Close"] < lw["ema21"] and lw["Close"] < lw["ema120"] and lw["Close"] < lw["sma200"]
        cond_m = lm["Close"] < lm["ema21"] and lm["Close"] < lm["ema120"] and lm["Close"] < lm["sma200"]
        padrao_barras = PADRAO_BARRAS_VENDA

    if DEBUG:
        dbg(f"W1 acima/abaixo das médias ({direcao}): {'OK' if cond_w else 'FALHA'}")
        dbg(f"MN1 acima/abaixo das médias ({direcao}): {'OK' if cond_m else 'FALHA'}")

    if not (cond_w and cond_m):
        dbg("REPROVADO — não está acima/abaixo das 3 médias em W1 e/ou MN1")
        return False

    # --- Padrão das últimas 4 barras FECHADAS no W1 ---
    if len(df_w) < 4:
        dbg("REPROVADO — menos de 4 barras W1 disponíveis")
        return False

    ultimas_4 = df_w.iloc[-4:]  # as 4 barras mais recentes, todas fechadas

    if DEBUG:
        for i, (idx, row) in enumerate(ultimas_4.iterrows()):
            real_bull = row["Close"] > row["Open"]
            esperado  = "bull" if padrao_barras[i] else "bear"
            real      = "bull" if real_bull else "bear"
            ok_dir    = "OK" if real_bull == padrao_barras[i] else "FALHA"
            dbg(
                f"barra[{i}] {idx.date()} O={row['Open']:.2f} C={row['Close']:.2f} "
                f"-> {real} (esperado {esperado}) [{ok_dir}]"
            )

    # 1) Verificar direção de cada barra (bear/bull)
    for i, (_, row) in enumerate(ultimas_4.iterrows()):
        esperado_bull = padrao_barras[i]
        real_bull     = row["Close"] > row["Open"]
        if real_bull != esperado_bull:
            dbg("REPROVADO — padrão de direção das barras não corresponde")
            return False

    closes = ultimas_4["Close"].values

    if direcao == "compra":
        # closes crescentes APENAS entre as 3 barras bull (índices 1-3),
        # ignora o close da barra bear (índice 0)
        closes_ref = closes[1:]
        if DEBUG:
            seq = " -> ".join(f"{c:.2f}" for c in closes)
            seq_ref = " -> ".join(f"{c:.2f}" for c in closes_ref)
            crescente = all(closes_ref[i] > closes_ref[i-1] for i in range(1, len(closes_ref)))
            dbg(f"closes (4 barras): {seq}")
            dbg(f"closes bull (1-3): {seq_ref}  (crescente: {'OK' if crescente else 'FALHA'})")
        for i in range(1, len(closes_ref)):
            if closes_ref[i] <= closes_ref[i - 1]:
                dbg("REPROVADO — closes das barras bull não são estritamente crescentes")
                return False
    else:
        # espelho exato: closes decrescentes APENAS entre as 3 barras bear (índices 1-3),
        # ignora o close da barra bull (índice 0)
        closes_ref = closes[1:]
        if DEBUG:
            seq = " -> ".join(f"{c:.2f}" for c in closes)
            seq_ref = " -> ".join(f"{c:.2f}" for c in closes_ref)
            decrescente = all(closes_ref[i] < closes_ref[i-1] for i in range(1, len(closes_ref)))
            dbg(f"closes (4 barras): {seq}")
            dbg(f"closes bear (1-3): {seq_ref}  (decrescente: {'OK' if decrescente else 'FALHA'})")
        for i in range(1, len(closes_ref)):
            if closes_ref[i] >= closes_ref[i - 1]:
                dbg("REPROVADO — closes das barras bear não são estritamente decrescentes")
                return False

    dbg("APROVADO — todas as condições atendidas")
    return True

# =======================
# EXECUÇÃO DIRETA
# =======================

def main():
    tz_brt = zoneinfo.ZoneInfo("America/Sao_Paulo")
    agora  = datetime.datetime.now(tz_brt)
    hoje   = agora.strftime("%d/%m/%Y %H:%M")

    print(f"[{hoje}] Iniciando radar S1...")

    buys, sells = [], []
    for sym in TICKERS:
        try:
            if check_symbol(sym, "compra"):
                buys.append(sym)
                print(f"  ✅ compra {sym}")
        except Exception as e:
            print(f"  ⚠️  {sym}: {e}")
        try:
            if check_symbol(sym, "venda"):
                sells.append(sym)
                print(f"  🔻 venda {sym}")
        except Exception as e:
            print(f"  ⚠️  {sym}: {e}")

    header = f"*Radar 3WS S1 — {hoje}*\n\n"
    body = ""
    if buys:
        body += "*Sinais de Compra:* " + ", ".join(buys) + "\n\n"
    else:
        body += "Nenhum sinal de compra.\n\n"
    if sells:
        body += "*Sinais de Venda:* " + ", ".join(sells)
    else:
        body += "Nenhum sinal de venda."

    send_telegram(header + body)
    print(f"\n[{hoje}] Finalizado. {len(buys)} compra(s), {len(sells)} venda(s).")

if __name__ == "__main__":
    main()
