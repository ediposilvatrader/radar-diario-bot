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

PRECO_MIN_USD = 50.0

# Modo debug: imprime diagnóstico detalhado de cada ticker no log do GitHub Actions
# (NÃO afeta a mensagem do Telegram, só aparece nos logs da execução)
DEBUG = True

# Padrão das últimas 4 barras FECHADAS no H1
# False = bear (close < open) | True = bull (close > open)
PADRAO_BARRAS = [False, True, True, True]  # bear, bull, bull, bull

# Horário de fechamento do mercado americano em UTC
# NYSE/NASDAQ fecha às 21:00 UTC (16:00 ET / 20:00 UTC com horário de verão EUA)
# Usamos 21:00 UTC como referência segura — o radar roda às 21:30 UTC (18:30 BRT)
# então o mercado JÁ está fechado e a última barra H1 está 100% fechada
MERCADO_FECHA_UTC = datetime.time(21, 0)

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

def mercado_fechado() -> bool:
    """
    Retorna True se o mercado americano já fechou hoje (após 21:00 UTC).
    Mantido aqui para diagnóstico/uso manual — a checagem real de barra
    fechada é feita por descartar_barra_aberta(), pois agora o radar roda
    de hora em hora durante o pregão.
    """
    agora_utc = datetime.datetime.now(datetime.timezone.utc).time()
    return agora_utc >= MERCADO_FECHA_UTC

def descartar_barra_aberta(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove a última barra do DataFrame se ela ainda não tiver fechado.
    Uma barra H1 com horário de início `t` só fecha em `t + 1h`.
    Compara isso com o horário atual em UTC; se a barra ainda não fechou,
    ela é descartada para não contaminar médias/padrão com dado parcial.
    """
    if df is None or df.empty:
        return df

    agora_utc = pd.Timestamp.now(tz="UTC")
    ultimo_idx = df.index[-1]

    # Garante que o timestamp do índice tenha timezone para comparar com agora_utc
    if ultimo_idx.tzinfo is None:
        ultimo_idx = ultimo_idx.tz_localize("UTC")

    fechamento_barra = ultimo_idx + pd.Timedelta(hours=1)

    if fechamento_barra > agora_utc:
        df = df.iloc[:-1]

    return df

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

    def dbg(msg):
        if DEBUG:
            print(f"    [{sym}] {msg}")

    # 0) Preço mínimo
    last_price = get_last_price_usd(ticker)
    if last_price is None or last_price < PRECO_MIN_USD:
        dbg(f"REPROVADO — preço ({last_price}) abaixo de {PRECO_MIN_USD}")
        return False

    # 1) Histórico
    # H1 (sinal) — Yahoo limita dados intraday de 1h a ~730 dias
    df_h = ticker.history(period="730d", interval="1h", auto_adjust=True)
    # D1 (viés)
    df_d = ticker.history(period="600d", interval="1d", auto_adjust=True)

    if df_h is None or df_d is None or df_h.empty or df_d.empty:
        dbg("REPROVADO — histórico vazio")
        return False

    # Descarta a barra H1 em formação (ainda não fechada) — essencial agora
    # que o radar roda de hora em hora, inclusive durante o pregão
    antes = len(df_h)
    df_h = descartar_barra_aberta(df_h)
    if DEBUG and len(df_h) < antes:
        dbg(f"barra H1 aberta descartada (última barra fechada: {df_h.index[-1]})")
    if len(df_h) < 205 or len(df_d) < 205:
        dbg(f"REPROVADO — histórico insuficiente (H1={len(df_h)}, D1={len(df_d)})")
        return False

    # Médias H1
    df_h["ema21"]  = df_h["Close"].ewm(span=EMA_FAST, adjust=False).mean()
    df_h["ema120"] = df_h["Close"].ewm(span=EMA_MID,  adjust=False).mean()
    df_h["sma200"] = df_h["Close"].rolling(window=SMA_LONG).mean()

    # Médias D1
    df_d["ema21"]  = df_d["Close"].ewm(span=EMA_FAST, adjust=False).mean()
    df_d["ema120"] = df_d["Close"].ewm(span=EMA_MID,  adjust=False).mean()
    df_d["sma200"] = df_d["Close"].rolling(window=SMA_LONG).mean()

    lh = df_h.iloc[-1]
    ld = df_d.iloc[-1]

    if DEBUG:
        dbg(f"última barra H1: {df_h.index[-1]}  Close={lh['Close']:.2f}")
        dbg(f"última barra D1: {df_d.index[-1].date()}  Close={ld['Close']:.2f}")

    # Preço acima das 3 médias no H1
    cond_h_21  = lh["Close"] > lh["ema21"]
    cond_h_120 = lh["Close"] > lh["ema120"]
    cond_h_200 = lh["Close"] > lh["sma200"]
    cond_h = cond_h_21 and cond_h_120 and cond_h_200

    # Preço acima das 3 médias no D1
    cond_d_21  = ld["Close"] > ld["ema21"]
    cond_d_120 = ld["Close"] > ld["ema120"]
    cond_d_200 = ld["Close"] > ld["sma200"]
    cond_d = cond_d_21 and cond_d_120 and cond_d_200

    if DEBUG:
        dbg(
            f"H1 médias -> ema21:{'OK' if cond_h_21 else 'FALHA'} "
            f"ema120:{'OK' if cond_h_120 else 'FALHA'} "
            f"sma200:{'OK' if cond_h_200 else 'FALHA'} "
            f"(close={lh['Close']:.2f} ema21={lh['ema21']:.2f} "
            f"ema120={lh['ema120']:.2f} sma200={lh['sma200']:.2f})"
        )
        dbg(
            f"D1 médias -> ema21:{'OK' if cond_d_21 else 'FALHA'} "
            f"ema120:{'OK' if cond_d_120 else 'FALHA'} "
            f"sma200:{'OK' if cond_d_200 else 'FALHA'} "
            f"(close={ld['Close']:.2f} ema21={ld['ema21']:.2f} "
            f"ema120={ld['ema120']:.2f} sma200={ld['sma200']:.2f})"
        )

    if not (cond_h and cond_d):
        dbg("REPROVADO — não está acima das 3 médias em H1 e/ou D1")
        return False

    # --- Padrão das últimas 4 barras FECHADAS no H1 ---
    if len(df_h) < 4:
        dbg("REPROVADO — menos de 4 barras H1 disponíveis")
        return False

    ultimas_4 = df_h.iloc[-4:]  # as 4 barras mais recentes, todas fechadas

    if DEBUG:
        for i, (idx, row) in enumerate(ultimas_4.iterrows()):
            real_bull = row["Close"] > row["Open"]
            esperado  = "bull" if PADRAO_BARRAS[i] else "bear"
            real      = "bull" if real_bull else "bear"
            ok_dir    = "OK" if real_bull == PADRAO_BARRAS[i] else "FALHA"
            dbg(
                f"barra[{i}] {idx} O={row['Open']:.2f} C={row['Close']:.2f} "
                f"-> {real} (esperado {esperado}) [{ok_dir}]"
            )

    # 1) Verificar direção de cada barra (bear/bull)
    for i, (_, row) in enumerate(ultimas_4.iterrows()):
        esperado_bull = PADRAO_BARRAS[i]
        real_bull     = row["Close"] > row["Open"]
        if real_bull != esperado_bull:
            dbg("REPROVADO — padrão de direção das barras não corresponde")
            return False

    # 2) Verificar fechamentos crescentes APENAS entre as 3 barras bull
    #    (close[1] < close[2] < close[3]) — ignora o close da barra bear (índice 0),
    #    pois é comum a 1ª barra bull fechar abaixo do close da barra bear anterior
    #    (gap down + recuperação parcial) e ainda assim configurar o padrão.
    closes = ultimas_4["Close"].values
    closes_bull = closes[1:]  # close[1], close[2], close[3]

    if DEBUG:
        seq = " -> ".join(f"{c:.2f}" for c in closes)
        seq_bull = " -> ".join(f"{c:.2f}" for c in closes_bull)
        crescente = all(closes_bull[i] > closes_bull[i-1] for i in range(1, len(closes_bull)))
        dbg(f"closes (4 barras): {seq}")
        dbg(f"closes bull (1-3): {seq_bull}  (crescente: {'OK' if crescente else 'FALHA'})")

    for i in range(1, len(closes_bull)):
        if closes_bull[i] <= closes_bull[i - 1]:
            dbg("REPROVADO — closes das barras bull não são estritamente crescentes")
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

    print(f"[{hoje}] Iniciando radar H1...")

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

    if hits:
        msg = (
            f"*Radar 3WS H1 — {hoje}*\n\n"
            f"*Sinais:* {', '.join(hits)}"
        )
    else:
        msg = (
            f"*Radar 3WS H1 — {hoje}*\n\n"
            f"Nenhum sinal hoje."
        )
    send_telegram(msg)
    print(f"\n[{hoje}] Finalizado. {len(hits)} sinal(is) enviado(s).")

if __name__ == "__main__":
    main()
