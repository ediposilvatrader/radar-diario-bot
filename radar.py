import os
import time
import datetime
import zoneinfo
import yfinance as yf
import requests
import pandas as pd

# — Secrets do GitHub Actions
TELEGRAM_TOKEN       = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID     = os.environ["TELEGRAM_CHAT_ID"]
DISCORD_WEBHOOK_URL  = os.environ.get("DISCORD_WEBHOOK_URL")  # opcional — se ausente, só envia Telegram

# Grupo dos assinantes pagos (produto Radar 3WS) — grupo com Tópicos (Forum
# mode) ligado, um tópico por timeframe (D1/H1/S1) + Geral + Suporte. Opcional;
# se ausente, o radar continua funcionando só com o destino interno de sempre.
TELEGRAM_CHANNEL_ID_CLIENTES     = os.environ.get("TELEGRAM_CHANNEL_ID_CLIENTES")
TELEGRAM_THREAD_ID_CLIENTES_D1   = os.environ.get("TELEGRAM_THREAD_ID_CLIENTES_D1")

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

# Padrão das últimas 4 barras FECHADAS no D1
# False = bear (close < open) | True = bull (close > open)
PADRAO_BARRAS = [False, True, True, True]  # bear, bull, bull, bull

# Horário de fechamento do mercado americano em UTC
# NYSE/NASDAQ fecha às 21:00 UTC (16:00 ET / 20:00 UTC com horário de verão EUA)
# Usamos 21:00 UTC como referência segura — o radar roda às 21:30 UTC (18:30 BRT)
# então o mercado JÁ está fechado e a última barra D1 está 100% fechada
MERCADO_FECHA_UTC = datetime.time(21, 0)

# Yahoo Finance às vezes demora pra consolidar o candle diário depois do
# fechamento do mercado — duas execuções minutos apart podem ler closes
# diferentes pro mesmo dia. Antes de varrer os tickers, checamos duas
# leituras seguidas de um ticker de referência líquido; só seguimos
# quando elas baterem (dado "estabilizado").
TICKER_REFERENCIA_ESTABILIDADE = "AAPL"
TENTATIVAS_ESTABILIDADE        = 3
ESPERA_ESTABILIDADE_SEG        = 60

# O Yahoo Finance às vezes devolve o histórico DIÁRIO de um ticker específico
# com um dia útil inteiro faltando, mesmo esse dia existindo normalmente pra
# outros tickers e no próprio intraday do ticker afetado (visto em 28/08/2026
# com DE e XOM: o `.history(interval="1d")` pulava o dia, mas AAPL/SPY tinham
# e o intraday de 60m de DE/XOM daquele dia existia). Isso desloca a janela
# das últimas 4 barras do padrão e pode mascarar/inventar sinais. Verificamos
# só os últimos N dias úteis (custo baixo) contra um calendário de referência
# e, se achar buraco, reconstruímos a barra faltante via intraday.
JANELA_VERIFICACAO_GAP_DIAS = 10

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

def send_telegram(msg: str, chat_id: str = None, thread_id: str = None):
    """chat_id/thread_id opcionais — por padrão manda pro TELEGRAM_CHAT_ID de
    sempre (sem tópico); passar chat_id (e opcionalmente thread_id) explícitos
    permite mandar a mesma mensagem pra outros destinos (ex.: tópico D1 do
    grupo dos assinantes pagos), sem duplicar a lógica."""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id or TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"}
    if thread_id:
        payload["message_thread_id"] = thread_id
    try:
        requests.post(url, json=payload, timeout=20)
    except Exception as e:
        print(f"Erro Telegram: {e}")

def send_discord(msg: str):
    if not DISCORD_WEBHOOK_URL:
        return
    # Discord usa Markdown próprio: converte *negrito* (estilo Telegram) para **negrito**
    payload = {"content": msg.replace("*", "**")}
    try:
        requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=20)
    except Exception as e:
        print(f"Erro Discord: {e}")

def safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

def mercado_fechado() -> bool:
    """
    Retorna True se o mercado americano já fechou hoje (após 21:00 UTC).
    O radar roda às 21:30 UTC (18:30 BRT), então sempre será True em produção.
    Mantido aqui para deixar a lógica explícita caso alguém rode manualmente.
    """
    agora_utc = datetime.datetime.now(datetime.timezone.utc).time()
    return agora_utc >= MERCADO_FECHA_UTC

def get_daily_bar_snapshot(sym: str):
    """Retorna (data_da_ultima_barra, close_arredondado) pra comparar entre leituras."""
    try:
        df = yf.Ticker(sym).history(period="5d", interval="1d", auto_adjust=True)
        if df is None or df.empty:
            return None
        return (df.index[-1].date(), round(float(df["Close"].iloc[-1]), 4))
    except Exception as e:
        print(f"    [estabilidade] erro lendo {sym}: {e}")
        return None

def aguardar_dado_estavel() -> bool:
    """
    Compara leituras sucessivas do candle diário de um ticker de referência
    líquido. Só considera o dado do dia "estabilizado" quando duas leituras
    seguidas baterem exatamente. Retorna False (sem travar a execução) se
    não estabilizar dentro das tentativas configuradas — o scan roda mesmo
    assim, mas a mensagem final avisa que o dado pode não estar 100% final.
    """
    anterior = get_daily_bar_snapshot(TICKER_REFERENCIA_ESTABILIDADE)
    print(f"[estabilidade] leitura inicial ({TICKER_REFERENCIA_ESTABILIDADE}): {anterior}")

    for tentativa in range(1, TENTATIVAS_ESTABILIDADE + 1):
        time.sleep(ESPERA_ESTABILIDADE_SEG)
        atual = get_daily_bar_snapshot(TICKER_REFERENCIA_ESTABILIDADE)
        if anterior is not None and atual == anterior:
            print(f"[estabilidade] OK após {tentativa} checagem(ns) extra — dado consolidado: {atual}")
            return True
        print(f"[estabilidade] tentativa {tentativa}: {anterior} -> {atual} (mudou, aguardando mais)")
        anterior = atual

    print("[estabilidade] dado NÃO estabilizou dentro do tempo — seguindo mesmo assim, com aviso na mensagem")
    return False

def obter_calendario_referencia() -> set:
    """
    Dias em que o mercado esteve confirmadamente aberto, segundo o histórico
    diário do ticker de referência (AAPL — líquido, praticamente nunca tem
    buraco de dado). Usado pra detectar buracos no histórico de outros
    tickers, sem confundir buraco de dado com feriado/fim de semana.
    """
    try:
        df = yf.Ticker(TICKER_REFERENCIA_ESTABILIDADE).history(period="1mo", interval="1d", auto_adjust=True)
        if df is None or df.empty:
            return set()
        return set(idx.date() for idx in df.index)
    except Exception as e:
        print(f"[gap] erro obtendo calendário de referência: {e}")
        return set()

def reconstruir_barra_intraday(sym: str, dia: datetime.date):
    """Reconstrói O/H/L/C/Volume do dia a partir do intraday (60m) do próprio dia."""
    try:
        df_intra = yf.Ticker(sym).history(
            start=dia, end=dia + datetime.timedelta(days=1), interval="60m", auto_adjust=True
        )
        if df_intra is None or df_intra.empty:
            return None
        return {
            "Open":  float(df_intra["Open"].iloc[0]),
            "High":  float(df_intra["High"].max()),
            "Low":   float(df_intra["Low"].min()),
            "Close": float(df_intra["Close"].iloc[-1]),
            "Volume": float(df_intra["Volume"].sum()),
        }
    except Exception as e:
        print(f"    [gap] erro reconstruindo {sym} {dia}: {e}")
        return None

def corrigir_gaps_recentes(sym: str, df_d: pd.DataFrame, calendario_ref: set, dbg):
    """
    Verifica, só na janela recente (JANELA_VERIFICACAO_GAP_DIAS), se algum dia
    útil do calendário de referência está faltando no histórico diário de
    `sym`. Se achar, tenta reconstruir a barra via intraday e insere no
    DataFrame antes do cálculo de médias/padrão. Retorna (df_d, dias_corrigidos).
    """
    if not calendario_ref or df_d.empty:
        return df_d, []

    datas_presentes  = set(idx.date() for idx in df_d.index)
    ultima_data       = df_d.index[-1].date()
    limite_inferior   = ultima_data - datetime.timedelta(days=JANELA_VERIFICACAO_GAP_DIAS * 2)
    dias_esperados    = sorted(d for d in calendario_ref if limite_inferior <= d <= ultima_data)
    dias_faltando     = [d for d in dias_esperados if d not in datas_presentes]

    if not dias_faltando:
        return df_d, []

    dias_corrigidos = []
    for dia in dias_faltando:
        dbg(f"[gap] dia útil {dia} ausente no histórico diário — tentando reconstruir via intraday")
        barra = reconstruir_barra_intraday(sym, dia)
        if barra is None:
            dbg(f"[gap] não foi possível reconstruir {dia} (sem dado intraday disponível)")
            continue
        novo_idx = df_d.index[0].replace(year=dia.year, month=dia.month, day=dia.day)
        df_d.loc[novo_idx] = barra
        dias_corrigidos.append(dia)
        dbg(f"[gap] {dia} reconstruído: O={barra['Open']:.2f} C={barra['Close']:.2f}")

    if dias_corrigidos:
        df_d = df_d.sort_index()

    return df_d, dias_corrigidos

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

def check_symbol(sym: str, calendario_ref: set, gaps_registrados: dict) -> bool:
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
    df_d = ticker.history(period="600d", interval="1d",  auto_adjust=True)
    df_w = ticker.history(period="7y",   interval="1wk", auto_adjust=True)

    if df_d is None or df_w is None or df_d.empty or df_w.empty:
        dbg("REPROVADO — histórico vazio")
        return False
    if len(df_d) < 205 or len(df_w) < 205:
        dbg(f"REPROVADO — histórico insuficiente (D1={len(df_d)}, W1={len(df_w)})")
        return False

    # Corrige buracos de dia útil no histórico diário recente (ver comentário
    # de JANELA_VERIFICACAO_GAP_DIAS) antes de calcular médias/padrão — senão
    # a janela das últimas 4 barras fica deslocada e o padrão pode ser
    # avaliado errado (falso negativo ou falso positivo).
    df_d, dias_corrigidos = corrigir_gaps_recentes(sym, df_d, calendario_ref, dbg)
    if dias_corrigidos:
        gaps_registrados[sym] = dias_corrigidos

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

    if DEBUG:
        dbg(f"última barra D1: {df_d.index[-1].date()}  Close={ld['Close']:.2f}")
        dbg(f"última barra W1: {df_w.index[-1].date()}  Close={lw['Close']:.2f}")

    # Preço acima das 3 médias no D1
    cond_d_21  = ld["Close"] > ld["ema21"]
    cond_d_120 = ld["Close"] > ld["ema120"]
    cond_d_200 = ld["Close"] > ld["sma200"]
    cond_d = cond_d_21 and cond_d_120 and cond_d_200

    # Preço acima das 3 médias no W1
    cond_w_21  = lw["Close"] > lw["ema21"]
    cond_w_120 = lw["Close"] > lw["ema120"]
    cond_w_200 = lw["Close"] > lw["sma200"]
    cond_w = cond_w_21 and cond_w_120 and cond_w_200

    if DEBUG:
        dbg(
            f"D1 médias -> ema21:{'OK' if cond_d_21 else 'FALHA'} "
            f"ema120:{'OK' if cond_d_120 else 'FALHA'} "
            f"sma200:{'OK' if cond_d_200 else 'FALHA'} "
            f"(close={ld['Close']:.2f} ema21={ld['ema21']:.2f} "
            f"ema120={ld['ema120']:.2f} sma200={ld['sma200']:.2f})"
        )
        dbg(
            f"W1 médias -> ema21:{'OK' if cond_w_21 else 'FALHA'} "
            f"ema120:{'OK' if cond_w_120 else 'FALHA'} "
            f"sma200:{'OK' if cond_w_200 else 'FALHA'} "
            f"(close={lw['Close']:.2f} ema21={lw['ema21']:.2f} "
            f"ema120={lw['ema120']:.2f} sma200={lw['sma200']:.2f})"
        )

    if not (cond_d and cond_w):
        dbg("REPROVADO — não está acima das 3 médias em D1 e/ou W1")
        return False

    # --- Padrão das últimas 4 barras FECHADAS no D1 ---
    if len(df_d) < 4:
        dbg("REPROVADO — menos de 4 barras D1 disponíveis")
        return False

    ultimas_4 = df_d.iloc[-4:]  # as 4 barras mais recentes, todas fechadas

    if DEBUG:
        for i, (idx, row) in enumerate(ultimas_4.iterrows()):
            real_bull = row["Close"] > row["Open"]
            esperado  = "bull" if PADRAO_BARRAS[i] else "bear"
            real      = "bull" if real_bull else "bear"
            ok_dir    = "OK" if real_bull == PADRAO_BARRAS[i] else "FALHA"
            dbg(
                f"barra[{i}] {idx.date()} O={row['Open']:.2f} C={row['Close']:.2f} "
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

    print(f"[{hoje}] Iniciando radar...")

    dado_estavel   = aguardar_dado_estavel()
    calendario_ref = obter_calendario_referencia()

    hits = []
    gaps_registrados = {}
    for sym in TICKERS:
        try:
            if check_symbol(sym, calendario_ref, gaps_registrados):
                hits.append(sym)
                print(f"  ✅ {sym}")
            else:
                print(f"  — {sym}")
        except Exception as e:
            print(f"  ⚠️  {sym}: {e}")

    aviso = (
        "_(aviso: dado do Yahoo Finance não estabilizou antes do scan — "
        "resultado pode mudar numa nova execução)_\n\n"
        if not dado_estavel else ""
    )

    if gaps_registrados:
        detalhes = "; ".join(
            f"{sym} ({', '.join(d.strftime('%d/%m') for d in dias)})"
            for sym, dias in gaps_registrados.items()
        )
        aviso += (
            f"_(aviso: buraco no histórico diário do Yahoo Finance reconstruído "
            f"via intraday em: {detalhes})_\n\n"
        )

    if hits:
        msg = (
            f"*Radar 3WS Diário — {hoje}*\n\n"
            f"{aviso}*Sinais:* {', '.join(hits)}"
        )
    else:
        msg = (
            f"*Radar 3WS Diário — {hoje}*\n\n"
            f"{aviso}Nenhum sinal hoje."
        )
    send_telegram(msg)
    send_discord(msg)
    if TELEGRAM_CHANNEL_ID_CLIENTES:
        send_telegram(msg, chat_id=TELEGRAM_CHANNEL_ID_CLIENTES, thread_id=TELEGRAM_THREAD_ID_CLIENTES_D1)
    print(f"\n[{hoje}] Finalizado. {len(hits)} sinal(is) enviado(s).")

if __name__ == "__main__":
    main()
