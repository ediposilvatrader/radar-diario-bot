"""
Radar 3WS — B3 (ações do Ibovespa + ETFs), timeframe D1.

Mesma lógica do radar.py (ações americanas): preço acima de EMA21/EMA120/SMA200
no D1 e no semanal + últimas 4 barras D1 fechadas = 1 bear seguida de 3 bull
com closes estritamente crescentes. O que muda para a B3:
- tickers com sufixo .SA no Yahoo Finance;
- filtro de LIQUIDEZ (volume financeiro médio em R$) no lugar do preço mínimo
  em US$;
- BOVA11 como referência de dado estável e de calendário de pregões (feriados
  da B3 são diferentes dos americanos).

FASE DE TESTE (desde 24/11/2026): envia SÓ para o privado do Édipo
(TELEGRAM_CHAT_ID_B3_TESTE). Não manda para a Comunidade nem para o Discord.
"""
import os
import time
import datetime
import zoneinfo
import yfinance as yf
import requests
import pandas as pd

TELEGRAM_TOKEN      = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID_B3 = os.environ["TELEGRAM_CHAT_ID_B3_TESTE"]

# =========================
# CONFIGURAÇÕES
# =========================
EMA_FAST  = 21
EMA_MID   = 120
SMA_LONG  = 200

# Liquidez mínima: média de (close x volume) dos últimos 21 pregões.
VOLUME_FIN_MIN_BRL = 20_000_000
JANELA_LIQUIDEZ    = 21

DEBUG = True

PADRAO_BARRAS = [False, True, True, True]  # bear, bull, bull, bull

TICKER_REFERENCIA_ESTABILIDADE = "BOVA11.SA"
TENTATIVAS_ESTABILIDADE        = 3
ESPERA_ESTABILIDADE_SEG        = 60
JANELA_VERIFICACAO_GAP_DIAS    = 10

# Carteira do Ibovespa (a B3 revisa em jan, mai e set) — conferir e atualizar
# esta lista nessas datas. Códigos que não existirem no Yahoo só aparecem como
# "histórico vazio" no log, não quebram o radar.
ACOES_IBOVESPA = [
    "ABEV3", "ALOS3", "ASAI3", "AURE3", "AXIA3", "AZZA3", "B3SA3",
    "BBAS3", "BBDC3", "BBDC4", "BBSE3", "BEEF3", "BPAC11", "BRAP4", "BRAV3",
    "BRKM5", "CMIG4", "CMIN3", "COGN3", "CPFE3", "CPLE3",
    "CSAN3", "CSMG3", "CSNA3", "CURY3", "CVCB3", "CXSE3", "CYRE3", "DIRR3",
    "EGIE3", "EMBJ3", "ENEV3", "ENGI11", "EQTL3",
    "FLRY3", "GGBR4", "GOAU4", "HAPV3", "HYPE3", "IGTI11", "IRBR3", "ISAE4",
    "ITSA4", "ITUB4", "KLBN11", "LREN3", "MBRF3", "MGLU3", "MOTV3", "MRVE3",
    "MULT3", "NATU3", "PCAR3", "PETR3", "PETR4", "POMO4", "PRIO3", "PSSA3",
    "RADL3", "RAIL3", "RAIZ4", "RDOR3", "RECV3", "RENT3", "SANB11", "SBSP3",
    "SLCE3", "SMFT3", "SMTO3", "SUZB3", "TAEE11", "TIMS3", "TOTS3",
    "UGPA3", "USIM5", "VALE3", "VAMO3", "VBBR3", "VIVA3", "VIVT3", "WEGE3",
    "YDUQ3",
]

# ETFs relevantes da B3: índice cheio, small caps, S&P 500 em reais,
# dividendos e ouro.
ETFS = ["BOVA11", "SMAL11", "IVVB11", "DIVO11", "GOLD11"]

TICKERS = [f"{t}.SA" for t in ACOES_IBOVESPA + ETFS]

# =======================
# HELPERS
# =======================

def send_telegram(msg: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID_B3, "text": msg, "parse_mode": "Markdown"}
    try:
        r = requests.post(url, json=payload, timeout=20)
        if not r.ok:
            print(f"Erro Telegram {r.status_code}: {r.text}")
    except Exception as e:
        print(f"Erro Telegram: {e}")

def safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

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
    diário do ticker de referência (BOVA11 — líquido, praticamente nunca tem
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

def check_symbol(sym: str, calendario_ref: set, gaps_registrados: dict) -> bool:
    ticker = yf.Ticker(sym)

    def dbg(msg):
        if DEBUG:
            print(f"    [{sym}] {msg}")

    # 1) Histórico
    df_d = ticker.history(period="600d", interval="1d",  auto_adjust=True)
    df_w = ticker.history(period="7y",   interval="1wk", auto_adjust=True)

    if df_d is None or df_w is None or df_d.empty or df_w.empty:
        dbg("REPROVADO — histórico vazio (ticker pode ter mudado de código)")
        return False

    # 0) Liquidez: volume financeiro médio (close x volume) dos últimos
    #    JANELA_LIQUIDEZ pregões. Substitui o preço mínimo em US$ do radar
    #    americano — na B3 o que separa papel operável de papel travado é a
    #    liquidez, não o preço.
    recentes = df_d.iloc[-JANELA_LIQUIDEZ:]
    vol_fin = float((recentes["Close"] * recentes["Volume"]).mean())
    if vol_fin < VOLUME_FIN_MIN_BRL:
        dbg(f"REPROVADO — liquidez (R$ {vol_fin/1e6:.1f} mi/dia) abaixo de R$ {VOLUME_FIN_MIN_BRL/1e6:.0f} mi")
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

    print(f"[{hoje}] Iniciando radar B3...")

    dado_estavel   = aguardar_dado_estavel()
    calendario_ref = obter_calendario_referencia()

    hits = []
    gaps_registrados = {}
    for sym in TICKERS:
        try:
            if check_symbol(sym, calendario_ref, gaps_registrados):
                hits.append(sym.replace(".SA", ""))
                print(f"  ✅ {sym}")
            else:
                print(f"  — {sym}")
        except Exception as e:
            print(f"  ⚠️  {sym}: {e}")

    if not dado_estavel:
        print("[aviso] dado do Yahoo Finance não estabilizou antes do scan — resultado pode mudar numa nova execução")
    if gaps_registrados:
        todas_as_datas = sorted({d for dias in gaps_registrados.values() for d in dias})
        datas_str = ", ".join(d.strftime("%d/%m") for d in todas_as_datas)
        print(f"[aviso] buraco no histórico diário reconstruído via intraday em {len(gaps_registrados)} ticker(s) — data(s): {datas_str}")

    # Data do último pregão analisado: em feriado da B3 (ou rodando antes do
    # fechamento) o radar avalia o pregão anterior — a mensagem deixa claro.
    pregao = max(calendario_ref).strftime("%d/%m") if calendario_ref else "?"

    corpo = f"*Padrões identificados:* {', '.join(hits)}" if hits else "Nenhum padrão identificado."
    msg = f"*Radar 3WS B3 Diário (teste) — pregão de {pregao}*\n\n{corpo}"
    send_telegram(msg)
    print(f"\n[{hoje}] Finalizado. {len(hits)} padrão(ões) identificado(s).")

if __name__ == "__main__":
    main()
