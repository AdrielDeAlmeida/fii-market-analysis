import yfinance as yf
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import sys
from datetime import datetime
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


def get_fii_tickers() -> list[str]:
    """
    Busca a lista de tickers de FIIs listados na B3 via Fundamentus.
    Apenas os tickers (papel) — sem cotação, sem depender do preço deles.
    """
    url = "https://www.fundamentus.com.br/fii_resultado.php"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
    }
    log(f"Buscando lista de FIIs em {url}...")
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table", {"id": "tabelaResultado"})
    if not table:
        raise RuntimeError("Tabela de FIIs não encontrada no Fundamentus.")

    tickers = []
    for row in table.find("tbody").find_all("tr"):
        cols = row.find_all("td")
        if cols:
            ticker = cols[0].get_text(strip=True)
            if ticker:
                tickers.append(ticker)

    log(f"{len(tickers)} tickers encontrados.")
    return tickers


def get_realtime_prices(tickers: list[str]) -> pd.DataFrame:
    """
    Busca o preço atual (D+0) de cada FII via Yahoo Finance.
    Tickers da B3 precisam do sufixo .SA  ex: MXRF11 → MXRF11.SA
    """
    yahoo_tickers = [f"{t}.SA" for t in tickers]

    log(f"Buscando preços em tempo real para {len(yahoo_tickers)} FIIs via Yahoo Finance...")

    # download em lote — muito mais rápido que um a um
    raw = yf.download(
        tickers=yahoo_tickers,
        period="1d",
        interval="1m",
        group_by="ticker",
        auto_adjust=True,
        progress=False,
        threads=True,
    )

    records = []
    for ticker_sa, ticker_original in zip(yahoo_tickers, tickers):
        try:
            if len(yahoo_tickers) == 1:
                last_price = float(raw["Close"].dropna().iloc[-1])
            else:
                last_price = float(raw[ticker_sa]["Close"].dropna().iloc[-1])

            records.append({
                "papel": ticker_original,
                "cotacao": round(last_price, 2),
                "data_atualizacao": datetime.now().isoformat(),
            })
        except Exception:
            log(f"  ⚠ Sem dado para {ticker_original} — ignorado.")

    df = pd.DataFrame(records)
    log(f"{len(df)} FIIs com preço obtido com sucesso.")
    return df


def main():
    # ── Supabase ────────────────────────────────────────────────────────────
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")

    if not supabase_url or not supabase_key:
        log("ERRO: Defina SUPABASE_URL e SUPABASE_KEY no ambiente (.env).")
        sys.exit(1)

    log("Conectando ao Supabase...")
    supabase: Client = create_client(supabase_url, supabase_key)
    log("Conexão estabelecida!")

    # ── Coleta ───────────────────────────────────────────────────────────────
    tickers = get_fii_tickers()
    df = get_realtime_prices(tickers)

    if df.empty:
        log("ERRO: Nenhum preço obtido. Abortando.")
        sys.exit(1)

    # ── Supabase: limpa e insere ─────────────────────────────────────────────
    log("Limpando dados antigos da tabela fii_precos...")
    supabase.table("fii_precos").delete().neq("papel", "").execute()

    records = df.to_dict("records")
    batch_size = 100
    total_inserted = 0

    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        log(f"Inserindo lote {i // batch_size + 1} ({len(batch)} registros)...")
        supabase.table("fii_precos").insert(batch).execute()
        total_inserted += len(batch)

    log(f"✓ Concluído! {total_inserted} registros inseridos no Supabase (preços D+0).")


if __name__ == "__main__":
    main()
