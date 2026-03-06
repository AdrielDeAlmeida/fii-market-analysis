from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import yfinance as yf
import pandas as pd
from io import StringIO
import os
import sys
from datetime import datetime
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


def get_fii_tickers_selenium() -> list[str]:
    """Usa Selenium para buscar a lista de tickers do Fundamentus (bypassa 403)."""
    driver = None
    try:
        log("Configurando WebDriver...")
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(30)

        url = "https://www.fundamentus.com.br/fii_resultado.php"
        log(f"Acessando {url}...")
        driver.get(url)

        wait = WebDriverWait(driver, 30)
        table = wait.until(EC.presence_of_element_located((By.ID, "tabelaResultado")))
        html_content = table.get_attribute("outerHTML")
        log("Tabela carregada com sucesso!")

        df = pd.read_html(StringIO(html_content))[0]
        # Primeira coluna = papel/ticker
        tickers = df.iloc[:, 0].astype(str).str.strip().tolist()
        tickers = [t for t in tickers if t and t != "nan"]

        log(f"{len(tickers)} tickers encontrados no Fundamentus.")
        return tickers

    finally:
        if driver:
            driver.quit()
            log("WebDriver fechado.")


def get_realtime_prices(tickers: list[str]) -> pd.DataFrame:
    """Busca preço D+0 via yfinance para cada ticker da B3 (.SA)."""
    yahoo_tickers = [f"{t}.SA" for t in tickers]

    log(f"Buscando preços em tempo real para {len(yahoo_tickers)} FIIs via Yahoo Finance...")

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
    log(f"{len(df)} FIIs com preço D+0 obtido.")
    return df


def main():
    # ── Supabase ─────────────────────────────────────────────────────────────
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")

    if not supabase_url or not supabase_key:
        log("ERRO: Defina SUPABASE_URL e SUPABASE_KEY no ambiente (.env).")
        sys.exit(1)

    log("Conectando ao Supabase...")
    supabase: Client = create_client(supabase_url, supabase_key)
    log("Conexão estabelecida!")

    # ── Coleta ────────────────────────────────────────────────────────────────
    tickers = get_fii_tickers_selenium()
    df = get_realtime_prices(tickers)

    if df.empty:
        log("ERRO: Nenhum preço obtido. Abortando.")
        sys.exit(1)

    # ── Supabase: limpa e insere ──────────────────────────────────────────────
    log("Limpando dados antigos da tabela precoreal...")
    supabase.table("precoreal").delete().neq("papel", "").execute()

    records = df.to_dict("records")
    batch_size = 100
    total_inserted = 0

    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        log(f"Inserindo lote {i // batch_size + 1} ({len(batch)} registros)...")
        supabase.table("precoreal").insert(batch).execute()
        total_inserted += len(batch)

    log(f"✓ Concluído! {total_inserted} registros inseridos no Supabase (preços D+0).")


if __name__ == "__main__":
    main()
