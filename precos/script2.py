from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
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


def get_fii_data_statusinvest() -> pd.DataFrame:
    """
    Busca todos os FIIs com preço atual diretamente do Status Invest via Selenium.
    O Status Invest exibe cotação D+0 durante o pregão.
    """
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
        driver.set_page_load_timeout(60)

        url = "https://statusinvest.com.br/fundos-imobiliarios"
        log(f"Acessando {url}...")
        driver.get(url)

        wait = WebDriverWait(driver, 60)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.default-fiis-table tbody tr")))
        log("Tabela carregada, extraindo dados...")

        rows = driver.find_elements(By.CSS_SELECTOR, "table.default-fiis-table tbody tr")

        records = []
        for row in rows:
            try:
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) < 2:
                    continue

                ticker = (cols[0].get_attribute("title") or cols[0].text).strip().upper()
                preco_raw = (cols[1].get_attribute("title") or cols[1].text).strip()

                preco_raw = (
                    preco_raw
                    .replace("R$", "")
                    .replace("\xa0", "")
                    .replace(".", "")
                    .replace(",", ".")
                    .strip()
                )

                if not ticker or not preco_raw:
                    continue

                preco = float(preco_raw)
                records.append({"papel": ticker, "cotacao": round(preco, 2)})

            except Exception:
                continue

        df = pd.DataFrame(records)
        log(f"{len(df)} FIIs extraídos do Status Invest.")
        return df

    finally:
        if driver:
            driver.quit()
            log("WebDriver fechado.")


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
    df = get_fii_data_statusinvest()

    if df.empty:
        log("ERRO: Nenhum dado obtido do Status Invest. Abortando.")
        sys.exit(1)

    df["data_atualizacao"] = datetime.now().isoformat()

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

    log(f"✓ Concluído! {total_inserted} FIIs inseridos na tabela precoreal (preços D+0).")


if __name__ == "__main__":
    main()
