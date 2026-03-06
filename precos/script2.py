import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import os
import sys
from datetime import datetime
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


def get_fii_data() -> pd.DataFrame:
    """
    Busca todos os FIIs com preço atual via fiis.com.br/lista-de-fundos-imobiliarios.
    Usa requests + BeautifulSoup — sem Selenium, sem API bloqueada.
    """
    url = "https://fiis.com.br/lista-de-fundos-imobiliarios/"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9",
        "Referer": "https://fiis.com.br/",
    }

    log(f"Acessando {url}...")
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    log(f"Status HTTP: {resp.status_code} — {len(resp.text)} chars recebidos.")

    soup = BeautifulSoup(resp.text, "lxml")
    records = []

    # Todos os links de FIIs têm href no padrão /ticker11/ ou /ticker11b/
    fii_pattern = re.compile(r'^/[a-z0-9]{4,12}/$')

    for a in soup.find_all("a", href=fii_pattern):
        try:
            # Ticker: primeiro span ou texto direto do link
            spans = a.find_all("span")
            if not spans:
                continue

            ticker = spans[0].get_text(strip=True).upper()
            if not re.match(r'^[A-Z]{4}[0-9]{2}[A-Z]?$', ticker):
                continue

            # Preço: procura nos spans um valor numérico válido (ex: "9,72" ou "158,20")
            preco = None
            for span in spans:
                txt = span.get_text(strip=True)
                txt_norm = txt.replace(".", "").replace(",", ".")
                try:
                    val = float(txt_norm)
                    if val > 0:
                        preco = round(val, 2)
                        break
                except ValueError:
                    continue

            if not preco:
                continue

            records.append({"papel": ticker, "cotacao": preco})

        except Exception:
            continue

    df = pd.DataFrame(records).drop_duplicates(subset="papel")
    log(f"{len(df)} FIIs extraídos de fiis.com.br.")
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
    df = get_fii_data()

    if df.empty:
        log("ERRO: Nenhum dado obtido. Abortando.")
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

    log(f"✓ Concluído! {total_inserted} FIIs inseridos na tabela precoreal.")


if __name__ == "__main__":
    main()
