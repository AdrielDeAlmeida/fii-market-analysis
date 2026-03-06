import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import os
import sys
from datetime import datetime
from supabase import create_client, Client
from dotenv import load_dotenv

# Fix SSL: caminho com acentos quebra o curl_cffi/requests no Windows
_cert_path = r"C:\certs\cacert.pem"
if os.path.exists(_cert_path):
    os.environ["SSL_CERT_FILE"]       = _cert_path
    os.environ["REQUESTS_CA_BUNDLE"]  = _cert_path
    os.environ["CURL_CA_BUNDLE"]      = _cert_path

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

    # Encontra todos os blocos de FIIs, mas ignora os que são apenas 'destaque' 
    # pois os destaques mostram Dividend Yield em vez de Preço.
    boxes = soup.find_all("div", class_="tickerBox")
    log(f"Encontrados {len(boxes)} blocos tickerBox.")

    for box in boxes:
        try:
            # Pula os blocos de destaque (esses costumam ter métricas diferentes)
            if "tickerBox--destaque" in box.get("class", []):
                continue

            # Ticker: div com classe tickerBox__title
            ticker_div = box.find("div", class_="tickerBox__title")
            if not ticker_div:
                continue
            ticker = ticker_div.get_text(strip=True).upper()
            
            # Validar ticker (ex: MXRF11)
            if not re.match(r'^[A-Z]{4}[0-9]{2}[A-Z]?$', ticker):
                continue

            # Preço: os valores ficam dentro de div.tickerBox__info -> div.tickerBox__info__box
            # Geralmente o primeiro info__box é a cotação e o segundo é o PL ou algo assim
            info_boxes = box.find_all("div", class_="tickerBox__info__box")
            if not info_boxes:
                continue
            
            preco = None
            # Tenta pegar o primeiro valor numérico válido dos info_boxes
            for info in info_boxes:
                txt = info.get_text(strip=True)
                txt_norm = txt.replace(".", "").replace(",", ".")
                try:
                    val = float(txt_norm)
                    if val > 0:
                        preco = round(val, 2)
                        break
                except ValueError:
                    continue

            if preco:
                records.append({"papel": ticker, "cotacao": preco})

        except Exception as e:
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

    try:
        log("Conectando ao Supabase...")
        supabase: Client = create_client(supabase_url, supabase_key)
        log("Conexão estabelecida!")
    except Exception as e:
        log(f"ERRO ao conectar ao Supabase: {e}")
        sys.exit(1)

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