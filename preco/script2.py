import os
import sys
import time
import random
import re
from datetime import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor, as_completed

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

def create_driver():
    """Cria uma instância configurada do Chrome Headless"""
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    return webdriver.Chrome(options=options)

def process_fii_batch(fii_batch, worker_id):
    """Worker que processa um lote de FIIs usando uma única instância do WebDriver"""
    driver = create_driver()
    records = []
    
    log(f"Worker {worker_id} iniciou processamento de {len(fii_batch)} FIIs.")
    
    for ticker, url in fii_batch:
        try:
            driver.get(url)
            # Espera até 15 segundos para a proteção do Cloudflare passar e o preço carregar
            wait = WebDriverWait(driver, 15)
            
            try:
                price_element = wait.until(
                    EC.presence_of_element_located((By.CLASS_NAME, "headerTicker__content__price"))
                )
                text = price_element.text
                
                preco_val = None
                var_val = 0.0
                
                # Extrair Preço (Regex procura por R$ seguido de números)
                match_preco = re.search(r"R\$\s?([\d.,]+)", text)
                if match_preco:
                    preco_val = float(match_preco.group(1).replace(".", "").replace(",", "."))
                    
                # Extrair Variação (Regex procura por números seguidos de %)
                match_var = re.search(r"([-+]?[\d.,]+)%", text)
                if match_var:
                    var_val = float(match_var.group(1).replace(".", "").replace(",", "."))
                    
                if preco_val is not None:
                    records.append({
                        "papel": ticker,
                        "cotacao": preco_val,
                        "variacao": var_val
                    })
            except Exception:
                # Se não encontrar o elemento (FII não existe ou timeout), segue em frente
                pass
                
            # Pequena pausa humana para evitar bloqueios agressivos
            time.sleep(random.uniform(1.0, 2.5))
            
        except Exception:
            pass
            
    driver.quit()
    return records

def get_fii_data() -> pd.DataFrame:
    """
    Busca a lista de FIIs via Regex e usa Selenium em paralelo para pegar a cotação real.
    """
    url_lista = "https://fiis.com.br/lista-de-fundos-imobiliarios/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    log("Coletando lista de FIIs...")
    resp = requests.get(url_lista, headers=headers, timeout=30)
    resp.raise_for_status()
    
    # Busca apenas o padrão do Ticker no texto puro, ignorando problemas de HTML
    tickers_encontrados = set(re.findall(r'\b[A-Z]{4}11\b', resp.text.upper()))
    
    fii_links = []
    for ticker in tickers_encontrados:
        fii_links.append((ticker, f"https://www.fundsexplorer.com.br/funds/{ticker.lower()}"))

    log(f"Iniciando coleta de preços para {len(fii_links)} FIIs usando Selenium...")
    
    # Divide a lista de FIIs em 3 lotes para rodar em 3 navegadores paralelos. 
    # Isso evita estourar a memória (RAM) limitada do GitHub Actions (7GB).
    num_workers = 3
    batch_size = (len(fii_links) // num_workers) + 1
    batches = [fii_links[i:i + batch_size] for i in range(0, len(fii_links), batch_size)]
    
    all_records = []
    
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        for i, batch in enumerate(batches):
            futures.append(executor.submit(process_fii_batch, batch, i+1))
            
        for future in as_completed(futures):
            try:
                batch_records = future.result()
                all_records.extend(batch_records)
            except Exception as e:
                log(f"Erro em lote paralelo: {e}")

    df = pd.DataFrame(all_records)
    if not df.empty:
        df = df.drop_duplicates(subset="papel")
    else:
        df = pd.DataFrame(columns=["papel", "cotacao", "variacao"])
        
    log(f"{len(df)} FIIs com preço real extraídos com sucesso.")
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
    log("Limpando todos os dados antigos da tabela 'precoreal' (apagando tudo para reescrever)...")
    try:
        res = supabase.table("precoreal").select("papel").execute()
        if res.data:
            papeis = [row["papel"] for row in res.data]
            for i in range(0, len(papeis), 100):
                lote = papeis[i:i+100]
                supabase.table("precoreal").delete().in_("papel", lote).execute()
        log("Tabela limpa com sucesso!")
    except Exception as e:
        log(f"Aviso ao limpar tabela: {e}")

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
