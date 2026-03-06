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


from concurrent.futures import ThreadPoolExecutor, as_completed

def get_individual_fii_price(session, ticker, url):
    """Acessa a página individual do FII e extrai o preço real atual."""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        resp = session.get(url, headers=headers, timeout=15)
        if resp.status_code != 200:
            return None
        
        soup = BeautifulSoup(resp.text, "lxml")
        
        # Procura o valor que fica no topo da página, geralmente dentro de um span ou div destaque
        # O seletor abaixo busca o valor principal da cotação
        valor_wrapper = soup.find("div", class_="headerTicker__content__price") 
        if not valor_wrapper:
            # Fallback para outros layouts comuns no site
            valor_wrapper = soup.find("span", string=re.compile(r"R\$"))
            if valor_wrapper:
                valor_wrapper = valor_wrapper.parent

        if valor_wrapper:
            text_nodes = [t.strip() for t in valor_wrapper.find_all(string=True)]
            for i, t in enumerate(text_nodes):
                if 'R$' in t:
                    # O preço real é logo no próximo texto após o R$
                    for price_txt in text_nodes[i+1:]:
                        if price_txt:
                            match = re.search(r'^([\d.,]+)', price_txt)
                            if match:
                                val_str = match.group(1).replace(".", "").replace(",", ".")
                                return float(val_str)
            
            # Fallback de segurança na busca antiga se os textos estiverem juntos
            txt = valor_wrapper.get_text(separator=' ', strip=True)
            match = re.search(r"R\$\s*([\d.]+,\d{1,2})", txt)
            if match:
                val_str = match.group(1).replace(".", "").replace(",", ".")
                return float(val_str)
    except Exception:
        pass
    return None


def get_fii_data() -> pd.DataFrame:
    """
    Busca a lista de FIIs e depois acessa cada um em paralelo para pegar o preço real.
    """
    url_lista = "https://fiis.com.br/lista-de-fundos-imobiliarios/"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    log("Coletando lista de FIIs...")
    resp = requests.get(url_lista, headers=headers, timeout=30)
    resp.raise_for_status()
    
    soup = BeautifulSoup(resp.text, "lxml")
    fii_links = []

    # Captura todos os blocos tickerBox que possuem links
    boxes = soup.find_all("div", class_="tickerBox")
    for box in boxes:
        if "tickerBox--destaque" in box.get("class", []):
            continue
            
        a_tag = box.find("a", href=True)
        ticker_div = box.find("div", class_="tickerBox__title")
        
        if a_tag and ticker_div:
            ticker = ticker_div.get_text(strip=True).upper()
            if re.match(r'^[A-Z]{4}[0-9]{2}[A-Z]?$', ticker):
                fii_links.append((ticker, a_tag['href']))

    log(f"Iniciando coleta de preços para {len(fii_links)} FIIs em paralelo...")
    
    records = []
    session = requests.Session()
    
    # Usa threads para acelerar a coleta (20 por vez)
    with ThreadPoolExecutor(max_workers=20) as executor:
        future_to_fii = {executor.submit(get_individual_fii_price, session, t, u): t for t, u in fii_links}
        for future in as_completed(future_to_fii):
            ticker = future_to_fii[future]
            try:
                preco = future.result()
                if preco:
                    records.append({"papel": ticker, "cotacao": preco})
            except Exception:
                continue

    df = pd.DataFrame(records).drop_duplicates(subset="papel")
    log(f"{len(df)} FIIs com preço real extraídos.")
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