import requests
import pandas as pd
import os
import sys
from datetime import datetime
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

BRAPI_TOKEN = "5jH55f3zhdwazrTcxrnF4h"
ANBIMA_BASE_URL = "https://api.anbima.com.br/feed/fundos/v1"

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def fetch_anbima_funds():
    """Busca todos os fundos da ANBIMA com paginação."""
    all_funds = []
    page = 1
    size = 1000

    while True:
        url = f"{ANBIMA_BASE_URL}/fundos"
        params = {"page": page, "size": size}
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        if not data:
            break

        all_funds.extend(data)
        log(f"ANBIMA: página {page} — {len(data)} fundos")

        if len(data) < size:
            break
        page += 1

    return all_funds

def filter_fiis(funds):
    """Filtra fundos que parecem FIIs com base no nome/ISIN."""
    fii_list = []
    for f in funds:
        isin = f.get("codigo_isin", "")
        name = f.get("nome_fantasia", "").upper()
        # Heurística: ticker/ISIN com "FII" ou nome com fundo imobiliário
        if "FII" in name or isin.endswith("11"):
            fii_list.append(f)
    return fii_list

def fetch_brp_quote(ticker):
    """Busca cotação via BRAPI para um ticker."""
    url = f"https://brapi.dev/api/quote/{ticker}"
    params = {"token": BRAPI_TOKEN}
    response = requests.get(url, params=params, timeout=15)
    response.raise_for_status()
    data = response.json()
    results = data.get("results")
    if results:
        return results[0]
    return None

def main():
    try:
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_KEY")

        if not supabase_url or not supabase_key:
            log("ERRO: SUPABASE_URL e SUPABASE_KEY obrigatórias")
            sys.exit(1)

        log("Conectando ao Supabase...")
        supabase: Client = create_client(supabase_url, supabase_key)

        log("Buscando lista completa de fundos na ANBIMA...")
        all_funds = fetch_anbima_funds()
        log(f"{len(all_funds)} fundos totais encontrados na ANBIMA")

        log("Filtrando FIIs...")
        fii_candidates = filter_fiis(all_funds)
        log(f"{len(fii_candidates)} fundos candidatos a FII após filtragem")

        registros = []
        for f in fii_candidates:
            ticker = f.get("codigo_isin")[:6]  # pegar as primeiras 6 letras como ticker
            quote = None
            try:
                quote = fetch_brp_quote(ticker)
            except Exception as e:
                log(f"BRAPI erro no ticker {ticker}: {str(e)}")

            registros.append({
                "papel": ticker,
                "segmento": None,
                "cotacao": quote.get("regularMarketPrice") if quote else None,
                "ffo_yield": None,
                "dividend_yield": quote.get("dividendYield") if quote else None,
                "p_vp": None,
                "valor_mercado": quote.get("marketCap") if quote else None,
                "liquidez": quote.get("regularMarketVolume") if quote else None,
                "qtd_imoveis": None,
                "preco_m2": None,
                "aluguel_m2": None,
                "cap_rate": None,
                "vacancia_media": None,
                "data_atualizacao": datetime.now().isoformat()
            })

        df = pd.DataFrame(registros)
        log(f"Total de {len(df)} registros FIIs coletados")

        log("Limpando tabela antiga no Supabase...")
        supabase.table("fii_fundamentus").delete().neq("papel", "").execute()

        batch_size = 100
        total_inserted = 0

        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size].to_dict("records")
            supabase.table("fii_fundamentus").insert(batch).execute()
            total_inserted += len(batch)

        log(f"✓ Sucesso! {total_inserted} registros inseridos no Supabase")

    except Exception as e:
        log(f"✗ ERRO: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
