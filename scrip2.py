import requests
import pandas as pd
import os
import sys
from datetime import datetime
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

BRAPI_TOKEN = "5jH55f3zhdwazrTcxrnF4h"

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def fetch_all_fiis(token):
    """Busca todas as cotações de FIIs paginadas da BRAPI"""
    all_fiis = []
    limit = 100
    page = 1

    while True:
        url = "https://brapi.dev/api/quote/list"
        params = {
            "token": token,
            "type": "fund",
            "limit": limit,
            "page": page
        }
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        results = data.get("results", [])
        if not results:
            break

        all_fiis.extend(results)
        log(f"Página {page} — {len(results)} FIIs obtidos")

        # Verifica se há mais páginas
        total_count = data.get("totalCount", 0)
        if page * limit >= total_count:
            break
        page += 1

    return all_fiis

def main():
    try:
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_KEY")

        if not supabase_url or not supabase_key:
            log("ERRO: Variáveis SUPABASE_URL e SUPABASE_KEY obrigatórias")
            sys.exit(1)

        log("Conectando ao Supabase...")
        supabase = create_client(supabase_url, supabase_key)

        log("Buscando lista completa de FIIs via BRAPI...")
        all_fiis = fetch_all_fiis(BRAPI_TOKEN)
        log(f"Total geral de FIIs encontrados: {len(all_fiis)}")

        registros = []
        for ativo in all_fiis:
            registros.append({
                "papel": ativo.get("symbol"),
                "segmento": None,
                "cotacao": ativo.get("close"),
                "ffo_yield": None,
                "dividend_yield": ativo.get("dividendYield"),
                "p_vp": None,
                "valor_mercado": ativo.get("marketCap"),
                "liquidez": ativo.get("volume"),
                "qtd_imoveis": None,
                "preco_m2": None,
                "aluguel_m2": None,
                "cap_rate": None,
                "vacancia_media": None,
                "data_atualizacao": datetime.now().isoformat()
            })

        df = pd.DataFrame(registros)
        log(f"DataFrame criado com {len(df)} registros")

        log("Limpando dados antigos no Supabase...")
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
