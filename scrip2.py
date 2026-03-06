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

def main():
    try:
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_KEY")

        if not supabase_url or not supabase_key:
            log("ERRO: Variáveis SUPABASE_URL e SUPABASE_KEY obrigatórias")
            sys.exit(1)

        log("Conectando ao Supabase...")
        supabase: Client = create_client(supabase_url, supabase_key)

        # 1️⃣ Obter todos os FIIs listados na B3
        log("Obtendo todos os FIIs da B3 via Brapi...")
        list_url = f"https://brapi.dev/api/quote/list?token={BRAPI_TOKEN}&type=FII"
        resp = requests.get(list_url, timeout=30)
        resp.raise_for_status()
        list_data = resp.json()

        if "symbols" not in list_data:
            log("Resposta inesperada da API ao listar FIIs")
            sys.exit(1)

        tickers = [t["symbol"] for t in list_data["symbols"]]
        log(f"{len(tickers)} FIIs encontrados")

        # 2️⃣ Consultar dados de mercado em lotes
        batch_size = 50
        registros = []

        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i + batch_size]
            ticker_str = ",".join(batch)
            quote_url = f"https://brapi.dev/api/quote/{ticker_str}?token={BRAPI_TOKEN}"
            log(f"Consultando API para lote {i//batch_size + 1} ({len(batch)} FIIs)")
            r = requests.get(quote_url, timeout=30)
            r.raise_for_status()
            data = r.json()
            for ativo in data.get("results", []):
                registro = {
                    "papel": ativo.get("symbol"),
                    "segmento": None,
                    "cotacao": ativo.get("regularMarketPrice"),
                    "ffo_yield": None,
                    "dividend_yield": ativo.get("dividendYield"),
                    "p_vp": None,
                    "valor_mercado": ativo.get("marketCap"),
                    "liquidez": ativo.get("regularMarketVolume"),
                    "qtd_imoveis": None,
                    "preco_m2": None,
                    "aluguel_m2": None,
                    "cap_rate": None,
                    "vacancia_media": None,
                    "data_atualizacao": datetime.now().isoformat()
                }
                registros.append(registro)

        df = pd.DataFrame(registros)
        log(f"Total de {len(df)} registros coletados")

        # 3️⃣ Limpar tabela antiga e inserir novos registros
        log("Limpando dados antigos...")
        supabase.table("fii_fundamentus") \
            .delete() \
            .neq("papel", "") \
            .execute()

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
