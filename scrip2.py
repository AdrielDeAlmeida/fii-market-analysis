import requests
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

def main():

    try:

        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_KEY")

        if not supabase_url or not supabase_key:
            log("ERRO: Variáveis SUPABASE_URL e SUPABASE_KEY são obrigatórias")
            sys.exit(1)

        log("Conectando ao Supabase...")
        supabase: Client = create_client(supabase_url, supabase_key)

        url = "https://www.fundamentus.com.br/fii_resultado.php"

        log(f"Baixando dados de {url}")

        headers = {
            "User-Agent": "Mozilla/5.0"
        }

        response = requests.get(url, headers=headers, timeout=30)

        if response.status_code != 200:
            log(f"Erro HTTP {response.status_code}")
            sys.exit(1)

        log("Página carregada")

        tables = pd.read_html(StringIO(response.text))

        if len(tables) == 0:
            log("Nenhuma tabela encontrada")
            sys.exit(1)

        df = tables[0]

        df.columns = [
            "papel",
            "segmento",
            "cotacao",
            "ffo_yield",
            "dividend_yield",
            "p_vp",
            "valor_mercado",
            "liquidez",
            "qtd_imoveis",
            "preco_m2",
            "aluguel_m2",
            "cap_rate",
            "vacancia_media"
        ]

        log(f"{len(df)} registros encontrados")

        df["data_atualizacao"] = datetime.now().isoformat()

        log("Limpando dados antigos")

        supabase.table("fii_fundamentus") \
            .delete() \
            .neq("papel", "") \
            .execute()

        records = df.to_dict("records")

        batch_size = 100
        total_inserted = 0

        for i in range(0, len(records), batch_size):

            batch = records[i:i + batch_size]

            log(f"Inserindo lote {i//batch_size + 1}")

            supabase.table("fii_fundamentus") \
                .insert(batch) \
                .execute()

            total_inserted += len(batch)

        log(f"✓ Sucesso! {total_inserted} registros inseridos no Supabase")

    except Exception as e:

        log(f"✗ ERRO: {str(e)}")

        import traceback
        traceback.print_exc()

        sys.exit(1)

if __name__ == "__main__":
    main()
