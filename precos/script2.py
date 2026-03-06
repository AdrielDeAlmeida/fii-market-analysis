import requests
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
    Busca todos os FIIs com preço atual via API interna do Status Invest.
    Retorna JSON com ticker e cotação D+0 — sem necessidade de Selenium.
    """
    url = "https://statusinvest.com.br/category/advancedsearchresultpaginated"
    params = {
        "search": '{"Sector":"","SubSector":"","Segment":"","my_range":"-;-","forecast":{"upsideDownside":{"minValue":"-","maxValue":"-"},"estimatesNumber":{"minValue":"-","maxValue":"-"},"revisedUp":{"minValue":"-","maxValue":"-"},"revisedDown":{"minValue":"-","maxValue":"-"}},"dy":{"minValue":"-","maxValue":"-"},"p_l":{"minValue":"-","maxValue":"-"},"peg_ratio":{"minValue":"-","maxValue":"-"},"p_assets":{"minValue":"-","maxValue":"-"},"p_cap_giro":{"minValue":"-","maxValue":"-"},"p_ebit":{"minValue":"-","maxValue":"-"},"p_ativo_circ_liq":{"minValue":"-","maxValue":"-"},"vpa":{"minValue":"-","maxValue":"-"},"p_vpa":{"minValue":"-","maxValue":"-"},"p_sr":{"minValue":"-","maxValue":"-"},"p_working_cap":{"minValue":"-","maxValue":"-"},"p_fcf":{"minValue":"-","maxValue":"-"},"ev_ebit":{"minValue":"-","maxValue":"-"},"ev_ebitda":{"minValue":"-","maxValue":"-"},"mrg_ebit":{"minValue":"-","maxValue":"-"},"mrg_liq":{"minValue":"-","maxValue":"-"},"liq_corr":{"minValue":"-","maxValue":"-"},"roic":{"minValue":"-","maxValue":"-"},"roe":{"minValue":"-","maxValue":"-"},"patrimonio":{"minValue":"-","maxValue":"-"},"receita_liq":{"minValue":"-","maxValue":"-"},"lucro_liq":{"minValue":"-","maxValue":"-"},"liq":{"minValue":"-","maxValue":"-"}}',
        "orderColumn": "",
        "isAsc": "",
        "page": 0,
        "take": 2000,
        "categoryType": 2  # 2 = FIIs
    }
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://statusinvest.com.br/fundos-imobiliarios",
        "X-Requested-With": "XMLHttpRequest",
    }

    log("Buscando FIIs via API do Status Invest...")
    resp = requests.get(url, params=params, headers=headers, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    items = data.get("list", [])

    if not items:
        raise RuntimeError("API retornou lista vazia.")

    records = []
    for item in items:
        ticker = str(item.get("ticker", "")).strip().upper()
        preco = item.get("price", None)

        if not ticker or preco is None:
            continue

        try:
            records.append({
                "papel": ticker,
                "cotacao": round(float(preco), 2),
            })
        except (ValueError, TypeError):
            continue

    df = pd.DataFrame(records)
    log(f"{len(df)} FIIs extraídos do Status Invest.")
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
    df = get_fii_data_statusinvest()

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

    log(f"✓ Concluído! {total_inserted} FIIs inseridos na tabela precoreal (preços D+0).")


if __name__ == "__main__":
    main()
