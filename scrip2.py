import os
import sys
import requests
import pandas as pd
from datetime import datetime
from supabase import create_client, Client
from dotenv import load_dotenv
from typing import List, Optional, Dict
from dataclasses import dataclass

load_dotenv()

BRAPI_TOKEN = "5jH55f3zhdwazrTcxrnF4h"

@dataclass
class Quote:
    symbol: str
    short_name: str
    regular_market_price: float
    regular_market_change: float
    regular_market_change_percent: float
    currency: str
    market_cap: Optional[float] = None
    dividend_yield: Optional[float] = None
    volume: Optional[float] = None

    @classmethod
    def from_dict(cls, data: Dict) -> 'Quote':
        return cls(
            symbol=data['symbol'],
            short_name=data.get('shortName'),
            regular_market_price=data.get('regularMarketPrice'),
            regular_market_change=data.get('regularMarketChange'),
            regular_market_change_percent=data.get('regularMarketChangePercent'),
            currency=data.get('currency'),
            market_cap=data.get('marketCap'),
            dividend_yield=data.get('dividendYield'),
            volume=data.get('regularMarketVolume')
        )

class BrapiClient:
    BASE_URL = 'https://brapi.dev/api'

    def __init__(self, token: str):
        self.token = token
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Python BrapiClient/1.0'})

    def get_funds_list(self) -> List[str]:
        """Retorna todos os FIIs listados na B3"""
        url = f'{self.BASE_URL}/fund/list'
        params = {'token': self.token}
        resp = self.session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        symbols = [item['symbol'] for item in data.get('funds', [])]
        return symbols

    def get_multiple_quotes(self, tickers: List[str]) -> List[Quote]:
        """Busca cotações de múltiplos FIIs"""
        tickers_param = ','.join(tickers)
        url = f'{self.BASE_URL}/quote/fund/{tickers_param}'
        params = {'token': self.token}
        resp = self.session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return [Quote.from_dict(item) for item in data.get('results', [])]

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.session.close()

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

        supabase: Client = create_client(supabase_url, supabase_key)
        log("Conectado ao Supabase")

        with BrapiClient(BRAPI_TOKEN) as client:
            # 1️⃣ Pegar todos os FIIs
            tickers = client.get_funds_list()
            log(f"{len(tickers)} FIIs encontrados")

            # 2️⃣ Puxar cotações em lotes
            batch_size = 50
            registros = []

            for i in range(0, len(tickers), batch_size):
                batch = tickers[i:i+batch_size]
                log(f"Consultando lote {i//batch_size + 1} ({len(batch)} FIIs)")
                quotes = client.get_multiple_quotes(batch)
                for q in quotes:
                    registros.append({
                        "papel": q.symbol,
                        "segmento": None,
                        "cotacao": q.regular_market_price,
                        "ffo_yield": None,
                        "dividend_yield": q.dividend_yield,
                        "p_vp": None,
                        "valor_mercado": q.market_cap,
                        "liquidez": q.volume,
                        "qtd_imoveis": None,
                        "preco_m2": None,
                        "aluguel_m2": None,
                        "cap_rate": None,
                        "vacancia_media": None,
                        "data_atualizacao": datetime.now().isoformat()
                    })

        df = pd.DataFrame(registros)
        log(f"Total de {len(df)} registros coletados")

        # 3️⃣ Limpar tabela antiga e inserir novos dados
        log("Limpando dados antigos")
        supabase.table("fii_fundamentus").delete().neq("papel", "").execute()

        total_inserted = 0
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size].to_dict("records")
            supabase.table("fii_fundamentus").insert(batch).execute()
            total_inserted += len(batch)

        log(f"✓ Sucesso! {total_inserted} registros inseridos")

    except Exception as e:
        log(f"✗ ERRO: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
