from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
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
    driver = None
    try:

        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_KEY")

        if not supabase_url or not supabase_key:
            log("ERRO: Variáveis SUPABASE_URL e SUPABASE_KEY obrigatórias")
            sys.exit(1)

        log("Conectando ao Supabase...")
        supabase: Client = create_client(supabase_url, supabase_key)

        log("Configurando WebDriver...")

        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("user-agent=Mozilla/5.0")

        driver = webdriver.Chrome(options=options)

        # NOVA FONTE
        url = "https://fiis.com.br/lista-de-fundos-imobiliarios/"

        log(f"Acessando {url}")

        driver.set_page_load_timeout(30)
        driver.get(url)

        wait = WebDriverWait(driver, 30)

        table = wait.until(
            EC.presence_of_element_located((By.TAG_NAME, "table"))
        )

        html_content = table.get_attribute("outerHTML")

        log("Tabela carregada")

        df = pd.read_html(StringIO(html_content))[0]

        log(f"{len(df)} registros capturados")

        # Ajustar nomes para manter padrão do script original
        df = df.rename(columns={
            "Ticker": "papel",
            "Segmento": "segmento",
            "Preço Atual": "cotacao",
            "Dividend Yield": "dividend_yield",
            "P/VP": "p_vp",
            "Liquidez Diária": "liquidez",
            "Qtd. Imóveis": "qtd_imoveis",
            "Preço/m²": "preco_m2",
            "Aluguel/m²": "aluguel_m2",
            "Cap Rate": "cap_rate",
            "Vacância Média": "vacancia_media"
        })

        # criar coluna que não existe na nova fonte
        if "ffo_yield" not in df.columns:
            df["ffo_yield"] = None

        if "valor_mercado" not in df.columns:
            df["valor_mercado"] = None

        # manter ordem original
        df = df[
            [
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
        ]

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

        log(f"Sucesso! {total_inserted} registros inseridos")

    except Exception as e:

        log(f"ERRO: {str(e)}")

        import traceback
        traceback.print_exc()

        sys.exit(1)

    finally:

        if driver:
            driver.quit()
            log("WebDriver fechado")


if __name__ == "__main__":
    main()
