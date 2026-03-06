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


# Carregar variáveis de ambiente (para testes locais)
load_dotenv()


# Configuração de logging
def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


def main():

    driver = None

    try:

        # Verificar variáveis de ambiente obrigatórias
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_KEY")

        if not supabase_url or not supabase_key:
            log("ERRO: Variáveis de ambiente SUPABASE_URL e SUPABASE_KEY são obrigatórias!")
            sys.exit(1)

        log("Iniciando conexão com Supabase...")

        supabase: Client = create_client(supabase_url, supabase_key)

        log("Conexão com Supabase estabelecida com sucesso!")

        # Configurando o WebDriver
        log("Configurando WebDriver...")

        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")

        # User-Agent realista para evitar bloqueios
        options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        driver = webdriver.Chrome(options=options)

        # URL da página
        url = "https://www.fundamentus.com.br/fii_resultado.php"

        log(f"Acessando {url}...")

        # Aumentar timeout da página
        driver.set_page_load_timeout(30)

        driver.get(url)

        # Aguarde o carregamento da página
        wait = WebDriverWait(driver, 30)

        try:

            table = wait.until(
                EC.presence_of_element_located((By.ID, "tabelaResultado"))
            )

            html_content = table.get_attribute("outerHTML")

            log("Tabela carregada com sucesso!")

        except Exception as te:

            log("Erro ao carregar tabela. Salvando evidência...")

            # Opcional: verificar se existe captcha
            # print(driver.page_source[:500])

            raise te

        # Convertendo a tabela para DataFrame
        df = pd.read_html(StringIO(html_content))[0]

        # Ajustando e renomeando as colunas
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
            "vacancia_media",
        ]

        log(f"Total de {len(df)} registros extraídos")

        # Adicionar timestamp de atualização
        df["data_atualizacao"] = datetime.now().isoformat()

        # Limpar dados existentes
        log("Limpando dados antigos da tabela...")

        supabase.table("fii_fundamentus").delete().neq("papel", "").execute()

        # Converter DataFrame para lista de dicionários
        records = df.to_dict("records")

        # Inserir dados em lotes (limite do Supabase)
        batch_size = 100
        total_inserted = 0

        for i in range(0, len(records), batch_size):

            batch = records[i : i + batch_size]

            log(f"Inserindo lote {i // batch_size + 1} ({len(batch)} registros)...")

            response = supabase.table("fii_fundamentus").insert(batch).execute()

            total_inserted += len(batch)

        log(f"✓ Sucesso! {total_inserted} registros inseridos no Supabase")

    except Exception as e:

        log(f"✗ ERRO: {str(e)}")

        import traceback

        traceback.print_exc()

        sys.exit(1)

    finally:

        # Fechando o WebDriver
        if driver:
            driver.quit()

        log("WebDriver fechado")


if __name__ == "__main__":
    main()
