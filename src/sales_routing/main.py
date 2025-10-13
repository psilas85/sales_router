# src/sales_routing/main.py

import time
from loguru import logger

if __name__ == "__main__":
    logger.info("🚀 Sales Routing container iniciado.")
    logger.info("⏸️ Modo CLI ativo — aguardando execuções manuais de run_routing.py")
    logger.info("💡 Exemplo de uso:")
    logger.info("   docker exec -it sales_routing python3 -m src.cli.run_routing "
                "--uf CE --cidade Fortaleza --workday 600 --routekm 200 --service 15 --vel 40 --alpha 1.4")

    # Mantém o container ativo indefinidamente
    while True:
        time.sleep(3600)
