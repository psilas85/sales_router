#sales_router/src/sales_routing/reporting/exporters/json_exporter.py

import json
from pathlib import Path
from loguru import logger


class JSONExporter:
    @staticmethod
    def export(data, output_path: str):
        if not data:
            logger.warning("⚠️ Nenhum dado para exportar.")
            return

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.success(f"✅ Relatório JSON salvo em {output_path}")
