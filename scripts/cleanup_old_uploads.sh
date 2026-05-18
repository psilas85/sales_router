#!/usr/bin/env bash
# =============================================================
# Cleanup de uploads de PDV antigos
# =============================================================
# Remove arquivos `pdvs_*` do diretório de uploads que sejam mais antigos
# que N dias (default: 30).
#
# Esses arquivos são gravados pelo endpoint /pdv/upload-file
# (src/pdv_preprocessing/api/routes.py:upload_arquivo) em /app/data dentro
# do container, que é bind-mountado em ./data no host (docker-compose.yml).
# Os dados estruturados já estão no PostgreSQL — o XLSX original é só
# fonte para reprocessamento manual em caso de incidente.
#
# Uso:
#   ./cleanup_old_uploads.sh                # remove > 30 dias
#   ./cleanup_old_uploads.sh 60             # remove > 60 dias
#   DRY_RUN=1 ./cleanup_old_uploads.sh      # só lista, sem deletar
#   DATA_DIR=/path/to/data ./cleanup...sh   # overrideando o caminho
#
# Crontab sugerida (rodar diariamente às 03:00):
#   0 3 * * * /home/ubuntu/sales_router/scripts/cleanup_old_uploads.sh >> /var/log/sales_router_cleanup.log 2>&1
#
# Ver entradas atuais: `crontab -l`
# Editar:              `crontab -e`
# =============================================================

set -euo pipefail

DATA_DIR="${DATA_DIR:-/home/ubuntu/sales_router/data}"
RETENTION_DAYS="${1:-30}"
DRY_RUN="${DRY_RUN:-0}"

if [ ! -d "$DATA_DIR" ]; then
  echo "[$(date -Is)] ERRO: DATA_DIR não encontrado: $DATA_DIR" >&2
  exit 1
fi

echo "[$(date -Is)] Limpando uploads PDV > ${RETENTION_DAYS} dias em ${DATA_DIR}"

# -maxdepth 1     → só na raiz do data/, não recursivo
# -name 'pdvs_*'  → só arquivos do upload PDV (não toca em mais nada)
# -type f         → só arquivos regulares (ignora pastas, links, etc)
# -mtime +N       → modificados há mais de N dias

if [ "$DRY_RUN" = "1" ]; then
  echo "[$(date -Is)] DRY-RUN ativo. Os arquivos abaixo SERIAM removidos:"
  find "$DATA_DIR" -maxdepth 1 -name 'pdvs_*' -type f -mtime "+${RETENTION_DAYS}" -print
  echo "[$(date -Is)] Nenhum arquivo deletado (DRY_RUN=1)."
else
  # -print -delete: imprime cada arquivo antes de deletar (find avalia em ordem)
  removidos=$(find "$DATA_DIR" -maxdepth 1 -name 'pdvs_*' -type f -mtime "+${RETENTION_DAYS}" -print -delete | wc -l)
  echo "[$(date -Is)] ${removidos} arquivo(s) removido(s)."
fi
