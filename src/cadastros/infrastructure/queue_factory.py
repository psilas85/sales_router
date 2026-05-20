# sales_router/src/cadastros/infrastructure/queue_factory.py
#
# Fila RQ do cadastro de Clientes. Usada pelo import em lote de PDVs via
# planilha — o endpoint enfileira o job e o worker `sales_cadastros_worker`
# consome a fila `cadastros_import`.

import os

from redis import Redis
from rq import Queue

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
redis_conn = Redis.from_url(REDIS_URL)

FILA_IMPORT = "cadastros_import"


def fila_import() -> Queue:
    return Queue(FILA_IMPORT, connection=redis_conn, default_timeout=3600)
