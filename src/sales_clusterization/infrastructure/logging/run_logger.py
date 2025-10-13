#sales_clusterization/infrastructure/logging/run_logger.py

import json

def snapshot_params(**kwargs) -> str:
    return json.dumps({k: v for k, v in kwargs.items()}, ensure_ascii=False)
