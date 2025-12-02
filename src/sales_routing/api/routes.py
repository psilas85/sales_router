#sales_router/src/sales_routing/api/routes.py

from fastapi import APIRouter, Depends
from sales_routing.api.dependencies import verify_token

router = APIRouter()

@router.get("/health")
async def health_check():
    return {"status": "ok", "service": "sales_routing"}

@router.post("/roteirizar", dependencies=[Depends(verify_token)])
async def executar_roteirizacao(tenant_id: int, envio_data: str):
    # Aqui você vai chamar o pipeline real depois
    return {
        "status": "queued",
        "tenant_id": tenant_id,
        "envio_data": envio_data
    }
