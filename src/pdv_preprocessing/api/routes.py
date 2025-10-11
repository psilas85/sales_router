# src/pdv_preprocessing/api/routes.py
from fastapi import APIRouter, HTTPException, Query, Depends, Request
from database.db_connection import get_connection
from pdv_preprocessing.infrastructure.database_reader import DatabaseReader
from pdv_preprocessing.infrastructure.database_writer import DatabaseWriter
from pdv_preprocessing.entities.pdv_entity import PDV
from .dependencies import verify_token

router = APIRouter()

# ==========================================================
# 🧠 Health check (sem autenticação)
# ==========================================================
@router.get("/health")
def health_check():
    return {"status": "ok", "message": "PDV Preprocessing API saudável 🧩"}

# ==========================================================
# 🔍 Buscar PDV por CNPJ (autenticado)
# ==========================================================
@router.get("/buscar", dependencies=[Depends(verify_token)])
def buscar_pdv(request: Request, tenant_id: int = Query(...), cnpj: str = Query(...)):
    user = request.state.user
    conn = get_connection()
    reader = DatabaseReader(conn)
    pdv = reader.buscar_pdv_por_cnpj(tenant_id, cnpj)
    conn.close()

    if not pdv:
        raise HTTPException(status_code=404, detail="PDV não encontrado.")

    return {"pdv": pdv, "usuario": user}

# ==========================================================
# 📋 Listar PDVs (autenticado)
# ==========================================================
@router.get("/listar", dependencies=[Depends(verify_token)])
def listar_pdvs(request: Request, tenant_id: int):
    user = request.state.user
    conn = get_connection()
    reader = DatabaseReader(conn)
    df = reader.listar_pdvs_por_tenant(tenant_id)
    conn.close()

    if df.empty:
        return {"total": 0, "pdvs": []}

    return {"usuario": user, "total": len(df), "pdvs": df.to_dict(orient="records")}

# ==========================================================
# ✏️ Atualizar PDV (autenticado + role check)
# ==========================================================
@router.put("/atualizar", dependencies=[Depends(verify_token)])
def atualizar_pdv(
    request: Request,
    tenant_id: int,
    cnpj: str,
    logradouro: str = None,
    numero: str = None,
    bairro: str = None,
    cidade: str = None,
    uf: str = None,
    cep: str = None,
    lat: float = None,
    lon: float = None
):
    user = request.state.user

    # 🔒 Somente admin e operacional podem editar
    if user.get("role") not in ["admin", "operacional"]:
        raise HTTPException(status_code=403, detail="Usuário sem permissão para editar PDVs")

    conn = get_connection()
    reader = DatabaseReader(conn)
    writer = DatabaseWriter(conn)

    existente = reader.buscar_pdv_por_cnpj(tenant_id, cnpj)
    if not existente:
        raise HTTPException(status_code=404, detail="PDV não encontrado.")

    atualizado = {**existente}
    for campo, valor in {
        "logradouro": logradouro,
        "numero": numero,
        "bairro": bairro,
        "cidade": cidade,
        "uf": uf,
        "cep": cep,
        "pdv_lat": lat,
        "pdv_lon": lon
    }.items():
        if valor is not None:
            atualizado[campo] = valor

    atualizado["pdv_endereco_completo"] = (
        f"{atualizado.get('logradouro', '')}, {atualizado.get('numero', '')}, "
        f"{atualizado.get('bairro', '')}, {atualizado.get('cidade', '')} - "
        f"{atualizado.get('uf', '')}, {atualizado.get('cep', '')}"
    )
    atualizado["status_geolocalizacao"] = "manual_edit"

    pdv = PDV(**{**atualizado, "tenant_id": tenant_id})
    writer.inserir_pdvs([pdv])
    conn.close()

    return {"status": "success", "message": "PDV atualizado com sucesso", "usuario": user, "pdv": atualizado}
