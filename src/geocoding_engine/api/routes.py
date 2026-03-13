#sales_router/src/geocoding_engine/api/routes.py

from fastapi import APIRouter, Depends, UploadFile, File, HTTPException
from fastapi.responses import FileResponse
from .dependencies import verify_token
from .schemas import GeocodeRequest
from geocoding_engine.application.geocode_addresses_use_case import GeocodeAddressesUseCase
from geocoding_engine.application.geocode_spreadsheet_use_case import GeocodeSpreadsheetUseCase

from loguru import logger
import tempfile
import uuid
import os
import pandas as pd
import time


router = APIRouter()


# ============================================================
# HEALTH
# ============================================================

@router.get("/health")
def health():
    return {"status": "ok", "service": "geocoding_engine"}


# ============================================================
# GEOCODE JSON
# ============================================================

@router.post(
    "/geocode",
    dependencies=[Depends(verify_token)]
)
def geocode_json(body: GeocodeRequest):

    endereco = f"{body.endereco}, {body.cidade} - {body.uf}"

    logger.info(f"[GEOCODE] request recebido: {endereco}")

    uc = GeocodeAddressesUseCase()

    res = uc.execute([
        {
            "id": 1,
            "address": endereco
        }
    ])

    r = res["results"][0]

    logger.info(
        f"[GEOCODE] resultado lat={r['lat']} lon={r['lon']} source={r['source']}"
    )

    return {
        "lat": r["lat"],
        "lon": r["lon"],
        "status": "ok" if r["lat"] else "not_found"
    }


# ============================================================
# GEOCODE PLANILHA
# ============================================================

@router.post(
    "/upload",
    dependencies=[Depends(verify_token)]
)
async def geocode_upload(file: UploadFile = File(...)):

    start = time.time()

    logger.info(f"[UPLOAD] arquivo recebido: {file.filename}")

    if not file.filename.endswith(".xlsx"):
        logger.warning(f"[UPLOAD] formato inválido: {file.filename}")
        raise HTTPException(status_code=400, detail="Arquivo deve ser XLSX")

    temp_input = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    temp_input.write(await file.read())
    temp_input.close()

    logger.info(f"[UPLOAD] salvo temporário: {temp_input.name}")

    df = pd.read_excel(temp_input.name)

    logger.info(f"[UPLOAD] linhas carregadas: {len(df)}")

    uc = GeocodeSpreadsheetUseCase()

    df_out, stats = uc.execute(df)

    OUTPUT_DIR = "/app/output/geocoding_engine"
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    output_path = f"{OUTPUT_DIR}/geocode_{uuid.uuid4()}.xlsx"

    df_out.to_excel(output_path, index=False)

    elapsed = round(time.time() - start, 2)

    logger.info(
        f"[RESULT] total={stats['total']} sucesso={stats['sucesso']} "
        f"falhas={stats['falhas']} tempo={elapsed}s arquivo={output_path}"
    )

    return FileResponse(
        path=output_path,
        filename="resultado_geocode.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": "attachment; filename=resultado_geocode.xlsx"
        }
    )