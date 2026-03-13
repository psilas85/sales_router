#sales_router/src/geocoding_engine/api/geocoding_controller.py

from fastapi import FastAPI
from geocoding_engine.application.geocode_addresses_use_case import GeocodeAddressesUseCase

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse
import pandas as pd
import io

from geocoding_engine.application.geocode_spreadsheet_use_case import GeocodeSpreadsheetUseCase

app = FastAPI(
    title="SalesRouter Geocoding Engine",
    version="1.0.0"
)

@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/geocode/upload")
async def geocode_upload(file: UploadFile = File(...)):

    if not file.filename.lower().endswith((".xlsx", ".xls", ".csv")):
        raise HTTPException(status_code=400, detail="Arquivo deve ser XLSX ou CSV")

    try:

        if file.filename.lower().endswith(".csv"):
            df = pd.read_csv(file.file)
        else:
            df = pd.read_excel(file.file)

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro lendo planilha: {e}")

    uc = GeocodeSpreadsheetUseCase()

    df_result, stats = uc.execute(df)

    output = io.BytesIO()
    df_result.to_excel(output, index=False)
    output.seek(0)

    headers = {
        "X-Geocode-Total": str(stats["total"]),
        "X-Geocode-Sucesso": str(stats["sucesso"]),
        "X-Geocode-Falhas": str(stats["falhas"])
    }

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers
    )