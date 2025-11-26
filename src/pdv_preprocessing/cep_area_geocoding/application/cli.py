#sales_router/src/pdv_preprocessing/cep_area_geocoding/application/cli.py

import argparse
import uuid
from pdv_preprocessing.cep_area_geocoding.infrastructure.database_reader import DatabaseReader
from pdv_preprocessing.cep_area_geocoding.infrastructure.database_writer import DatabaseWriter
from pdv_preprocessing.cep_area_geocoding.domain.area_geolocation_service import AreaGeolocationService
from pdv_preprocessing.cep_area_geocoding.application.area_geocoding_use_case import AreaGeocodingUseCase


def main():
    parser = argparse.ArgumentParser(description="CEP Bairro Geocoding")
    parser.add_argument("--tenant", required=True, type=int)
    parser.add_argument("--descricao", required=True)
    parser.add_argument("--arquivo", required=True, help="Caminho para o CSV enviado pela aplicação")

    args = parser.parse_args()

    input_id = str(uuid.uuid4())

    reader = DatabaseReader()
    writer = DatabaseWriter()
    geo = AreaGeolocationService(reader, writer)
    use_case = AreaGeocodingUseCase(reader, writer, geo)

    use_case.processar(
        tenant_id=args.tenant,
        input_id=input_id,
        descricao=args.descricao,
        arquivo=args.arquivo
    )


if __name__ == "__main__":
    main()
