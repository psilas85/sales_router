# src/pdv_preprocessing/infrastructure/database_writer.py

import logging
import time
from pdv_preprocessing.entities.pdv_entity import PDV


class DatabaseWriter:
    def __init__(self, conn):
        self.conn = conn

    # ==========================================================
    # 🧭 Insere coordenadas no cache persistente
    # ==========================================================
    def inserir_localizacao(self, endereco: str, lat: float, lon: float):
        try:
            inicio = time.time()
            cur = self.conn.cursor()
            cur.execute("""
                INSERT INTO enderecos_cache (endereco, lat, lon)
                VALUES (%s, %s, %s)
                ON CONFLICT (endereco) DO NOTHING
            """, (endereco.strip().lower(), lat, lon))
            self.conn.commit()
            dur = time.time() - inicio
            cur.close()
            logging.info(f"💾 [CACHE_DB] ({dur:.2f}s) Endereço salvo no cache: {endereco}")
        except Exception as e:
            logging.error(f"❌ [CACHE_DB] Erro ao inserir localização: {e}")

    # ==========================================================
    # 🏪 Insere ou atualiza PDVs
    # ==========================================================
    def inserir_pdvs(self, lista_pdvs):
        try:
            inicio = time.time()
            cur = self.conn.cursor()
            for p in lista_pdvs:
                cur.execute("""
                    INSERT INTO pdvs (
                        tenant_id, cnpj, logradouro, numero, bairro, cidade, uf, cep,
                        pdv_endereco_completo, pdv_lat, pdv_lon, status_geolocalizacao
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (tenant_id, cnpj) DO UPDATE SET
                        logradouro = EXCLUDED.logradouro,
                        numero = EXCLUDED.numero,
                        bairro = EXCLUDED.bairro,
                        cidade = EXCLUDED.cidade,
                        uf = EXCLUDED.uf,
                        cep = EXCLUDED.cep,
                        pdv_endereco_completo = EXCLUDED.pdv_endereco_completo,
                        pdv_lat = EXCLUDED.pdv_lat,
                        pdv_lon = EXCLUDED.pdv_lon,
                        status_geolocalizacao = EXCLUDED.status_geolocalizacao,
                        atualizado_em = NOW();
                """, (
                    p.tenant_id, p.cnpj, p.logradouro, p.numero, p.bairro, p.cidade,
                    p.uf, p.cep, p.pdv_endereco_completo, p.pdv_lat, p.pdv_lon,
                    p.status_geolocalizacao
                ))
            self.conn.commit()
            dur = time.time() - inicio
            cur.close()
            logging.info(f"💾 [PDV_DB] {len(lista_pdvs)} PDVs inseridos/atualizados ({dur:.2f}s)")
        except Exception as e:
            logging.error(f"❌ [PDV_DB] Erro ao inserir/atualizar PDVs: {e}")
