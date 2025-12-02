#sales_router/src/sales_clusterization/mkp_pdv/domain/pdv_balanceamento.py

# sales_router/src/sales_clusterization/mkp_pdv/domain/pdv_balanceamento.py

import numpy as np
import pandas as pd
from loguru import logger
from sales_clusterization.domain.haversine_utils import haversine


class PDVBalanceamento:

    @staticmethod
    def balancear(lista, min_pdv, max_pdv, centros):
        """
        Balanceamento REAL de PDVs baseado no algoritmo de CEP balanceado.
        Com suporte total a 'bairro' e preservação de dados.
        """

        # -------------------------------------------------------------------
        # Convertendo lista → DataFrame
        # -------------------------------------------------------------------
        df = pd.DataFrame(lista)
        df.columns = df.columns.str.lower()

        # 🔧 PATCH — garantir coluna 'bairro'
        # Se não existir, cria usando cluster_bairro
        if "bairro" not in df.columns:
            df["bairro"] = df.get("cluster_bairro", "")

        # -------------------------------------------------------------------
        # Centros → DataFrame com case-normalized
        # -------------------------------------------------------------------
        centros_df = pd.DataFrame(centros)
        centros_df.columns = centros_df.columns.str.lower()

        # padronizar cluster_id como int
        df["cluster_id"] = df["cluster_id"].astype(int)
        centros_df["cluster_id"] = centros_df["cluster_id"].astype(int)

        # garantir que centro possua campo bairro
        if "bairro" not in centros_df.columns:
            centros_df["bairro"] = ""

        centros_dict = centros_df.set_index("cluster_id").to_dict("index")

        # parâmetros
        max_iter = 10
        OUTLIER_MAX_KM = 30
        total_mov = 0

        # ------------------------------------------------------
        # Recalcula distancia do PDV para o centro atual
        # ------------------------------------------------------
        df["dist_km"] = df.apply(
            lambda r: haversine(
                (r["lat"], r["lon"]),
                (r["cluster_lat"], r["cluster_lon"])
            ),
            axis=1
        )

        # Remove PDVs muito distantes do centro atual
        antes = len(df)
        df = df[df["dist_km"] <= OUTLIER_MAX_KM].copy()
        removidos = antes - len(df)
        if removidos > 0:
            logger.warning(
                f"🧹 Removidos {removidos} PDVs outliers (> {OUTLIER_MAX_KM} km)."
            )

        # ======================================================
        # 🔁 ITERAÇÕES DE BALANCEAMENTO
        # ======================================================
        for it in range(1, max_iter + 1):
            resumo = df.groupby("cluster_id")["pdv_id"].count().reset_index()
            resumo = resumo.rename(columns={"pdv_id": "qtd_pdvs"})

            acima = resumo[resumo["qtd_pdvs"] > max_pdv]
            abaixo = resumo[resumo["qtd_pdvs"] < min_pdv]

            logger.info(f"🔁 Iteração {it} — acima={len(acima)}, abaixo={len(abaixo)}")

            if acima.empty and abaixo.empty:
                logger.success("✅ Clusterização dentro dos limites min/max.")
                break

            mov1 = PDVBalanceamento._redistribuir_acima(
                df, centros_df, centros_dict, min_pdv, max_pdv
            )
            mov2 = PDVBalanceamento._fundir_abaixo(
                df, centros_df, centros_dict, min_pdv, max_pdv
            )

            mov = mov1 + mov2
            total_mov += mov

            if mov == 0:
                logger.warning("⚠️ Nenhuma realocação possível nesta iteração.")
                break

            # Atualiza centroides mantendo bairro original dos centros
            novos_centroides = (
                df.groupby("cluster_id")[["cluster_lat", "cluster_lon"]]
                .mean()
                .reset_index()
                .rename(columns={"cluster_lat": "lat", "cluster_lon": "lon"})
            )

            # Reanexar bairros antigos
            novos_centroides = novos_centroides.merge(
                centros_df[["cluster_id", "bairro"]],
                on="cluster_id",
                how="left"
            )

            novos_centroides["bairro"] = novos_centroides["bairro"].fillna("")

            centros_df = novos_centroides.copy()
            centros_dict = centros_df.set_index("cluster_id").to_dict("index")

        # ======================================================
        # Recalcula distância e tempo final
        # ======================================================
        df["cluster_lat"] = df["cluster_id"].apply(lambda cid: centros_dict[cid]["lat"])
        df["cluster_lon"] = df["cluster_id"].apply(lambda cid: centros_dict[cid]["lon"])

        # garantir cluster_bairro correto
        df["cluster_bairro"] = df["cluster_id"].apply(
            lambda cid: centros_dict[cid].get("bairro") or ""
        )

        df["dist_km"] = df.apply(
            lambda r: haversine(
                (r["lat"], r["lon"]),
                (r["cluster_lat"], r["cluster_lon"]),
            ),
            axis=1,
        )

        df["tempo_min"] = df["dist_km"] * (60 / 40)

        logger.success(f"🏁 Balanceamento concluído — {total_mov} movimentações.")

        return df.to_dict(orient="records")


    # ======================================================================
    # Redistribuir clusters acima do limite
    # ======================================================================
    @staticmethod
    def _redistribuir_acima(df, centros_df, centros_dict, min_pdv, max_pdv):
        alteracoes = 0
        stats = df.groupby("cluster_id")["pdv_id"].count().to_dict()

        for cid, total in stats.items():
            if total <= max_pdv:
                continue

            excedente = total - max_pdv
            centro_ref = centros_df[centros_df["cluster_id"] == cid]
            if centro_ref.empty:
                continue

            lat_c, lon_c = centro_ref.iloc[0]["lat"], centro_ref.iloc[0]["lon"]

            centros_df["dist_km"] = centros_df.apply(
                lambda r: haversine((lat_c, lon_c), (r["lat"], r["lon"])),
                axis=1
            )

            vizinhos = centros_df[centros_df["cluster_id"] != cid].copy()
            vizinhos["qtd"] = vizinhos["cluster_id"].map(stats).fillna(0)
            vizinhos["capacidade"] = max_pdv - vizinhos["qtd"]
            vizinhos = vizinhos[vizinhos["capacidade"] > 0].sort_values("dist_km")

            if vizinhos.empty:
                continue

            pdvs_cluster = df[df["cluster_id"] == cid].copy()
            pdvs_cluster["dist_centro"] = pdvs_cluster.apply(
                lambda r: haversine((r["lat"], r["lon"]), (lat_c, lon_c)),
                axis=1
            )

            pdvs_para_mover = pdvs_cluster.sort_values("dist_centro", ascending=False)

            for _, viz in vizinhos.iterrows():
                if excedente <= 0:
                    break

                mover_n = min(excedente, int(viz["capacidade"]))
                mover_df = pdvs_para_mover.head(mover_n)
                if mover_df.empty:
                    continue

                novo_id = viz["cluster_id"]
                novo = centros_dict[novo_id]
                mask = mover_df.index

                df.loc[mask, "cluster_id"] = novo_id
                df.loc[mask, "cluster_lat"] = novo["lat"]
                df.loc[mask, "cluster_lon"] = novo["lon"]
                df.loc[mask, "cluster_bairro"] = (
                    novo.get("bairro") or df.loc[mask, "bairro"]
                )

                excedente -= mover_n
                alteracoes += mover_n

                logger.info(
                    f"🔁 Movidos {mover_n} PDVs de {cid} → {novo_id} ({viz['dist_km']:.1f} km)."
                )

        return alteracoes

    # ======================================================================
    # Fundir clusters abaixo do limite
    # ======================================================================
    @staticmethod
    def _fundir_abaixo(df, centros_df, centros_dict, min_pdv, max_pdv):
        alteracoes = 0
        stats = df.groupby("cluster_id")["pdv_id"].count().to_dict()

        for cid, total in stats.items():
            if total >= min_pdv:
                continue

            centro_ref = centros_df[centros_df["cluster_id"] == cid]
            if centro_ref.empty:
                continue

            lat_c, lon_c = centro_ref.iloc[0]["lat"], centro_ref.iloc[0]["lon"]

            centros_df["dist_km"] = centros_df.apply(
                lambda r: haversine((lat_c, lon_c), (r["lat"], r["lon"])),
                axis=1
            )

            candidatos = centros_df[centros_df["cluster_id"] != cid].copy()
            if candidatos.empty:
                continue

            viz = candidatos.sort_values("dist_km").iloc[0]

            novo_id = viz["cluster_id"]
            novo = centros_dict[novo_id]

            mask = df["cluster_id"] == cid

            df.loc[mask, "cluster_id"] = novo_id
            df.loc[mask, "cluster_lat"] = novo["lat"]
            df.loc[mask, "cluster_lon"] = novo["lon"]
            df.loc[mask, "cluster_bairro"] = (
                novo.get("bairro") or df.loc[mask, "bairro"]
            )

            alteracoes += total

            novo_total = df[df["cluster_id"] == novo_id].shape[0]

            if novo_total > max_pdv:
                logger.warning(
                    f"⚠️ Fusão de {cid} → {novo_id} excedeu capacidade ({novo_total}>{max_pdv})."
                )
            else:
                logger.info(
                    f"🤝 Cluster {cid} ({total}) fundido ao {novo_id} ({viz['dist_km']:.1f} km)."
                )

        return alteracoes
