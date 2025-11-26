# ============================================================
# 📦 src/sales_clusterization/domain/operational_cluster_refiner.py
# ============================================================

import numpy as np
import math
from math import radians, sin, cos, sqrt, atan2
from loguru import logger
from typing import List, Tuple, Dict
from src.sales_clusterization.domain.sector_generator import kmeans_setores
from src.sales_clusterization.domain.k_estimator import _haversine_km


# ============================================================
# ⚙️ Classe principal
# ============================================================
class OperationalClusterRefiner:
    """
    Ajusta clusters com base em limites operacionais de tempo e distância.
    - Cria subrotas planejadas por capacidade mensal (dias_uteis/freq)
    - Usa rota simulada (vizinho mais próximo)
    - Reexecuta KMeans iterativamente até atender restrições.
    """

    def __init__(
        self,
        v_kmh: float,
        max_time_min: float,
        max_dist_km: float,
        tempo_servico_min: float,
        max_iter: int,
        tenant_id: int = None,
    ):
        self.v_kmh = v_kmh
        self.max_time_min = max_time_min
        self.max_dist_km = max_dist_km
        self.tempo_servico_min = tempo_servico_min
        self.max_iter = max_iter
        self.tenant_id = tenant_id

    # ============================================================
    # 🔹 Distância Haversine (km)
    # ============================================================
    def haversine(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        R = 6371.0
        dlat = radians(lat2 - lat1)
        dlon = radians(lon2 - lon1)
        a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
        return 2 * R * atan2(sqrt(a), sqrt(1 - a))

    # ============================================================
    # 🧭 Ordenação por vizinho mais próximo
    # ============================================================
    def _ordenar_por_vizinho_mais_proximo(self, coords, centro):
        """
        Retorna os pontos ordenados pela heurística do vizinho mais próximo.
        """
        if not coords:
            return []

        nao_visitados = coords.copy()
        rota = [centro]
        atual = np.array(centro)

        while nao_visitados:
            distancias = [_haversine_km(atual, p) for p in nao_visitados]
            prox_idx = int(np.argmin(distancias))
            prox_ponto = nao_visitados.pop(prox_idx)
            rota.append(prox_ponto)
            atual = np.array(prox_ponto)

        rota.append(centro)
        return rota

    # ============================================================
    # 🚚 Rota simulada (vizinho mais próximo)
    # ============================================================
    def calcular_rota_simulada(
        self,
        cluster_coords: List[Tuple[float, float]],
        centro: Tuple[float, float],
        tenant_id: int = None,
        cluster_label: str = None,
        debug_visual: bool = False,
    ) -> Tuple[float, float, List[Tuple[float, float]]]:
        """
        Simula uma rota fechada (centro → PDVs → centro) usando heurística de vizinho mais próximo.
        Retorna (distância_total_km, tempo_total_min, rota_sequencia).
        """

        if not cluster_coords:
            return 0.0, 0.0, []

        # Usa heurística interna
        rota_seq = self._ordenar_por_vizinho_mais_proximo(cluster_coords, centro)
        dist_km = sum(_haversine_km(rota_seq[i], rota_seq[i + 1]) for i in range(len(rota_seq) - 1))
        tempo_min = (dist_km / max(self.v_kmh, 1e-6)) * 60.0 + len(cluster_coords) * self.tempo_servico_min

        # ⚠️ Alerta se rota parecer anômala
        if dist_km > self.max_dist_km * 5:
            logger.warning(
                f"🚨 Rota simulada anômala: {dist_km:.1f} km | {tempo_min:.1f} min "
                f"→ verifique dispersão/duplicidades."
            )

        # ============================================================
        # 🗺️ Modo debug visual
        # ============================================================
        if debug_visual:
            try:
                import folium
                from folium import PolyLine, CircleMarker
                from pathlib import Path
                import os

                lat_centro, lon_centro = centro
                mapa = folium.Map(location=[lat_centro, lon_centro], zoom_start=10)

                folium.Marker(
                    location=[lat_centro, lon_centro],
                    icon=folium.Icon(color="red", icon="home"),
                    popup=f"Centro ({lat_centro:.4f}, {lon_centro:.4f})"
                ).add_to(mapa)

                for i, (lat, lon) in enumerate(rota_seq[1:-1], start=1):
                    CircleMarker(
                        location=(lat, lon),
                        radius=4,
                        color="blue",
                        fill=True,
                        fill_opacity=0.8,
                        popup=f"PDV {i}"
                    ).add_to(mapa)

                PolyLine(rota_seq, color="green", weight=3, opacity=0.7).add_to(mapa)
                tenant_dir = f"output/route_debug/{tenant_id or 'default'}"
                Path(tenant_dir).mkdir(parents=True, exist_ok=True)
                filepath = os.path.join(tenant_dir, f"cluster_{cluster_label or 'X'}.html")
                mapa.save(filepath)
                logger.info(f"🗺️ Mapa da rota teórica salvo em: {filepath}")
            except Exception as e:
                logger.warning(f"⚠️ Falha ao gerar mapa debug: {e}")

        return round(dist_km, 2), round(tempo_min, 2), rota_seq

    # ============================================================
    # 🧭 Avaliação operacional de clusters/subclusters
    # ============================================================
    def avaliar_clusters(self, setores: List) -> List[Dict]:
        resultados = []
        for setor in setores:
            lat = getattr(setor, "centro_lat", None)
            lon = getattr(setor, "centro_lon", None)
            if lat is None or lon is None:
                continue

            coords = getattr(setor, "coords", None)
            if not coords and hasattr(setor, "pdvs") and setor.pdvs:
                coords = [(p.lat, p.lon) for p in setor.pdvs if p.lat and p.lon]
            if not coords:
                continue

            n_pdvs = len(coords)
            centro = (lat, lon)
            dist_total, tempo_total, rota_seq = self.calcular_rota_simulada(coords, centro)
            status = "EXCEDIDO" if (tempo_total > self.max_time_min or dist_total > self.max_dist_km) else "OK"

            resultados.append({
                "cluster_label": getattr(setor, "cluster_label", -1),
                "n_pdvs": n_pdvs,
                "tempo_min": tempo_total,
                "dist_km": dist_total,
                "status": status,
                "rota_sequencia": rota_seq,
            })

        excedidos = [r for r in resultados if r["status"] == "EXCEDIDO"]
        if resultados:
            tempo_med = np.mean([r["tempo_min"] for r in resultados])
            dist_med = np.mean([r["dist_km"] for r in resultados])
            logger.info(
                f"📊 Avaliação global: {len(resultados)} rotas | {len(excedidos)} excedidas | "
                f"Tempo médio={tempo_med:.1f} min | Distância média={dist_med:.1f} km"
            )

        return resultados

    # ============================================================
    # ♻️ Refinamento iterativo com reexecução (K+1)
    # ============================================================
    def refinar_com_subclusters_iterativo(
        self,
        pdvs: List,
        dias_uteis: int,
        freq: int,
        max_pdv_cluster: int,
        k_inicial_param: int = None,
    ):
        """
        Refinamento operacional sem recalcular o K.
        Agora respeita o K gerado pelo KMeans balanceado.
        """

        total_pdvs = len(pdvs)

        # -----------------------------------------------------------
        # 🟢 1. Usa o K vindo do pipeline — NÃO recalcula mais
        # -----------------------------------------------------------
        if k_inicial_param is not None:
            k_inicial = max(1, int(k_inicial_param))
        else:
            # fallback seguro
            k_inicial = max(1, math.ceil(total_pdvs / (max_pdv_cluster * max(freq, 1))))

        logger.info(f"🚀 Refinamento operacional iniciado com K={k_inicial}")

        k_atual = k_inicial
        setores_finais = []
        setores_refinados = []  # precisa existir fora do loop

        for it in range(self.max_iter):
            logger.info(f"🔁 Iteração {it+1}/{self.max_iter} — K={k_atual}")

            setores_macro, labels = kmeans_setores(pdvs, k_atual)

            # atribui labels
            for i, p in enumerate(pdvs):
                if i < len(labels):
                    p.cluster_label = int(labels[i])

            houve_excesso = False
            setores_refinados = []

            for s in setores_macro:
                pdvs_local = [p for p in pdvs if p.cluster_label == s.cluster_label]
                if not pdvs_local:
                    continue

                n_sub = max(1, int(dias_uteis / max(freq, 1)))
                n_sub = min(n_sub, len(pdvs_local))

                sub_setores, _ = kmeans_setores(pdvs_local, n_sub)
                s.subclusters = []

                for j, sub in enumerate(sub_setores):
                    coords_sub = [(pp.lat, pp.lon) for pp in sub.pdvs if pp.lat and pp.lon]
                    if not coords_sub:
                        continue

                    dist_km, tempo_min, rota = self.calcular_rota_simulada(
                        coords_sub, (sub.centro_lat, sub.centro_lon)
                    )

                    excedeu = tempo_min > self.max_time_min or dist_km > self.max_dist_km

                    s.subclusters.append({
                        "seq": j + 1,
                        "centro_lat": sub.centro_lat,
                        "centro_lon": sub.centro_lon,
                        "n_pdvs": len(coords_sub),
                        "dist_km": dist_km,
                        "tempo_min": tempo_min,
                        "status": "EXCEDIDO" if excedeu else "OK",
                        "rota_sequencia": rota,
                    })

                    if excedeu:
                        houve_excesso = True

                setores_refinados.append(s)

            # -----------------------------------------------------------
            # 🟢 Se excedeu → aumenta o K
            # -----------------------------------------------------------
            if houve_excesso:
                k_atual += 1
                logger.warning(f"⚠️ Excedeu limites — aumentando K para {k_atual}")
                continue

            # -----------------------------------------------------------
            # 🟢 Caso contrário → solução encontrada
            # -----------------------------------------------------------
            logger.success(f"✅ Subclusters OK com K={k_atual}")
            setores_finais = setores_refinados
            break

        # ============================================================
        # 🔒 SEGURANÇA — fallback
        # ============================================================
        if not setores_finais:
            setores_finais = setores_refinados

        # ============================================================
        # 🔑 NORMALIZAÇÃO DOS LABELS
        # ============================================================
        labels_originais = sorted(s.cluster_label for s in setores_finais)
        mapa_labels = {old: new for new, old in enumerate(labels_originais)}

        for s in setores_finais:
            old = s.cluster_label
            new = mapa_labels[old]
            s.cluster_label = new

            if hasattr(s, "pdvs") and s.pdvs:
                for p in s.pdvs:
                    p.cluster_label = new

        logger.info(f"🔒 Labels normalizados: {mapa_labels}")

        return setores_finais



    # ============================================================
    # 🧭 Geração e refinamento de subrotas teóricas (sequência otimizada)
    # ============================================================
    def gerar_subrotas_teoricas(
        self,
        pdvs: List,
        setores_macro: List,
        dias_uteis: int,
        freq: int,
        max_pdv_cluster: int,
    ):
        n_sub_planejado = max(1, int(dias_uteis / max(freq, 1)))
        logger.info(f"🧭 Gerando rotas teóricas (n_sub={n_sub_planejado})")

        setores_resultantes = []
        for s in setores_macro:
            pdvs_cluster = getattr(s, "pdvs", [])
            if not pdvs_cluster:
                continue

            logger.info(f"📍 Cluster {s.cluster_label}: {len(pdvs_cluster)} PDVs")
            n_sub_seguro = min(n_sub_planejado, len(pdvs_cluster))
            sub_setores, _ = kmeans_setores(pdvs_cluster, n_sub_seguro)
            s.subclusters = []
            houve_excesso = False

            for j, sub in enumerate(sub_setores):
                coords_sub = [(p.lat, p.lon) for p in sub.pdvs if p.lat and p.lon]
                if not coords_sub:
                    continue

                dist_km, tempo_min, rota_seq = self.calcular_rota_simulada(coords_sub, (sub.centro_lat, sub.centro_lon))
                status = "EXCEDIDO" if tempo_min > self.max_time_min or dist_km > self.max_dist_km else "OK"

                s.subclusters.append({
                    "seq": j + 1,
                    "centro_lat": sub.centro_lat,
                    "centro_lon": sub.centro_lon,
                    "n_pdvs": len(coords_sub),
                    "dist_km": round(dist_km, 2),
                    "tempo_min": round(tempo_min, 2),
                    "status": status,
                    "rota_sequencia": rota_seq,
                })

                if status == "EXCEDIDO":
                    houve_excesso = True
                    logger.warning(
                        f"⚠️ Rota {j+1}/{n_sub_seguro} do cluster {s.cluster_label} excede "
                        f"({dist_km:.1f} km / {tempo_min:.1f} min)"
                    )

            if houve_excesso:
                logger.warning(f"⚠️ Cluster {s.cluster_label} excedeu limites — reclusterizando...")
                s_reclust = self.reclusterizar_recursivo(
                    pdvs_cluster, max_pdv_cluster, dias_uteis, freq, 2, str(s.cluster_label)
                )
                setores_resultantes.extend(s_reclust or [])
            else:
                setores_resultantes.append(s)

        # 🌍 Mapa global consolidado
        try:
            run_id = getattr(self, "run_id", 0)
            self.gerar_mapa_global(setores_resultantes, tenant_id=self.tenant_id or 0, run_id=run_id)
        except Exception as e:
            logger.warning(f"⚠️ Falha ao gerar mapa global consolidado: {e}")

        return setores_resultantes

    # ============================================================
    # 🌎 Mapa global consolidado
    # ============================================================
    def gerar_mapa_global(self, setores_resultantes: List, tenant_id: int, run_id: int):
        import folium
        from pathlib import Path
        from folium.plugins import MarkerCluster

        mapa = folium.Map(location=[-15.78, -47.93], zoom_start=5, tiles="cartodbpositron")
        marker_cluster = MarkerCluster(name="Centros").add_to(mapa)

        for s in setores_resultantes:
            color = "#{:06x}".format(abs(hash(str(s.cluster_label))) % 0xFFFFFF)
            folium.CircleMarker(
                location=[s.centro_lat, s.centro_lon],
                radius=6,
                color=color,
                fill=True,
                fill_color=color,
                fill_opacity=0.8,
                popup=f"Cluster {s.cluster_label} | {len(getattr(s, 'pdvs', []))} PDVs",
            ).add_to(marker_cluster)

            if hasattr(s, "subclusters") and s.subclusters:
                for sc in s.subclusters:
                    rota = sc.get("rota_sequencia")
                    if rota and len(rota) > 1:
                        folium.PolyLine(rota, color=color, weight=2, opacity=0.6).add_to(mapa)

        outdir = Path(f"output/diagnostics/{tenant_id}")
        outdir.mkdir(parents=True, exist_ok=True)
        outfile = outdir / f"clusterizacao_{run_id}.html"
        mapa.save(outfile)
        logger.info(f"🌍 Mapa global consolidado salvo em: {outfile}")

    # ============================================================
    # ♻️ Reclustering recursivo
    # ============================================================
    def reclusterizar_recursivo(
        self,
        pdvs_cluster: List,
        max_pdv_cluster: int,
        dias_uteis: int,
        freq: int,
        fator_div: int = 2,
        base_label: str = "",
    ):
        logger.info(f"🔁 Reclusterizando cluster '{base_label}' em {fator_div} partes...")

        if len(pdvs_cluster) <= max(4, max_pdv_cluster):
            logger.debug(f"   ⚙️ Cluster {base_label} pequeno demais para subdivisão ({len(pdvs_cluster)} PDVs)")
            return []

        sub_setores, sub_labels = kmeans_setores(pdvs_cluster, fator_div)
        setores_resultantes = []

        for i, s in enumerate(sub_setores):
            label_hierarquico = f"{base_label}.{i+1}" if base_label else f"A{i+1}"
            pdvs_local = [p for p in pdvs_cluster if sub_labels[pdvs_cluster.index(p)] == i]
            coords_local = [(p.lat, p.lon) for p in pdvs_local if p.lat and p.lon]

            if not coords_local:
                logger.warning(f"   ⚠️ Subcluster {label_hierarquico} vazio ou inválido.")
                continue

            dist_km, tempo_min, _ = self.calcular_rota_simulada(coords_local, (s.centro_lat, s.centro_lon))
            status = "EXCEDIDO" if tempo_min > self.max_time_min or dist_km > self.max_dist_km else "OK"

            logger.info(
                f"   ↪️ Subcluster {label_hierarquico}: {len(coords_local)} PDVs | "
                f"{dist_km:.1f} km | {tempo_min:.1f} min | {status}"
            )

            if status == "EXCEDIDO":
                setores_resultantes.extend(
                    self.reclusterizar_recursivo(
                        pdvs_local,
                        max_pdv_cluster,
                        dias_uteis,
                        freq,
                        fator_div + 1,
                        base_label=label_hierarquico,
                    )
                )
            else:
                s.metrics = {"dist_km": dist_km, "tempo_min": tempo_min, "status": status}
                setores_resultantes.append(s)

        return setores_resultantes
