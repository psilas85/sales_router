# src/sales_routing/application/cvrptw_subcluster_splitter.py
#
# Gerador de subclusters via OR-Tools CVRPTW (Capacitated VRP com janelas
# de atendimento), adaptado do desenho do HubRouter:
#   - 1 "veículo" = 1 vendedor (1 rota diária)
#   - depot = centroide do cluster (não é PDV)
#   - capacidade de entregas = max_pdvs_rota (sem peso/volume)
#   - dimensão Time com janelas (operação ou janelas individuais)
#   - dimensão extra "Strategic" pra limitar PDVs estratégicos por rota
#
# Convive com fixed/balanced/adaptativo. Aciona-se quando
# `algoritmo_roteirizacao == "time_windows"`.

import math
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger
from ortools.constraint_solver import pywrapcp, routing_enums_pb2

from src.sales_routing.application.route_distance_service import RouteDistanceService
from src.sales_routing.domain.entities.cluster_data_entity import ClusterData, PDVData


def gerar_subclusters_cvrptw(
    clusters: List[ClusterData],
    pdvs: List[PDVData],
    dias_uteis: int,
    freq_padrao: int,
    v_kmh: float,
    service_min: float,
    alpha_path: float,
    min_pdvs_rota: int,
    max_pdvs_rota: int,
    horario_inicio_operacao: int,
    horario_fim_operacao: int,
    usar_janelas_pdv: bool,
    modo_estrito_janelas: bool,
    janela_padrao_pdv_inicio: int,
    janela_padrao_pdv_fim: int,
    max_estrategicos_por_rota: int,
    tempo_atendimento_especial_min: float,
    permitir_rotas_excedentes: bool = True,
    # Tempo máximo por rota (minutos). 0 = sem limite extra (usa só a
    # janela operacional). Quando > 0, vira o cap por veículo na
    # dimensão Time do solver — não importa se a janela operacional
    # comporta mais, cada rota fica limitada a esse tempo.
    tempo_max_rota_min: int = 0,
) -> List[Dict[str, Any]]:
    """Roteiriza cada cluster via CVRPTW (OR-Tools). Retorna mesmo formato
    dos demais splitters (fixo/balanceado/adaptativo) para a persistência
    permanecer agnóstica ao modo."""

    if horario_fim_operacao <= horario_inicio_operacao:
        raise ValueError(
            f"horario_fim_operacao ({horario_fim_operacao}) deve ser maior "
            f"que horario_inicio_operacao ({horario_inicio_operacao})"
        )

    distance_service = RouteDistanceService(v_kmh=v_kmh, alpha_path=alpha_path)

    resultados: List[Dict[str, Any]] = []
    try:
        for cluster in clusters:
            pdvs_cluster = [p for p in pdvs if p.cluster_id == cluster.cluster_id]
            if not pdvs_cluster:
                continue

            logger.info(
                f"🧠 CVRPTW cluster {cluster.cluster_id} "
                f"({len(pdvs_cluster)} PDVs)"
            )
            try:
                resultado = _resolver_cluster_cvrptw(
                    cluster=cluster,
                    pdvs_cluster=pdvs_cluster,
                    distance_service=distance_service,
                    v_kmh=v_kmh,
                    service_min_global=service_min,
                    min_pdvs_rota=min_pdvs_rota,
                    max_pdvs_rota=max_pdvs_rota,
                    horario_inicio_operacao=horario_inicio_operacao,
                    horario_fim_operacao=horario_fim_operacao,
                    usar_janelas_pdv=usar_janelas_pdv,
                    modo_estrito_janelas=modo_estrito_janelas,
                    janela_padrao_pdv_inicio=janela_padrao_pdv_inicio,
                    janela_padrao_pdv_fim=janela_padrao_pdv_fim,
                    max_estrategicos_por_rota=max_estrategicos_por_rota,
                    tempo_atendimento_especial_min=tempo_atendimento_especial_min,
                    permitir_rotas_excedentes=permitir_rotas_excedentes,
                    tempo_max_rota_min=tempo_max_rota_min,
                )
                resultados.append(resultado)
            except Exception as e:
                logger.error(
                    f"❌ CVRPTW falhou no cluster {cluster.cluster_id}: {e}"
                )
                # Propaga: a falha do CVRPTW não deve mascarar problemas
                # de dados (PDV sem janela em modo estrito, por exemplo).
                raise
    finally:
        distance_service.close()

    logger.success(f"🏁 CVRPTW concluído ({len(resultados)} clusters)")
    return resultados


def _service_time_de(pdv: PDVData, service_min_global: float) -> int:
    """Tempo de atendimento (min, int) por PDV. Prioriza valor do cadastro."""
    val = pdv.tempo_atendimento_min
    if val is None or val <= 0:
        val = service_min_global
    return max(1, int(round(val)))


def _is_estrategico(
    pdv: PDVData,
    tempo_atendimento_especial_min: float,
) -> bool:
    """PDV é estratégico se marcado no cadastro OU se tem tempo de
    atendimento >= limiar configurado."""
    if pdv.is_estrategico:
        return True
    if (
        pdv.tempo_atendimento_min is not None
        and pdv.tempo_atendimento_min >= tempo_atendimento_especial_min
    ):
        return True
    return False


def _janela_pdv(
    pdv: PDVData,
    service_time: int,
    horario_inicio_operacao: int,
    horario_fim_operacao: int,
    usar_janelas_pdv: bool,
    modo_estrito_janelas: bool,
    janela_padrao_pdv_inicio: int,
    janela_padrao_pdv_fim: int,
) -> Tuple[int, int]:
    """Retorna (tw_lower, tw_upper) RELATIVOS ao horario_inicio_operacao,
    em minutos. tw_upper já desconta o service_time pra garantir que a
    parada termine dentro da janela.

    Fallback (quando usar_janelas_pdv e PDV sem janela individual):
      - modo_estrito_janelas=True  → ValueError
      - modo_estrito_janelas=False → janela_padrao_pdv_inicio/fim

    Sem usar_janelas_pdv: todos os PDVs caem no horario_inicio/fim_operacao.
    """

    janela = horario_fim_operacao - horario_inicio_operacao
    janela = max(janela, 1)

    if usar_janelas_pdv and (
        pdv.janela_atendimento_inicio is not None
        and pdv.janela_atendimento_fim is not None
    ):
        jan_ini = int(pdv.janela_atendimento_inicio)
        jan_fim = int(pdv.janela_atendimento_fim)
    elif usar_janelas_pdv:
        if modo_estrito_janelas:
            raise ValueError(
                f"PDV {pdv.pdv_id} sem janela_atendimento_inicio/fim e "
                f"modo_estrito_janelas=True"
            )
        # Fallback: janela padrão configurada nos parâmetros do tenant
        jan_ini = int(janela_padrao_pdv_inicio)
        jan_fim = int(janela_padrao_pdv_fim)
    else:
        # Sem usar janelas individuais: respeitar só o horário de operação
        jan_ini = int(horario_inicio_operacao)
        jan_fim = int(horario_fim_operacao)

    tw_lower = max(0, jan_ini - horario_inicio_operacao)
    tw_upper = max(tw_lower, jan_fim - horario_inicio_operacao - service_time)
    # Clamp ao horizonte da operação
    tw_lower = min(tw_lower, janela)
    tw_upper = min(tw_upper, janela)
    return tw_lower, tw_upper


def _estimar_num_veiculos(
    n_pdvs: int,
    n_estrategicos: int,
    max_pdvs_rota: int,
    max_estrategicos_por_rota: int,
) -> int:
    """Mínimo de veículos pra acomodar capacidades. Os dois constraints
    (entregas/rota e estratégicos/rota) precisam caber simultaneamente."""
    por_pdvs = math.ceil(n_pdvs / max(max_pdvs_rota, 1))
    por_esp = (
        math.ceil(n_estrategicos / max(max_estrategicos_por_rota, 1))
        if max_estrategicos_por_rota > 0
        else 0
    )
    return max(1, por_pdvs, por_esp)


def _resolver_cluster_cvrptw(
    cluster: ClusterData,
    pdvs_cluster: List[PDVData],
    distance_service: RouteDistanceService,
    v_kmh: float,
    service_min_global: float,
    min_pdvs_rota: int,
    max_pdvs_rota: int,
    horario_inicio_operacao: int,
    horario_fim_operacao: int,
    usar_janelas_pdv: bool,
    modo_estrito_janelas: bool,
    janela_padrao_pdv_inicio: int,
    janela_padrao_pdv_fim: int,
    max_estrategicos_por_rota: int,
    tempo_atendimento_especial_min: float,
    permitir_rotas_excedentes: bool = True,
    tempo_max_rota_min: int = 0,
) -> Dict[str, Any]:
    n_pdvs = len(pdvs_cluster)

    # --- Coords: depot (idx 0) + PDVs (idx 1..N) ---
    coords: List[Tuple[float, float]] = [
        (float(cluster.centro_lat), float(cluster.centro_lon))
    ]
    coords.extend((float(p.lat), float(p.lon)) for p in pdvs_cluster)
    n_nodes = len(coords)

    # --- Service time por nó (depot=0) ---
    service_times = [0] + [
        _service_time_de(p, service_min_global) for p in pdvs_cluster
    ]

    # --- Janelas por nó (depot janela inteira da operação) ---
    janela_total = max(1, horario_fim_operacao - horario_inicio_operacao)
    time_windows: List[Tuple[int, int]] = [(0, janela_total)]
    for i, p in enumerate(pdvs_cluster):
        tw = _janela_pdv(
            pdv=p,
            service_time=service_times[i + 1],
            horario_inicio_operacao=horario_inicio_operacao,
            horario_fim_operacao=horario_fim_operacao,
            usar_janelas_pdv=usar_janelas_pdv,
            modo_estrito_janelas=modo_estrito_janelas,
            janela_padrao_pdv_inicio=janela_padrao_pdv_inicio,
            janela_padrao_pdv_fim=janela_padrao_pdv_fim,
        )
        time_windows.append(tw)

    # --- Estratégicos ---
    estrategicos_flags = [0] + [
        1 if _is_estrategico(p, tempo_atendimento_especial_min) else 0
        for p in pdvs_cluster
    ]
    n_estrategicos = sum(estrategicos_flags)

    # --- Matriz de tempo (NxN, em minutos inteiros) ---
    dist_km_mat, time_min_mat, fonte_matriz = distance_service.get_time_matrix(coords)
    time_matrix: List[List[int]] = [
        [max(0, int(round(time_min_mat[i][j]))) for j in range(n_nodes)]
        for i in range(n_nodes)
    ]

    # --- Veículos ---
    num_vehicles = _estimar_num_veiculos(
        n_pdvs=n_pdvs,
        n_estrategicos=n_estrategicos,
        max_pdvs_rota=max_pdvs_rota,
        max_estrategicos_por_rota=max_estrategicos_por_rota,
    )

    # ---------- Build + solve do modelo OR-Tools (padrão HubRouter) ----------
    # Limite efetivo POR ROTA (capacity da dimensão Time de cada veículo).
    # Quando tempo_max_rota_min > 0, força esse cap. Senão, usa a janela
    # operacional inteira. Sempre limitado ao máximo da janela — não
    # adianta permitir mais que o expediente.
    if tempo_max_rota_min and tempo_max_rota_min > 0:
        limite_rota = min(int(tempo_max_rota_min), janela_total)
    else:
        limite_rota = janela_total

    # Penalty de drop escala com o limite — HubRouter usa limite × 200.
    # Quanto maior o limite, mais o solver resiste a dropar (já que
    # gerar mais rotas saiu mais barato em "tempo").
    PENALTY_NORMAL = max(1_000_000, int(limite_rota * 200))
    PENALTY_ESTRATEGICO = PENALTY_NORMAL * 100

    def _build_and_solve(n_veiculos: int, relaxado: bool = False):
        """Quando relaxado=True (3ª passagem padrão HubRouter):
          - janelas de cada nó abertas em [0, horizonte]
          - capacidade tempo por veículo = 2× janela_total (cada rota
            pode extrapolar até o dobro do dia, evita virar 1 rota
            gigantesca de 24h)
          - disjunctions com penalty 10× (resistência extra a dropar)
          - sem dimensão Strategic (relax total)
        """
        manager = pywrapcp.RoutingIndexManager(n_nodes, n_veiculos, 0)
        routing = pywrapcp.RoutingModel(manager)

        # Horizonte do veículo (capacidade da dimensão Time):
        #   - normal: limite_rota (= min(tempo_max_rota_min, janela_total))
        #   - relaxado: 2× janela_total (absorve PDVs distantes sem
        #     virar rota de 24h)
        horizonte = janela_total * 2 if relaxado else limite_rota

        def transit_cb(from_index, to_index):
            f = manager.IndexToNode(from_index)
            t = manager.IndexToNode(to_index)
            return time_matrix[f][t]

        transit_cb_idx = routing.RegisterTransitCallback(transit_cb)
        routing.SetArcCostEvaluatorOfAllVehicles(transit_cb_idx)

        def time_cb(from_index, to_index):
            f = manager.IndexToNode(from_index)
            t = manager.IndexToNode(to_index)
            return time_matrix[f][t] + service_times[f]

        time_cb_idx = routing.RegisterTransitCallback(time_cb)
        routing.AddDimension(
            time_cb_idx,
            horizonte, horizonte, False, "Time",
        )
        time_dim = routing.GetDimensionOrDie("Time")
        if relaxado:
            # Sem restrição de janela — qualquer instante no horizonte
            # estendido vale.
            for node in range(n_nodes):
                index = manager.NodeToIndex(node)
                time_dim.CumulVar(index).SetRange(0, horizonte)
        else:
            # Capa tw_upper no horizonte (importante quando
            # tempo_max_rota_min < janela_total — sem isso o solver
            # pode rejeitar todo o problema por inconsistência entre
            # janela do nó e capacity do veículo).
            for node, (a, b) in enumerate(time_windows):
                index = manager.NodeToIndex(node)
                a_cap = max(0, min(int(a), horizonte))
                b_cap = max(a_cap, min(int(b), horizonte))
                time_dim.CumulVar(index).SetRange(a_cap, b_cap)
        for v in range(n_veiculos):
            time_dim.CumulVar(routing.Start(v)).SetRange(0, horizonte)
            time_dim.CumulVar(routing.End(v)).SetRange(0, horizonte)

        def delivery_cb(from_index):
            n = manager.IndexToNode(from_index)
            return 0 if n == 0 else 1

        del_cb_idx = routing.RegisterUnaryTransitCallback(delivery_cb)
        routing.AddDimensionWithVehicleCapacity(
            del_cb_idx, 0, [max_pdvs_rota] * n_veiculos, True, "Deliveries",
        )

        # Dimensão Strategic — também aplicada na passagem relaxada.
        # Antes era pulada no relax, mas isso permitia o solver
        # concentrar vários PDVs estratégicos numa única rota
        # (violando max_estrategicos_por_rota). Mantemos a capacidade
        # sempre que houver estratégicos configurados.
        if n_estrategicos > 0 and max_estrategicos_por_rota > 0:
            def strategic_cb(from_index):
                n = manager.IndexToNode(from_index)
                return estrategicos_flags[n]
            str_cb_idx = routing.RegisterUnaryTransitCallback(strategic_cb)
            routing.AddDimensionWithVehicleCapacity(
                str_cb_idx, 0,
                [max_estrategicos_por_rota] * n_veiculos, True, "Strategic",
            )

        # Disjunctions: sempre presentes (caso contrário o solver junta
        # todos os nós em 1 rota gigante quando o atendimento é obrigatório).
        # Na relaxada, penalty 10× maior pra ser quase impossível dropar.
        penalty_normal = PENALTY_NORMAL * (10 if relaxado else 1)
        penalty_estrat = PENALTY_ESTRATEGICO * (10 if relaxado else 1)
        for node_idx in range(1, n_nodes):
            penalty = (
                penalty_estrat
                if estrategicos_flags[node_idx]
                else penalty_normal
            )
            routing.AddDisjunction(
                [manager.NodeToIndex(node_idx)], penalty
            )

        search_params = pywrapcp.DefaultRoutingSearchParameters()
        search_params.first_solution_strategy = (
            routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
        )
        search_params.local_search_metaheuristic = (
            routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
        )
        search_params.time_limit.seconds = max(5, min(30, n_pdvs // 3))

        sol = routing.SolveWithParameters(search_params)
        return manager, routing, sol

    def _drops_de(manager, routing, sol) -> List[int]:
        """Nós (1..N) que o solver decidiu não atender."""
        if not sol:
            return list(range(1, n_nodes))
        atendidos: set[int] = set()
        for v in range(routing.vehicles()):
            idx = routing.Start(v)
            while not routing.IsEnd(idx):
                n = manager.IndexToNode(idx)
                if n != 0:
                    atendidos.add(n)
                idx = sol.Value(routing.NextVar(idx))
        return [n for n in range(1, n_nodes) if n not in atendidos]

    # Tentativa #1
    manager, routing, solution = _build_and_solve(num_vehicles)
    if not solution:
        raise RuntimeError(
            f"CVRPTW cluster {cluster.cluster_id}: solver não retornou "
            f"solução mesmo com disjunctions (n_pdvs={n_pdvs}, "
            f"n_veic={num_vehicles}). Investigar callbacks/dimensões."
        )

    drops = _drops_de(manager, routing, solution)
    veiculos_usados = num_vehicles

    # Retry HubRouter: se houver drops, tenta com +len(drops) veículos
    # (no max 1×). Geralmente reabsorve tudo.
    if drops:
        retry_veic = min(n_pdvs, num_vehicles + len(drops))
        if retry_veic > num_vehicles:
            logger.info(
                f"🔁 CVRPTW cluster {cluster.cluster_id}: {len(drops)} PDVs "
                f"droppados com {num_vehicles} veículos — retry com {retry_veic}"
            )
            m2, r2, s2 = _build_and_solve(retry_veic)
            if s2:
                d2 = _drops_de(m2, r2, s2)
                if len(d2) < len(drops):
                    manager, routing, solution = m2, r2, s2
                    drops = d2
                    veiculos_usados = retry_veic

    # Marca PDVs originalmente droppados — usado pra dar status_rota
    # "fallback_excedente" às rotas que vierem da passagem relaxada.
    drops_originais: set[int] = set(drops)

    # 3ª passagem (HubRouter): se ainda há drops E permitir_rotas_excedentes,
    # roda solver com janelas abertas + horizonte estendido pra capturar
    # os PDVs inviáveis. Mantém disjunctions e capacity Strategic
    # (penalty 10× — não deixa o solver concentrar estratégicos numa
    # rota só pra economizar veículos).
    if drops and permitir_rotas_excedentes:
        # Garante capacidade pra estratégicos mesmo no relax: se há N
        # especiais com max=1/rota, precisa pelo menos N veículos.
        min_por_estrategicos = (
            n_estrategicos
            if max_estrategicos_por_rota and max_estrategicos_por_rota > 0
            else 0
        )
        retry_veic = min(
            n_pdvs,
            max(
                veiculos_usados,
                num_vehicles + len(drops),
                min_por_estrategicos,
            ),
        )
        logger.info(
            f"🔓 CVRPTW cluster {cluster.cluster_id}: {len(drops)} PDVs "
            f"ainda droppados — passagem RELAXADA com {retry_veic} veículos "
            f"(janelas abertas + horizonte estendido)"
        )
        m3, r3, s3 = _build_and_solve(retry_veic, relaxado=True)
        if s3:
            d3 = _drops_de(m3, r3, s3)
            if len(d3) < len(drops):
                manager, routing, solution = m3, r3, s3
                drops = d3
                veiculos_usados = retry_veic
                logger.info(
                    f"✅ Passagem relaxada absorveu PDVs (drops finais={len(drops)})"
                )

    if drops:
        pdv_ids_drops = [pdvs_cluster[n - 1].pdv_id for n in drops]
        logger.warning(
            f"⚠️ CVRPTW cluster {cluster.cluster_id}: {len(drops)} PDVs "
            f"não atendidos (inviáveis mesmo com janelas abertas): "
            f"{pdv_ids_drops[:10]}{'…' if len(pdv_ids_drops) > 10 else ''}"
        )

    logger.info(
        f"✅ CVRPTW cluster {cluster.cluster_id} resolvido com "
        f"{veiculos_usados} veículos | drops_originais={len(drops_originais)} | "
        f"drops_finais={len(drops)} (matriz: {fonte_matriz})"
    )

    # --- Extrair rotas resultantes ---
    subclusters: List[Dict[str, Any]] = []
    max_tempo = 0.0
    max_dist = 0.0

    subcluster_seq = 0
    for v in range(veiculos_usados):
        index = routing.Start(v)
        seq_nodes: List[int] = []
        while not routing.IsEnd(index):
            n = manager.IndexToNode(index)
            if n != 0:
                seq_nodes.append(n)
            index = solution.Value(routing.NextVar(index))
        if not seq_nodes:
            continue

        subcluster_seq += 1
        pdvs_da_rota_objs = [pdvs_cluster[n - 1] for n in seq_nodes]

        # Monta sequência (formato dos outros splitters: lista de dicts
        # com pdv_id/lat/lon).
        sequencia = [
            {"pdv_id": p.pdv_id, "lat": p.lat, "lon": p.lon}
            for p in pdvs_da_rota_objs
        ]

        # Métricas via /route multi-stop (geometria real + dist/tempo de
        # viagem completos). Cache já populado pelo /table — então
        # mesmo o fallback segmentado vai bem.
        centro = {"lat": cluster.centro_lat, "lon": cluster.centro_lon}
        coords_full = [(centro["lat"], centro["lon"])]
        coords_full.extend((p.lat, p.lon) for p in pdvs_da_rota_objs)
        coords_full.append((centro["lat"], centro["lon"]))
        try:
            full = distance_service.get_full_route(coords_full)
            dist_total_km = float(full.get("distancia_km", 0.0))
            tempo_viagem_min = float(full.get("tempo_min", 0.0))
            rota_coord = full.get("rota_coord") or []
        except Exception as e:
            logger.warning(
                f"⚠️ get_full_route falhou (cluster {cluster.cluster_id}, "
                f"rota {subcluster_seq}): {e} — usando agregado da matriz"
            )
            dist_total_km = 0.0
            tempo_viagem_min = 0.0
            prev_node = 0
            for n in seq_nodes + [0]:
                dist_total_km += dist_km_mat[prev_node][n]
                tempo_viagem_min += time_min_mat[prev_node][n]
                prev_node = n
            rota_coord = [
                {"lat": cluster.centro_lat, "lon": cluster.centro_lon}
            ] + [{"lat": p.lat, "lon": p.lon} for p in pdvs_da_rota_objs] + [
                {"lat": cluster.centro_lat, "lon": cluster.centro_lon}
            ]

        # Tempo total = viagem + soma dos service_times das paradas
        tempo_servico_total = sum(
            _service_time_de(p, service_min_global) for p in pdvs_da_rota_objs
        )
        tempo_total_min = tempo_viagem_min + tempo_servico_total

        # ---- Parciais (até o ÚLTIMO PDV, sem retornar ao centro) ----
        # Subtraímos a perna final (último PDV → depot) usando a matriz
        # OSRM /table como referência. O total vem de /route (multi-stop
        # com polyline real), então pode haver pequena divergência —
        # tolerável pra esse uso (estatística operacional).
        last_node = seq_nodes[-1]
        last_leg_dist = float(dist_km_mat[last_node][0])
        last_leg_time_via = float(time_min_mat[last_node][0])
        dist_parcial_km = max(0.0, dist_total_km - last_leg_dist)
        tempo_parcial_viagem = max(0.0, tempo_viagem_min - last_leg_time_via)
        tempo_parcial_min = tempo_parcial_viagem + tempo_servico_total

        max_tempo = max(max_tempo, tempo_total_min)
        max_dist = max(max_dist, dist_total_km)

        # status_rota: "fallback_excedente" se:
        #  - algum PDV originalmente dropado entrou aqui (via relaxada), OU
        #  - tempo total ultrapassou o limite por rota (tempo_max_rota ou janela)
        contém_excedente = any(n in drops_originais for n in seq_nodes)
        ultrapassa_limite = tempo_total_min > limite_rota
        status_rota = (
            "fallback_excedente"
            if (contém_excedente or ultrapassa_limite)
            else "viavel_sla"
        )

        # ---- Timeline de eventos (Gantt) -----------------------------
        # Reproduz a rota do veículo v com base em cumul.Time do solver.
        # Eventos por arco (atual → próximo):
        #   1. atendimento em "atual" (se atual != depot inicial)
        #   2. trânsito atual → próximo
        #   3. espera no próximo (se chegou antes da janela)
        # Todos os tempos são relativos ao horario_inicio_operacao
        # (em minutos). O frontend soma de volta pra exibir HH:MM real.
        time_dim_local = routing.GetDimensionOrDie("Time")
        timeline_eventos: List[Dict[str, Any]] = []
        try:
            idx = routing.Start(v)
            prev_node = manager.IndexToNode(idx)
            prev_cumul = solution.Value(time_dim_local.CumulVar(idx))
            while not routing.IsEnd(idx):
                next_idx = solution.Value(routing.NextVar(idx))
                cur_node = manager.IndexToNode(idx)
                nxt_node = manager.IndexToNode(next_idx)
                cur_cumul = solution.Value(time_dim_local.CumulVar(idx))
                nxt_cumul = solution.Value(time_dim_local.CumulVar(next_idx))
                svc_cur = service_times[cur_node]

                # 1) Atendimento no nó atual (depot tem service=0 → skip)
                if svc_cur > 0:
                    pdv = pdvs_cluster[cur_node - 1]
                    eh_especial = _is_estrategico(
                        pdv, tempo_atendimento_especial_min
                    )
                    timeline_eventos.append({
                        "tipo": "atendimento_especial" if eh_especial else "atendimento",
                        "inicio_min": int(cur_cumul),
                        "fim_min": int(cur_cumul + svc_cur),
                        "pdv_id": pdv.pdv_id,
                    })

                # 2) Trânsito do atual ao próximo
                travel = int(time_matrix[cur_node][nxt_node])
                inicio_transito = int(cur_cumul + svc_cur)
                fim_transito = inicio_transito + travel
                if travel > 0:
                    timeline_eventos.append({
                        "tipo": "transito",
                        "inicio_min": inicio_transito,
                        "fim_min": fim_transito,
                    })

                # 3) Espera no próximo (se chegou antes da janela)
                if nxt_cumul > fim_transito:
                    timeline_eventos.append({
                        "tipo": "espera",
                        "inicio_min": fim_transito,
                        "fim_min": int(nxt_cumul),
                    })

                idx = next_idx
        except Exception as e:
            logger.warning(
                f"⚠️ Falha ao extrair timeline (cluster {cluster.cluster_id}, "
                f"rota {subcluster_seq}): {e}"
            )
            timeline_eventos = []

        subclusters.append({
            "subcluster_id": subcluster_seq,
            "n_pdvs": len(sequencia),
            "tempo_total_min": round(tempo_total_min, 1),
            "dist_total_km": round(dist_total_km, 2),
            "tempo_parcial_min": round(tempo_parcial_min, 1),
            "dist_parcial_km": round(dist_parcial_km, 2),
            "pdvs": sequencia,
            "rota_coord": rota_coord,
            "status_rota": status_rota,
            "timeline_eventos": timeline_eventos,
            "horario_inicio_operacao": int(horario_inicio_operacao),
        })

    logger.success(
        f"🏁 Cluster {cluster.cluster_id}: {len(subclusters)} rotas | "
        f"tempo_max={max_tempo:.1f} min | dist_max={max_dist:.1f} km"
    )

    return {
        "cluster_id": cluster.cluster_id,
        "k_final": len(subclusters),
        "total_pdvs": n_pdvs,
        "max_tempo": round(max_tempo, 1),
        "max_dist": round(max_dist, 1),
        "iteracoes": [(veiculos_usados, round(max_tempo, 1), round(max_dist, 1))],
        "subclusters": subclusters,
        # Drops finais (PDVs que não cabem nem na passagem relaxada)
        "drops_pdv_ids": [pdvs_cluster[n - 1].pdv_id for n in drops],
        "n_drops": len(drops),
        "n_drops_originais": len(drops_originais),
    }
