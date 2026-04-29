# Integracao do `sales_routing` com `routing_engine` no modo balanceado

## Objetivo

Permitir que o `sales_routing` continue sendo o orquestrador oficial da roteirizacao, mas delegue ao `routing_engine` a execucao do modo `balanceado`, sem regressao na qualidade das rotas e sem perder controle de validacao, historico, persistencia e status do pipeline.

Escopo desta integracao:

- `sales_routing` continua validando request, resolvendo `run_id`, carregando clusters e PDVs, controlando status e persistindo resultado final.
- `routing_engine` passa a expor um fluxo interno especifico para receber grupos preparados e executar o balanceado via master job + subjobs.
- `fixo` e `adaptativo` permanecem locais no `sales_routing`.

Fora de escopo nesta etapa:

- migrar o fluxo batch de planilha do `routing_engine`
- substituir persistencia do `sales_routing`
- unificar todos os modos de roteirizacao em um unico motor

## Arquitetura alvo

### Responsabilidades

`sales_routing`

- valida request e token
- determina `tenant_id`, `routing_id` e `job_id`
- resolve `clusterization_id -> run_id`
- carrega clusters e PDVs
- decide o ramo por `modo`
- para `balanceado`, monta payload de grupos e chama o `routing_engine`
- acompanha execucao do engine
- persiste resultado final
- registra historico e status do pipeline principal

`routing_engine`

- recebe grupos ja preparados
- cria um master job proprio
- dispara subjobs por grupo ou cluster
- executa o algoritmo balanceado
- consolida resultados, metricas e falhas
- devolve resultado estruturado ao `sales_routing`

### Fluxo alvo

1. Request entra em `sales_routing`.
2. `sales_routing` valida tudo e cria `routing_id`.
3. O master job do `sales_routing` resolve os dados de entrada.
4. Se `modo != balanceado`, segue o fluxo atual local.
5. Se `modo == balanceado`, o `sales_routing` monta payload de grupos preparados e chama endpoint interno do `routing_engine`.
6. O endpoint interno do `routing_engine` cria um master job proprio.
7. O master job do `routing_engine` dispara subjobs por grupo.
8. Cada subjob calcula o balanceado do grupo e devolve resultado estruturado.
9. O master job do `routing_engine` agrega `results`, `failures` e `metrics`.
10. O `sales_routing` consulta o status do job do engine ate o termino.
11. Ao finalizar, o `sales_routing` persiste o resultado final e fecha seu proprio job.

## Contrato tecnico entre os modulos

## Endpoint interno sugerido

- `POST /api/v1/internal/prepared-groups/balanced-routing`
- `GET /api/v1/internal/prepared-groups/jobs/{job_id}`

## Payload de entrada

```json
{
  "tenant_id": 123,
  "routing_id": "uuid",
  "parent_job_id": "routing-master-uuid",
  "requested_by": "user@email.com",
  "mode": "balanceado",
  "params": {
    "modo_calculo": "frequencia",
    "dias_uteis": 21,
    "frequencia_visita": 2,
    "min_pdvs_rota": 8,
    "max_pdvs_rota": 12,
    "service_min": 30.0,
    "v_kmh": 35.0,
    "alpha_path": 1.3,
    "twoopt": false,
    "preserve_sequence": true
  },
  "groups": [
    {
      "group_id": "101",
      "group_type": "cluster",
      "cluster_id": 101,
      "run_id": 999,
      "centro_lat": -20.31,
      "centro_lon": -40.29,
      "pdvs": [
        {
          "pdv_id": 1,
          "lat": -20.30,
          "lon": -40.28,
          "cidade": "Vitoria",
          "uf": "ES",
          "freq_visita": 2
        }
      ]
    }
  ]
}
```

## Regras do contrato

- `tenant_id` obrigatorio e sem default.
- `routing_id` obrigatorio para rastreabilidade cruzada entre os modulos.
- `group_id` e `cluster_id` obrigatorios para o fluxo vindo do `sales_routing`.
- `group_type` deve aceitar pelo menos `cluster` e `consultor`.
- `centro_lat` e `centro_lon` obrigatorios.
- `preserve_sequence` deve ser `true` para o fluxo do `sales_routing`.
- `modo_calculo` deve ser suportado pelo `routing_engine` com os mesmos valores do `sales_routing`:
  - `frequencia`
  - `proporcional`
  - `capacidade`

## Resposta imediata do POST

```json
{
  "status": "queued",
  "engine_job_id": "uuid",
  "routing_id": "uuid"
}
```

## Resposta de status

### Em execucao

```json
{
  "job_id": "uuid",
  "status": "running",
  "progress": 62,
  "step": "Processando grupos (ok=14 falha=1 total=20)"
}
```

### Finalizado

```json
{
  "job_id": "uuid",
  "status": "done_with_warnings",
  "progress": 100,
  "result": {
    "routing_id": "uuid",
    "tenant_id": 123,
    "metrics": {
      "groups_received": 20,
      "groups_processed": 19,
      "groups_failed": 1,
      "cache_hits": 1200,
      "osrm_hits": 430,
      "google_hits": 0,
      "haversine_hits": 12
    },
    "results": [],
    "failures": []
  }
}
```

## Semantica de status recomendada

- `queued`: job enfileirado
- `running`: job em execucao
- `done`: todos os grupos concluidos sem falha
- `done_with_warnings`: todos os grupos concluiram, mas houve fallback ou degradacao relevante
- `partial_failed`: parte dos grupos falhou
- `failed`: falha estrutural do job inteiro

## Melhorias obrigatorias no motor de roteirizacao

Estas melhorias devem entrar antes de ativar a delegacao do modo balanceado.

### 1. Suportar `modo_calculo` no balanceado do `routing_engine`

Motivo:

- o `sales_routing` ja suporta `frequencia`, `proporcional` e `capacidade`
- o `routing_engine` hoje esta essencialmente fixado na logica equivalente a `frequencia`

Necessario em:

- `src/routing_engine/application/balanced_subcluster_splitter.py`

Resultado esperado:

- o calculo de `k_inicial` no engine fica compativel com o `sales_routing`

### 2. Preservar a sequencia calculada pelo optimizer

Motivo:

- `OSRM trip` pode reordenar waypoints
- no fluxo integrado, a sequencia definida pelo algoritmo balanceado deve ser preservada

Necessario em:

- `src/routing_engine/application/route_distance_service.py`
- `src/routing_engine/application/route_optimizer.py`

Resultado esperado:

- adicionar `preserve_sequence`
- quando `preserve_sequence == true`, usar rota multi-stop que respeite a ordem ja calculada
- `OSRM trip` permanece opcional para outros fluxos

### 3. Manter a robustez atual de fallback sem mascarar degradacao

Motivo:

- o engine e mais defensivo que o `sales_routing`
- essa robustez deve ser mantida, mas exposta ao chamador

Necessario em:

- `src/routing_engine/application/route_distance_service.py`
- `src/routing_engine/application/route_optimizer.py`

Resultado esperado:

- cada subcluster deve informar:
  - `route_source`
  - `sequence_source`
  - `used_twoopt`
  - `fallback_level`

### 4. Eliminar defaults silenciosos de tenant

Motivo:

- o fluxo integrado nao pode depender de `tenant_id = 1`

Necessario em:

- `src/routing_engine/application/route_spreadsheet_use_case.py`
- `src/routing_engine/routing_task_parallel.py`
- qualquer novo endpoint ou job criado

Resultado esperado:

- `tenant_id` obrigatorio no fluxo integrado
- falha imediata se estiver ausente

### 5. Tornar o motor independente de consultor

Motivo:

- no `routing_engine` atual, consultor funciona como identificador do grupo
- no `sales_routing`, esse papel sera do cluster

Necessario em:

- `src/routing_engine/application/route_spreadsheet_use_case.py`
- `src/routing_engine/routing_task_parallel.py`
- `src/routing_engine/application/consultor_service.py`
- `src/routing_engine/domain/entities.py`

Resultado esperado:

- `consultor` vira apenas uma forma de montar `RouteGroup` no fluxo batch
- o nucleo de execucao opera sobre grupos genericos

## Refatoracao proposta por etapa

### Etapa 1. Extrair nucleo puro de grupos preparados no `routing_engine`

Objetivo:

- separar calculo de rotas do fluxo de planilha

Arquivos principais:

- `src/routing_engine/application/route_spreadsheet_use_case.py`
- `src/routing_engine/routing_task_parallel.py`
- `src/routing_engine/domain/entities.py`

Entregas:

- novo use case interno para grupos preparados
- batch de planilha passa a montar grupos e chamar esse nucleo

### Etapa 2. Melhorar o motor do `routing_engine`

Objetivo:

- evitar regressao do balanceado

Arquivos principais:

- `src/routing_engine/application/balanced_subcluster_splitter.py`
- `src/routing_engine/application/route_optimizer.py`
- `src/routing_engine/application/route_distance_service.py`

Entregas:

- suporte a `modo_calculo`
- suporte a `preserve_sequence`
- metadata operacional por subcluster

### Etapa 3. Criar endpoint interno + master job + subjobs no `routing_engine`

Objetivo:

- receber grupos do `sales_routing`
- disparar job master e subjobs por grupo

Arquivos principais:

- `src/routing_engine/api/routes.py`
- `src/routing_engine/routing_task_parallel.py`
- `src/routing_engine/infrastructure/queue_factory.py`

Entregas:

- endpoint interno de start
- endpoint interno de status
- master job especifico
- subjob por grupo

### Etapa 4. Integrar o `sales_routing` ao `routing_engine` apenas no modo balanceado

Objetivo:

- manter `fixo` e `adaptativo` locais

Arquivos principais:

- `src/jobs/tasks/routing_task_parallel.py`
- `src/sales_routing/api/routes.py`
- `src/sales_routing/infrastructure/database_reader.py`
- `src/sales_routing/infrastructure/database_writer.py`

Entregas:

- branch condicional por `modo`
- adapter de chamada ao `routing_engine`
- polling interno do job do engine
- persistencia final no `sales_routing`

### Etapa 5. Validacao A/B antes de ativar como caminho oficial

Objetivo:

- comprovar que nao houve regressao

Arquivos principais:

- `src/routing_engine/tests/test_routing.py`
- testes equivalentes do `sales_routing`, se existirem

Entregas:

- cenarios de comparacao entre balanceado local do `sales_routing` e balanceado delegado ao `routing_engine`
- comparacao de:
  - numero de subclusters
  - distribuicao de PDVs
  - distancia total
  - tempo total
  - falhas parciais
  - nivel de fallback

## Backlog tecnico por arquivo

### `sales_routing`

`src/jobs/tasks/routing_task_parallel.py`

- manter a decisao de negocio por `modo`
- para `balanceado`, montar payload de grupos preparados
- chamar o endpoint interno do `routing_engine`
- acompanhar status do job do engine
- transformar retorno em estrutura persistivel

`src/sales_routing/api/routes.py`

- manter endpoint principal atual
- opcionalmente expor `modo_calculo` e `twoopt` futuramente

`src/sales_routing/infrastructure/database_reader.py`

- montar grupos com:
  - `cluster_id`
  - `centro_lat`
  - `centro_lon`
  - PDVs

`src/sales_routing/infrastructure/database_writer.py`

- manter persistencia atual
- opcionalmente aceitar metadata adicional do engine

### `routing_engine`

`src/routing_engine/application/route_spreadsheet_use_case.py`

- continuar existindo para o fluxo batch
- passar a usar o novo nucleo puro

`src/routing_engine/application/balanced_subcluster_splitter.py`

- adicionar `modo_calculo`
- manter compatibilidade semantica com o balanceado do `sales_routing`

`src/routing_engine/application/route_optimizer.py`

- manter defensividade atual
- expor metadata de sequencia e uso de `twoopt`

`src/routing_engine/application/route_distance_service.py`

- introduzir `preserve_sequence`
- separar comportamento `route` vs `trip`
- retornar origem e nivel de fallback

`src/routing_engine/routing_task_parallel.py`

- suportar novo master job de grupos preparados
- suportar novo subjob por grupo
- remover dependencia de `ConsultorService` nesse fluxo

`src/routing_engine/api/routes.py`

- manter upload atual
- adicionar endpoint interno para grupos preparados
- adicionar endpoint interno de status

## Criterios de aceite

### Funcionais

- `sales_routing` delega ao `routing_engine` apenas quando `modo == balanceado`
- `fixo` e `adaptativo` continuam funcionando sem alteracao de comportamento
- `routing_engine` aceita grupos preparados por endpoint interno
- o endpoint interno cria um master job e subjobs por grupo
- o `sales_routing` consegue acompanhar o status do engine e persistir o resultado final

### Algoritmicos

- o balanceado do `routing_engine` suporta `modo_calculo`
- o fluxo integrado preserva a sequencia calculada pelo optimizer
- o numero de subclusters e a distribuicao de PDVs permanecem compativeis com o balanceado atual do `sales_routing`

### Operacionais

- nenhum fluxo integrado depende de planilha, subprocesso ou consultor
- nenhum fluxo integrado usa `tenant_id` implicito
- falha parcial do engine e distinguida de falha total
- `sales_routing` continua sendo o dono da persistencia final

## Riscos principais a monitorar

- reordenacao silenciosa de waypoints por `OSRM trip`
- divergencia de `k_inicial` entre os motores
- falha parcial mascarada como sucesso agregado
- dependencia acidental do fluxo de consultor no caminho integrado
- defaults silenciosos de `tenant_id`

## Recomendacao de rollout

1. concluir a refatoracao do `routing_engine`
2. adicionar o endpoint interno e os jobs do engine
3. integrar o `sales_routing` no ramo `balanceado`
4. executar comparacao A/B com o balanceado local atual
5. so entao habilitar o fluxo delegado como caminho oficial