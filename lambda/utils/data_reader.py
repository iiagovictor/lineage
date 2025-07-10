import logging
from concurrent.futures import ThreadPoolExecutor
from gremlin_python.process.traversal import T, P, Order
from gremlin_python.process.graph_traversal import __
from neptune_python_utils.gremlin_utils import GremlinUtils
from utils.project_properties import project_job, project_table
from utils.constants import (
    VERTEX_LABEL_JOB,
    VERTEX_LABEL_TABLE,
    VERTEX_LABEL_FEATURE,
    VERTEX_LABEL_MODEL,
    VERTEX_LABEL_DATASET,
    VERTEX_LABEL_ANALYSIS,
    VERTEX_LABEL_DASHBOARD
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_node(g: GremlinUtils, node_type: str, node_name: str) -> list:
    logger.info("Getting node")
    return g.V().has(node_type, "table_name", node_name) \
            .project("id", "table_name") \
            .by(T.id).by("table_name").toList()


def get_edge_projections():
    """Retorna as projeções de arestas entre JOB e TABLE.

    Retorna duas projeções:

    1. JOB -> TABLE (produzido por)
    2. TABLE -> JOB (consumido por)

    Cada projeção contém os campos "from" e "to", que representam os IDs dos
    vértices de origem e destino, respectivamente.
    Essas projeções são usadas para construir um grafo de dependências entre
    jobs e tabelas, onde um job pode produzir uma tabela e uma tabela pode ser
    consumida por um job.
    A projeção é feita usando a sintaxe do Gremlin, que é uma linguagem de
    consulta para grafos. A consulta utiliza a função `union` para combinar
    os resultados de duas subconsultas:
    - A primeira subconsulta busca os jobs que produzem tabelas, ordenando por
      data de atualização (`dt_update`) em ordem decrescente e limitando o
      resultado a execução mais recente do job.
    - A segunda subconsulta busca as tabelas que são consumidas por jobs,
      também ordenando por data de atualização e limitando o resultado a
      execução mais recente do job.
    Ambas as subconsultas utilizam a função `local` para garantir que a
    consulta seja executada no contexto do job mais recente. A função `where`
    é usada para filtrar os jobs e tabelas relevantes, garantindo que apenas as
    relações válidas sejam consideradas. A função `select` é usada para extrair
    os IDs dos jobs e tabelas envolvidos, e a função `project` para formatar
    o resultado final com os campos "from" e "to".
    Essa função é útil para construir visualizações de dependências entre jobs
    e tabelas em um sistema de gerenciamento de dados, permitindo entender como
    os jobs interagem com as tabelas e quais tabelas são produzidas ou
    consumidas por quais jobs.

    Returns:
    --------
        Gremlin traversal: Projeções de arestas entre JOB e TABLE.

    """
    return __.union(
        # JOB -> TABLE
        __.local(
            __.out("schedule")
            .order().by("dt_update", Order.desc)
            .limit(1)
        )
        .out("has")
        .out("produce")
        .hasLabel(VERTEX_LABEL_TABLE).as_("table")
        .select("job", "table")
        .project("from", "to")
        .by(__.select("job").id_())
        .by(__.select("table").id_()),

        # TABLE -> JOB
        __.local(
            __.out("schedule")
            .order().by("dt_update", Order.desc)
            .limit(1)
        )
        .out("has")
        .in_("consumed_by")
        .hasLabel(VERTEX_LABEL_TABLE).as_("table")
        .select("job", "table")
        .project("from", "to")
        .by(__.select("table").id_())
        .by(__.select("job").id_())
    )


def get_edge_projections_from_table():
    """Retorna as projeções de arestas entre TABLE e JOB.

    Retorna duas projeções:
    1. TABELA → JOB (consumed_by)
    2. JOB → TABELA (produce)

    Cada projeção contém os campos "from" e "to", que representam os IDs dos
    vértices de origem e destino, respectivamente.
    Essas projeções são usadas para construir um grafo de dependências entre
    tabelas e jobs, onde uma tabela pode ser consumida por um job e um job
    pode produzir uma tabela.
    A projeção é feita usando a sintaxe do Gremlin, que é uma linguagem de
    consulta para grafos. A consulta utiliza a função `union` para combinar
    os resultados de duas subconsultas:
    - A primeira subconsulta busca as tabelas que são consumidas por jobs,
      ordenando por data de atualização (`dt_update`) em ordem decrescente e
      limitando o resultado a execução mais recente do job.
    - A segunda subconsulta busca os jobs que produzem tabelas, também
      ordenando por data de atualização e limitando o resultado a execução
      mais recente do job.
    Ambas as subconsultas utilizam a função `where` para filtrar os jobs e
    tabelas relevantes, garantindo que apenas as relações válidas sejam
    consideradas. A função `select` é usada para extrair os IDs dos jobs e
    tabelas envolvidos, e a função `project` para formatar o resultado final
    com os campos "from" e "to".
    Essa função é útil para construir visualizações de dependências entre
    tabelas e jobs em um sistema de gerenciamento de dados, permitindo entender
    como as tabelas interagem com os jobs e quais tabelas são consumidas ou
    produzidas por quais jobs.
    """
    return __.union(
        # TABELA → JOB (consumed_by)
        __.as_("table")
        .out("consumed_by").as_("plan")        # EXECUTION_PLAN
        .inE("has").outV().as_("job_run")      # JOB_RUN
        .inE("schedule").outV().as_("job")     # JOB
        .where(
            __.select("job")
            .out("schedule")
            .order().by("dt_update", Order.desc)
            .limit(1)
            .where(P.eq("job_run"))
        )
        .where(
            __.select("plan")
            .in_("consumed_by")
            .where(P.eq("table"))
        )
        .project("from", "to")
        .by(__.select("table").id_())
        .by(__.select("job").id_()),

        # JOB → TABELA (produce)
        __.as_("table")
        .in_("produce").as_("plan")            # EXECUTION_PLAN
        .inE("has").outV().as_("job_run")      # JOB_RUN
        .inE("schedule").outV().as_("job")     # JOB
        .where(
            __.select("job")
            .out("schedule")
            .order().by("dt_update", Order.desc)
            .limit(1)
            .where(P.eq("job_run"))
        )
        .where(
            __.select("plan")
            .out("produce")
            .where(P.eq("table"))
        )
        .project("from", "to")
        .by(__.select("job").id_())
        .by(__.select("table").id_())
    )


def asc_path_table():
    return __.as_("current_table") \
            .inE("produce").outV() \
            .inE("has").outV() \
            .inE("schedule").outV() \
            .where(
                __.local(
                    __.out("schedule")
                    .order().by("dt_update", Order.desc)
                    .limit(1)
                )
                .out("has")
                .out("produce")
                .where(P.eq("current_table"))
            ) \
            .local(
                __.out("schedule")
                .order().by("dt_update", Order.desc)
                .limit(1)
            ) \
            .out("has") \
            .in_("consumed_by")


def desc_path_table():
    return __.as_("current_table") \
            .outE("consumed_by").inV() \
            .inE("has").outV() \
            .inE("schedule").outV().as_("current_job") \
            .where(
                __.local(
                    __.out("schedule")
                    .order().by("dt_update", Order.desc)
                    .limit(1)
                )
                .out("has")
                .in_("consumed_by")
                .where(P.eq("current_table"))
            ) \
            .local(
                __.out("schedule")
                .order().by("dt_update", Order.desc)
                .limit(1)
            ) \
            .out("has") \
            .out("produce")


def asc_path_job():
    return __.out("schedule") \
            .out("has") \
            .in_("consumed_by") \
            .inE("produce").outV() \
            .inE("has").outV() \
            .inE("schedule").outV()


def desc_path_job():
    return __.out("schedule") \
            .out("has") \
            .out("produce") \
            .outE("consumed_by").inV() \
            .inE("has").outV() \
            .inE("schedule").outV()


def get_nodes_until_models(
        g: GremlinUtils,
        table_ids: list
        ) -> tuple:
    """Retorna os nós e arestas de features e modelos associados a tabelas.
    Esta função consulta o grafo para obter nós e arestas relacionados a
    features e modelos associados a tabelas específicas, identificadas por seus
    IDs. Ela utiliza a biblioteca Gremlin para realizar consultas em grafos e
    retorna uma tupla contendo três listas:
    - features: lista de dicionários representando as features.
    - models: lista de dicionários representando os modelos.
    - edges: lista de dicionários representando as arestas entre features e
      modelos, onde cada aresta contém os campos "from" e "to" representando
      os IDs dos vértices de origem e destino, respectivamente.
    Args:
    -----
    g (GremlinUtils): Instância da classe GremlinUtils para realizar
        consultas em grafos.
    table_ids (list): Lista de IDs de tabelas para as quais as features e
        modelos associados serão recuperados.

    Returns:
    -------
    tuple: Uma tupla contendo três listas:
        - features: Lista de dicionários representando as features.
        - models: Lista de dicionários representando os modelos.
        - edges: Lista de dicionários representando as arestas entre features e
          modelos, onde cada aresta contém os campos "from" e "to"
          representando os IDs dos vértices de origem e destino,
          respectivamente.
    """
    nodes = g.V().hasLabel(VERTEX_LABEL_TABLE) \
        .has(T.id, P.within(table_ids)).as_("table").union(
        __.out("belongs_to_table").hasLabel(VERTEX_LABEL_FEATURE),
        __.out("belongs_to_table").hasLabel(VERTEX_LABEL_FEATURE)
            .out("has_feature").hasLabel(VERTEX_LABEL_MODEL)
    ).dedup().choose(
        __.hasLabel(VERTEX_LABEL_FEATURE),
        __.project("id", "label", "feature_name")
            .by(T.id)
            .by(T.label)
            .by("feature_name"),
        __.project("id", "label", "model_name")
            .by(T.id)
            .by(T.label)
            .by("model_name")
    ).toList()

    edges = g.V().hasLabel(VERTEX_LABEL_TABLE) \
        .has(T.id, P.within(table_ids)).as_("table") \
        .union(
            # TABLE -> FEATURE
            __.out("belongs_to_table")
            .hasLabel(VERTEX_LABEL_FEATURE).as_("feature")
            .project("from", "to")
            .by(__.select("table").id_())
            .by(__.select("feature").id_()),

            # FEATURE -> MODEL
            __.out("belongs_to_table")
            .hasLabel(VERTEX_LABEL_FEATURE).as_("feature")
            .out("has_feature")
            .hasLabel(VERTEX_LABEL_MODEL).as_("model")
            .project("from", "to")
            .by(__.select("feature").id_())
            .by(__.select("model").id_())
        ).toList()

    features = [
        item for item in nodes if item['label'] == 'FEATURE'
    ]
    models = [
        item for item in nodes if item['label'] == 'MODEL'
    ]

    return features, models, edges


def get_nodes_until_dashboards(
        g: GremlinUtils,
        table_ids: list
        ) -> tuple:
    """Retorna os nós e arestas de datasets, análises e dashboards
    associados a tabelas. Esta função consulta o grafo para obter
    nós e arestas relacionados a datasets, análises e dashboards
    associados a tabelas específicas, identificadas por seus IDs. Ela
    utiliza a biblioteca Gremlin para realizar consultas em grafos e
    retorna uma tupla contendo três listas:
    - datasets: lista de dicionários representando os datasets.
    - analyses: lista de dicionários representando as análises.
    - dashboards: lista de dicionários representando os dashboards.
    - edges: lista de dicionários representando as arestas entre datasets,
      análises e dashboards, onde cada aresta contém os campos "from" e "to"
      representando os IDs dos vértices de origem e destino, respectivamente.

    Args:
    -----
    g (GremlinUtils): Instância da classe GremlinUtils para realizar
        consultas em grafos.
    table_ids (list): Lista de IDs de tabelas para as quais os datasets,
        análises e dashboards associados serão recuperados.

    Returns:
    -------
    tuple: Uma tupla contendo quatro listas:
        - datasets: Lista de dicionários representando os datasets.
        - analyses: Lista de dicionários representando as análises.
        - dashboards: Lista de dicionários representando os dashboards.
        - edges: Lista de dicionários representando as arestas entre datasets,
          análises e dashboards, onde cada aresta contém os campos "from"
          e "to" representando os IDs dos vértices de origem e destino,
          respectivamente.
    """
    nodes = g.V().hasLabel(VERTEX_LABEL_TABLE) \
        .has(T.id, P.within(table_ids)) \
        .union(
            __.out("read_by").hasLabel(VERTEX_LABEL_DATASET),
            __.out("read_by").hasLabel(VERTEX_LABEL_DATASET)
            .out("uses_dataset").hasLabel(VERTEX_LABEL_ANALYSIS),
            __.out("read_by").hasLabel(VERTEX_LABEL_DATASET)
            .out("uses_dataset").hasLabel(VERTEX_LABEL_ANALYSIS)
            .out("generate_by").hasLabel(VERTEX_LABEL_DASHBOARD)
        ).dedup().choose(
            __.hasLabel(VERTEX_LABEL_DATASET),
            __.project("id", "label", "dataset_name")
            .by(T.id)
            .by(T.label)
            .by("dataset_name"),
            __.choose(
                __.hasLabel(VERTEX_LABEL_ANALYSIS),
                __.project("id", "label", "analysis_name")
                .by(T.id)
                .by(T.label)
                .by("analysis_name"),
                __.project("id", "label", "dashboard_name")
                .by(T.id)
                .by(T.label)
                .by("dashboard_name")
            )
        ).toList()

    edges = g.V().hasLabel(VERTEX_LABEL_TABLE) \
        .has(T.id, P.within(table_ids)).as_("table") \
        .union(
            # TABLE -> DATASET
            __.out("read_by")
            .hasLabel(VERTEX_LABEL_DATASET).as_("dataset")
            .project("from", "to")
            .by(__.select("table").id_())
            .by(__.select("dataset").id_()),

            # DATASET -> ANALYSIS
            __.out("read_by")
            .hasLabel(VERTEX_LABEL_DATASET).as_("dataset")
            .out("uses_dataset")
            .hasLabel(VERTEX_LABEL_ANALYSIS).as_("analysis")
            .project("from", "to")
            .by(__.select("dataset").id_())
            .by(__.select("analysis").id_()),

            # ANALYSIS -> DASHBOARD
            __.out("read_by")
            .hasLabel(VERTEX_LABEL_DATASET).as_("dataset")
            .out("uses_dataset")
            .hasLabel(VERTEX_LABEL_ANALYSIS).as_("analysis")
            .out("generate_by")
            .hasLabel(VERTEX_LABEL_DASHBOARD).as_("dashboard")
            .project("from", "to")
            .by(__.select("analysis").id_())
            .by(__.select("dashboard").id_())
        ).toList()

    datasets = [
        item for item in nodes if item['label'] == 'DATASET'
    ]
    analyses = [
        item for item in nodes if item['label'] == 'ANALYSIS'
    ]
    dashboards = [
        item for item in nodes if item['label'] == 'DASHBOARD'
    ]

    return datasets, analyses, dashboards, edges


def get_ascendant_nodes_from_job(
        g: GremlinUtils,
        jobs_ids: list,
        level: int = 10
        ) -> list:
    """
    """
    return g.V().hasLabel(VERTEX_LABEL_JOB) \
        .has(T.id, P.within(jobs_ids)).as_("start") \
        .union(
            # Caminho original do job
            project_table(
                __.local(
                    __.out("schedule")
                    .order().by("dt_update", Order.desc)
                    .limit(1)
                )
                .out("has")
                .in_("consumed_by")
            ),
            # Caminho dos descendentes
            __.repeat(
                __.local(
                    __.out("schedule")
                    .order().by("dt_update", Order.desc)
                    .limit(1)
                )
                .out("has")
                .in_("consumed_by")
                .inE("produce").outV()
                .inE("has").outV()
                .inE("schedule").outV()
            ).emit().times(level)
             .dedup()
             .union(
                __.choose(
                    __.hasLabel(VERTEX_LABEL_JOB),
                    project_job(__.identity())
                ),
                __.local(
                    __.out("schedule")
                    .order().by("dt_update", Order.desc)
                    .limit(1)
                )
                .out("has")
                .in_("consumed_by")
                .choose(
                    __.hasLabel(VERTEX_LABEL_TABLE),
                    project_table(__.identity())
                )
             ).dedup()
        ).dedup().toList()


def get_descendant_nodes_from_job(
        g: GremlinUtils,
        jobs_ids: list,
        level: int = 10
        ) -> list:
    return g.V().hasLabel(VERTEX_LABEL_JOB) \
        .has(T.id, P.within(jobs_ids)).as_("start") \
        .union(
            # Caminho original do job
            project_table(
                __.local(
                    __.out("schedule")
                    .order().by("dt_update", Order.desc)
                    .limit(1)
                )
                .out("has")
                .out("produce")
            ),
            # Caminho dos descendentes
            __.repeat(
                __.local(
                    __.out("schedule")
                    .order().by("dt_update", Order.desc)
                    .limit(1)
                )
                .out("has")
                .out("produce").as_("current_table")
                .outE("consumed_by").inV()
                .inE("has").outV()
                .inE("schedule").outV().as_("current_job")
                .where(
                    __.local(
                        __.out("schedule")
                        .order().by("dt_update", Order.desc)
                        .limit(1)
                    )
                    .out("has")
                    .in_("consumed_by")
                    .where(P.eq("current_table"))
                )
            ).emit().times(level)
             .dedup()
             .union(
                __.choose(
                     __.hasLabel(VERTEX_LABEL_JOB),
                     project_job(__.identity())
                ),
                __.local(
                    __.out("schedule")
                    .order().by("dt_update", Order.desc)
                    .limit(1)
                )
                .out("has")
                .out("produce")
                .choose(
                    __.hasLabel(VERTEX_LABEL_TABLE),
                    project_table(__.identity())
                )
             ).dedup()
        ).dedup().toList()


def get_ascendant_nodes_from_table(
        g: GremlinUtils,
        tables_ids: list,
        level: int = 10
        ) -> list:
    return g.V().hasLabel(VERTEX_LABEL_TABLE) \
        .has(T.id, P.within(tables_ids)) \
        .repeat(
                __.as_("current_table")
                .inE("produce").outV()
                .inE("has").outV()
                .inE("schedule").outV().as_("current_job")
                .where(
                    __.local(
                        __.out("schedule")
                        .order().by("dt_update", Order.desc)
                        .limit(1)
                    )
                    .out("has")
                    .out("produce")
                    .where(P.eq("current_table"))
                )
                .local(
                    __.out("schedule")
                    .order().by("dt_update", Order.desc)
                    .limit(1)
                )
                .out("has").in_("consumed_by")
            ).emit().times(level) \
             .dedup() \
             .union(
                 __.choose(
                     __.hasLabel(VERTEX_LABEL_TABLE),
                     project_table(__.identity())
                 ),
                 __.as_("current_table")
                 .inE("produce").outV()
                 .inE("has").outV()
                 .inE("schedule").outV().as_("current_job")
                 .where(
                    __.local(
                        __.out("schedule")
                        .order().by("dt_update", Order.desc)
                        .limit(1)
                    )
                    .out("has")
                    .out("produce")
                    .where(P.eq("current_table"))
                    )
                   .choose(
                       __.hasLabel(VERTEX_LABEL_JOB),
                       project_job(__.identity())
                   )
             ).dedup().toList()


def get_descendant_nodes_from_table(
        g: GremlinUtils,
        tables_ids: list,
        level: int = 10
        ) -> list:
    return g.V().hasLabel(VERTEX_LABEL_TABLE) \
        .has(T.id, P.within(tables_ids)).as_("start") \
        .union(
            # Caminho original do job
            project_job(
                __.as_("current_table")
                .outE("consumed_by").inV()
                .inE("has").outV()
                .inE("schedule").outV().as_("current_job")
                .where(
                    __.local(
                        __.out("schedule")
                        .order().by("dt_update", Order.desc)
                        .limit(1)
                    )
                    .out("has")
                    .in_("consumed_by")
                    .where(P.eq("current_table"))
                )
            ),
            # Caminho dos descendentes
            __.repeat(
                __.as_("current_table")
                .outE("consumed_by").inV()
                .inE("has").outV()
                .inE("schedule").outV().as_("current_job")
                .where(
                    __.local(
                        __.out("schedule")
                        .order().by("dt_update", Order.desc)
                        .limit(1)
                        )
                    .out("has")
                    .in_("consumed_by")
                    .where(P.eq("current_table"))
                )
                .local(
                    __.out("schedule")
                    .order().by("dt_update", Order.desc)
                    .limit(1)
                )
                .out("has")
                .out("produce")
            ).emit().times(level)
             .dedup()
             .union(
                 __.choose(
                     __.hasLabel(VERTEX_LABEL_TABLE),
                     project_table(__.identity())
                 ),
                 __.as_("current_table")
                 .outE("consumed_by").inV()
                 .inE("has").outV()
                 .inE("schedule").outV().as_("current_job")
                 .where(
                    __.local(
                        __.out("schedule")
                        .order().by("dt_update", Order.desc)
                        .limit(1)
                    )
                    .out("has")
                    .in_("consumed_by")
                    .where(P.eq("current_table"))
                    )
                .choose(
                    __.hasLabel(VERTEX_LABEL_JOB),
                    project_job(__.identity())
                   )
             ).dedup()
        ).dedup().toList()


def get_edges_from_job(
        g: GremlinUtils,
        jobs_ids: list,
        level: int = 10
        ) -> list:
    return g.V().hasLabel(VERTEX_LABEL_JOB) \
        .has(T.id, P.within(jobs_ids)).as_("job") \
        .union(
            # ascendant edges
            get_edge_projections(),
            __.repeat(asc_path_job()).emit().times(level).as_("job")
            .union(get_edge_projections()),
            # descendant edges
            __.repeat(desc_path_job()).emit().times(level).as_("job")
            .union(get_edge_projections())
        ) \
        .dedup().toList()


def get_edges_from_table(
        g: GremlinUtils,
        tables_ids: list,
        level: int = 10
        ) -> list:
    return g.V().hasLabel(VERTEX_LABEL_TABLE) \
        .has(T.id, P.within(tables_ids)).as_("table") \
        .union(
            get_edge_projections_from_table(),
            __.repeat(asc_path_table()).emit().times(level)
            .as_("table")
            .union(get_edge_projections_from_table()),
            __.repeat(desc_path_table()).emit().times(level)
            .as_("table")
            .union(get_edge_projections_from_table())
        ).dedup().toList()


def get_subgraph_of_the_job(
        g: GremlinUtils,
        jobId: str,
        level: int = 10
        ) -> list:
    """Retorna o subgrafo de um job específico.
    Esta função consulta o grafo para obter informações sobre um job
    específico, identificado pelo seu ID. Ela utiliza a biblioteca Gremlin
    para realizar consultas em grafos e retorna um dicionário contendo as
    seguintes informações:
    - job: Dicionário representando o job selecionado.
    - ascendantJobs: Lista de dicionários representando os jobs ascendentes
      relacionados ao job.
    - ascendantTables: Lista de dicionários representando as tabelas
      ascendentes relacionadas ao job.
    - descendantJobs: Lista de dicionários representando os jobs descendentes
      relacionados ao job.
    - descendantTables: Lista de dicionários representando as tabelas
      descendentes relacionadas ao job.
    - features: Lista de dicionários representando as features associadas ao
      job.
    - models: Lista de dicionários representando os modelos associados ao
      job.
    - datasets: Lista de dicionários representando os datasets associados ao
      job.
    - analyses: Lista de dicionários representando as análises associadas ao
      job.
    - dashboards: Lista de dicionários representando os dashboards associados
      ao job.
    - edges: Lista de dicionários representando as arestas entre os nós do
      grafo, onde cada aresta contém os campos "from" e "to" representando
      os IDs dos vértices de origem e destino, respectivamente.

    Args:
    -----
    g (GremlinUtils): Instância da classe GremlinUtils para realizar
        consultas em grafos.
    jobId (str): ID do job para o qual o subgrafo será recuperado.
    level (int): Nível de profundidade para a busca de nós ascendentes e
        descendentes. O padrão é 10, o que significa que a busca irá até
        10 níveis acima e abaixo do job.

    Returns:
    -------
    dict: Um dicionário contendo as informações sobre o job e seus
        relacionamentos no grafo, incluindo o job selecionado, jobs
        ascendentes e descendentes, tabelas ascendentes e descendentes,
        features, modelos, datasets, análises, dashboards e arestas.
    """
    job_select = project_job(
        g.V().has(VERTEX_LABEL_JOB, "name", jobId)
        ).dedup().toList()

    job_id = job_select[0]["id"] if job_select else None

    with ThreadPoolExecutor(max_workers=4) as executor:
        future_asc = executor.submit(
            get_ascendant_nodes_from_job, g, [job_id], level
            )
        future_desc = executor.submit(
            get_descendant_nodes_from_job, g, [job_id], level
            )
        future_edges = executor.submit(
            get_edges_from_job, g, [job_id], level
            )

        # Coletando os resultados
        ascendant = future_asc.result()
        descendant = future_desc.result()
        raw_edges = future_edges.result()

    tables_ids = [
        node["id"] for node in descendant if node['label'] == VERTEX_LABEL_TABLE
    ]

    with ThreadPoolExecutor(max_workers=2) as executor:
        future_models = executor.submit(
            get_nodes_until_models, g, tables_ids
            )
        future_dashboards = executor.submit(
            get_nodes_until_dashboards, g, tables_ids
            )

        features, models, edges2 = future_models.result()
        datasets, analyses, dashboards, edges3 = future_dashboards.result()

    valid_ids = set()
    for node in job_select + ascendant + descendant + features + \
            models + datasets + analyses + dashboards:
        valid_ids.add(node["id"])

    edges = [
        edge for edge in raw_edges + edges2 + edges3
        if isinstance(edge, dict) and
        edge.get("from") in valid_ids and
        edge.get("to") in valid_ids
    ]

    ascendant_jobs = [
        item for item in ascendant if item['label'] == VERTEX_LABEL_JOB
    ]
    ascendant_tables = [
        item for item in ascendant if item['label'] == VERTEX_LABEL_TABLE
    ]
    descendant_jobs = [
        item for item in descendant if item['label'] == VERTEX_LABEL_JOB
    ]
    descendant_tables = [
        item for item in descendant if item['label'] == VERTEX_LABEL_TABLE
    ]

    return {
        "job": job_select[0] if job_select else {},
        "ascendantJobs": ascendant_jobs,
        "ascendantTables": ascendant_tables,
        "descendantJobs": descendant_jobs,
        "descendantTables": descendant_tables,
        "features": features,
        "models": models,
        "datasets": datasets,
        "analyses": analyses,
        "dashboards": dashboards,
        "edges": edges
    }


def get_subgraph_of_the_table(
        g: GremlinUtils,
        tableId: str,
        level: int = 10
        ) -> list:
    """Retorna o subgrafo de uma tabela específica.
    Esta função consulta o grafo para obter informações sobre uma tabela
    específica, identificada pelo seu ID. Ela utiliza a biblioteca Gremlin
    para realizar consultas em grafos e retorna um dicionário contendo as
    seguintes informações:
    - table: Dicionário representando a tabela selecionada.
    - sourceJob: Lista de dicionários representando os jobs que produzem a
      tabela.
    - ascendantJobs: Lista de dicionários representando os jobs ascendentes
      relacionados à tabela.
    - ascendantTables: Lista de dicionários representando as tabelas
      ascendentes relacionadas à tabela.
    - descendantJobs: Lista de dicionários representando os jobs descendentes
      relacionados à tabela.
    - descendantTables: Lista de dicionários representando as tabelas
      descendentes relacionadas à tabela.
    - features: Lista de dicionários representando as features associadas à
      tabela.
    - models: Lista de dicionários representando os modelos associados à
      tabela.
    - datasets: Lista de dicionários representando os datasets associados à
      tabela.
    - analyses: Lista de dicionários representando as análises associadas à
      tabela.
    - dashboards: Lista de dicionários representando os dashboards associados
      à tabela.
    - edges: Lista de dicionários representando as arestas entre os nós do
      grafo, onde cada aresta contém os campos "from" e "to" representando
      os IDs dos vértices de origem e destino, respectivamente.

    Args:
    -----
    g (GremlinUtils): Instância da classe GremlinUtils para realizar
        consultas em grafos.
    tableId (str): ID da tabela para a qual o subgrafo será recuperado.
    level (int): Nível de profundidade para a busca de nós ascendentes e
        descendentes. O padrão é 10, o que significa que a busca irá até
        10 níveis acima e abaixo da tabela.

    Returns:
    -------
    dict: Um dicionário contendo as informações sobre a tabela e seus
        relacionamentos no grafo, incluindo a tabela selecionada, jobs
        ascendentes e descendentes, tabelas ascendentes e descendentes,
        features, modelos, datasets, análises, dashboards e arestas.
    """
    table_select = project_table(
        g.V().has(VERTEX_LABEL_TABLE, "table_name", tableId)
        ).dedup().toList()

    table_id = table_select[0]["id"] if table_select else None

    with ThreadPoolExecutor(max_workers=5) as executor:
        future_source_job = executor.submit(
            lambda: project_job(
                g.V().has(VERTEX_LABEL_TABLE, "table_name", tableId)
                .inE("produce").outV()
                .inE("has").outV()
                .inE("schedule").outV()
            ).dedup().toList()
        )
        future_asc = executor.submit(
            get_ascendant_nodes_from_table, g, [table_id], level
            )
        future_desc = executor.submit(
            get_descendant_nodes_from_table, g, [table_id], level
            )
        future_edges = executor.submit(
            get_edges_from_table, g, [table_id], level
            )

        source_job = future_source_job.result()
        ascendant = future_asc.result()
        descendant = future_desc.result()
        raw_edges = future_edges.result()

    tables_ids = [
        node["id"] for node in table_select + descendant
        if node['label'] == VERTEX_LABEL_TABLE
    ]

    with ThreadPoolExecutor(max_workers=2) as executor:
        future_models = executor.submit(
            get_nodes_until_models, g, tables_ids
            )
        future_dashboards = executor.submit(
            get_nodes_until_dashboards, g, tables_ids
            )

        features, models, edges2 = future_models.result()
        datasets, analyses, dashboards, edges3 = future_dashboards.result()

    valid_ids = set()
    for node in table_select + ascendant + descendant + source_job + \
            features + models + datasets + analyses + dashboards:
        valid_ids.add(node["id"])

    edges = [
        edge for edge in raw_edges + edges2 + edges3
        if isinstance(edge, dict) and
        edge.get("from") in valid_ids and
        edge.get("to") in valid_ids
    ]

    ascendant_jobs = [
        item for item in ascendant if item['label'] == VERTEX_LABEL_JOB
    ]
    ascendant_tables = [
        item for item in ascendant if item['label'] == VERTEX_LABEL_TABLE
    ]
    descendant_jobs = [
        item for item in descendant if item['label'] == VERTEX_LABEL_JOB
    ]
    descendant_tables = [
        item for item in descendant if item['label'] == VERTEX_LABEL_TABLE
    ]
    return {
        "table": table_select[0] if table_select else {},
        "sourceJob": source_job,
        "ascendantJobs": ascendant_jobs,
        "ascendantTables": ascendant_tables,
        "descendantJobs": descendant_jobs,
        "descendantTables": descendant_tables,
        "features": features,
        "models": models,
        "datasets": datasets,
        "analyses": analyses,
        "dashboards": dashboards,
        "edges": edges
    }
