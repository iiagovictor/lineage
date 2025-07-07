import logging
from gremlin_python.process.traversal import T, P, Order
from gremlin_python.process.graph_traversal import __
from neptune_python_utils.gremlin_utils import GremlinUtils
from utils.project_properties import project_job, project_table
from utils.constants import (
    VERTEX_LABEL_JOB, VERTEX_LABEL_TABLE
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_node(g: GremlinUtils, node_type: str, node_name: str) -> list:
    logger.info("Getting node")
    return g.V().has(node_type, "table_name", node_name) \
            .project("id", "table_name") \
            .by(T.id).by("table_name").toList()


def get_edge_projections():
    return __.union(
        # JOB -> TABLE
        __.local(
            __.out("schedule")
            .order().by("dt_updated", Order.desc)
            .limit(1)
        ).out("has")
        .out("produce")
        .hasLabel(VERTEX_LABEL_TABLE).as_("table")
        .select("job", "table")
        .project("from", "to")
        .by(__.select("job").id_())
        .by(__.select("table").id_()),

        # TABLE -> JOB
        __.local(
            __.out("schedule")
            .order().by("dt_updated", Order.desc)
            .limit(1)
        ).out("has")
        .in_("consumed_by")
        .hasLabel(VERTEX_LABEL_TABLE).as_("table")
        .select("job", "table")
        .project("from", "to")
        .by(__.select("table").id_())
        .by(__.select("job").id_())
    )


def get_edge_projections_from_table():
    return __.union(
        # TABELA → JOB (consumed_by)
        __.out("consumed_by")            # to EXECUTION_PLAN
          .inE("has").outV()             # to JOB_RUN
          .inE("schedule").outV()        # to JOB
          .hasLabel(VERTEX_LABEL_JOB).as_("job")
          .select("table", "job")
          .project("from", "to")
          .by(__.select("table").id_())
          .by(__.select("job").id_()),

        # JOB → TABELA (produce)
        __.in_("produce")                # from EXECUTION_PLAN
          .inE("has").outV()             # to JOB_RUN
          .inE("schedule").outV()        # to JOB
          .hasLabel(VERTEX_LABEL_JOB).as_("job")
          .select("job", "table")
          .project("from", "to")
          .by(__.select("job").id_())
          .by(__.select("table").id_())
    )


def asc_path_table():
    return __.inE("produce").outV() \
            .inE("has").outV() \
            .inE("schedule").outV() \
            .out("schedule").out("has") \
            .in_("consumed_by")


def desc_path_table():
    return __.outE("consumed_by").inV() \
            .inE("has").outV() \
            .inE("schedule").outV() \
            .out("schedule").out("has") \
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
        g: GremlinUtils, table_ids: list
        ) -> tuple:

    nodes = g.V().hasLabel(VERTEX_LABEL_TABLE) \
        .has(T.id, P.within(table_ids)).as_("table").union(
        __.out("belongs_to_table").hasLabel("FEATURE"),
        __.out("belongs_to_table").hasLabel("FEATURE")
            .out("has_feature").hasLabel("MODEL")
    ).dedup().choose(
        __.hasLabel("FEATURE"),
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
            __.out("belongs_to_table").hasLabel("FEATURE").as_("feature")
            .project("from", "to")
            .by(__.select("table").id_())
            .by(__.select("feature").id_()),

            # FEATURE -> MODEL
            __.out("belongs_to_table").hasLabel("FEATURE").as_("feature")
            .out("has_feature").hasLabel("MODEL").as_("model")
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


def get_nodes_until_dashboards(g: GremlinUtils, table_ids: list) -> tuple:
    nodes = g.V().hasLabel(VERTEX_LABEL_TABLE) \
        .has(T.id, P.within(table_ids)) \
        .union(
            __.out("read_by").hasLabel("DATASET"),
            __.out("read_by").hasLabel("DATASET")
            .out("uses_dataset").hasLabel("ANALYSIS"),
            __.out("read_by").hasLabel("DATASET")
            .out("uses_dataset").hasLabel("ANALYSIS")
            .out("generate_by").hasLabel("DASHBOARD")
        ).dedup().choose(
            __.hasLabel("DATASET"),
            __.project("id", "label", "dataset_name")
            .by(T.id)
            .by(T.label)
            .by("dataset_name"),
            __.choose(
                __.hasLabel("ANALYSIS"),
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
            __.out("read_by").hasLabel("DATASET").as_("dataset")
            .project("from", "to")
            .by(__.select("table").id_())
            .by(__.select("dataset").id_()),

            # DATASET -> ANALYSIS
            __.out("read_by").hasLabel("DATASET").as_("dataset")
            .out("uses_dataset").hasLabel("ANALYSIS").as_("analysis")
            .project("from", "to")
            .by(__.select("dataset").id_())
            .by(__.select("analysis").id_()),

            # ANALYSIS -> DASHBOARD
            __.out("read_by").hasLabel("DATASET").as_("dataset")
            .out("uses_dataset").hasLabel("ANALYSIS").as_("analysis")
            .out("generate_by").hasLabel("DASHBOARD").as_("dashboard")
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


def get_subgraph_of_the_job(
        g: GremlinUtils,
        job_name: str,
        level: int = 10
        ) -> list:
    job_select = project_job(
        g.V().has(VERTEX_LABEL_JOB, "name", job_name)
    ).dedup().toList()

    ascendant = g.V().has(VERTEX_LABEL_JOB, "name", job_name).as_("start") \
        .union(
            # Caminho original do job
            project_table(
                __.out("schedule").out("has").in_("consumed_by")
            ),
            # Caminho dos descendentes
            __.repeat(
                __.out("schedule")
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
                 __.out("schedule")
                 .out("has")
                 .in_("consumed_by")
                   .choose(
                       __.hasLabel(VERTEX_LABEL_TABLE),
                       project_table(__.identity())
                   )
             ).dedup()
        ).dedup().toList()

    descendant = g.V().has(VERTEX_LABEL_JOB, "name", job_name).as_("start") \
        .union(
            # Caminho original do job
            project_table(
                __.local(
                    __.out("schedule")
                    .order().by("dt_updated", Order.desc)
                    .limit(1)
                ).out("has").out("produce")
            ),
            # Caminho dos descendentes
            __.repeat(
                __.local(
                    __.out("schedule")
                    .order().by("dt_updated", Order.desc)
                    .limit(1)
                ).out("has")
                .out("produce")
                .outE("consumed_by").inV()
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
                    .order().by("dt_updated", Order.desc)
                    .limit(1)
                ).out("has")
                .out("produce")
                .choose(
                    __.hasLabel(VERTEX_LABEL_TABLE),
                    project_table(__.identity())
                )
             ).dedup()
        ).dedup().toList()

    raw_edges = g.V().has(VERTEX_LABEL_JOB, "name", job_name).as_("job") \
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

    tables_ids = [
        node["id"] for node in descendant if node['label'] == 'TABLE'
    ]

    features, models, edges2 = get_nodes_until_models(
        g=g,
        table_ids=tables_ids
    )

    datasets, analyses, dashboards, edges3 = get_nodes_until_dashboards(
            g=g,
            table_ids=tables_ids
        )

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
        item for item in ascendant if item['label'] == 'JOB'
    ]
    ascendant_tables = [
        item for item in ascendant if item['label'] == 'TABLE'
    ]
    descendant_jobs = [
        item for item in descendant if item['label'] == 'JOB'
    ]
    descendant_tables = [
        item for item in descendant if item['label'] == 'TABLE'
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
        table_name: str,
        level: int = 10
        ) -> list:

    table_select = project_table(
        g.V().has(VERTEX_LABEL_TABLE, "table_name", table_name)
    ).dedup().toList()

    source_job = project_job(
        g.V().has(VERTEX_LABEL_TABLE, "table_name", table_name)
        .inE("produce").outV()
        .inE("has").outV()
        .inE("schedule").outV()
    ).dedup().toList()

    ascendant = g.V().has(VERTEX_LABEL_TABLE, "table_name", table_name) \
        .repeat(
                __.inE("produce").outV()
                  .inE("has").outV()
                  .inE("schedule").outV()
                  .out("schedule").out("has").in_("consumed_by")
            ).emit().times(level) \
             .dedup() \
             .union(
                 __.choose(
                     __.hasLabel(VERTEX_LABEL_TABLE),
                     project_table(__.identity())
                 ),
                 __.inE("produce").outV()
                   .inE("has").outV()
                   .inE("schedule").outV()
                   .choose(
                       __.hasLabel(VERTEX_LABEL_JOB),
                       project_job(__.identity())
                   )
             ).dedup().toList()

    descendant = g.V().hasLabel(VERTEX_LABEL_TABLE) \
        .has("table_name", table_name).as_("start") \
        .union(
            # Caminho original do job
            project_job(
                __.outE("consumed_by").inV().inE("has").outV().inE("schedule")
                .outV()
            ),
            # Caminho dos descendentes
            __.repeat(
                __.outE("consumed_by").inV()
                  .inE("has").outV()
                  .inE("schedule").outV()
                  .out("schedule").out("has").out("produce")
            ).emit().times(level)
             .dedup()
             .union(
                 __.choose(
                     __.hasLabel(VERTEX_LABEL_TABLE),
                     project_table(__.identity())
                 ),
                 __.outE("consumed_by").inV()
                   .inE("has").outV()
                   .inE("schedule").outV()
                   .choose(
                       __.hasLabel(VERTEX_LABEL_JOB),
                       project_job(__.identity())
                   )
             ).dedup()
        ).dedup().toList()

    raw_edges = g.V().hasLabel(VERTEX_LABEL_TABLE) \
        .has("table_name", table_name).as_("table") \
        .union(
            get_edge_projections_from_table(),
            __.repeat(asc_path_table()).emit().times(level)
            .as_("table")
            .union(get_edge_projections_from_table()),
            __.repeat(desc_path_table()).emit().times(level)
            .as_("table")
            .union(get_edge_projections_from_table())
        ).dedup().toList()

    tables_ids = [
        node["id"] for node in table_select + descendant
        if node['label'] == 'TABLE'
    ]

    features, models, edges2 = get_nodes_until_models(
        g=g,
        table_ids=tables_ids
    )

    datasets, analyses, dashboards, edges3 = get_nodes_until_dashboards(
            g=g,
            table_ids=tables_ids
        )

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
        item for item in ascendant if item['label'] == 'JOB'
    ]
    ascendant_tables = [
        item for item in ascendant if item['label'] == 'TABLE'
    ]
    descendant_jobs = [
        item for item in descendant if item['label'] == 'JOB'
    ]
    descendant_tables = [
        item for item in descendant if item['label'] == 'TABLE'
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
