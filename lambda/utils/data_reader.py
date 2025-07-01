import logging
from gremlin_python.process.traversal import T
from gremlin_python.process.graph_traversal import __
from neptune_python_utils.gremlin_utils import GremlinUtils
from utils.project_properties import project_job, project_table

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
        __.out("schedule")
          .out("has")
          .out("produce")
          .hasLabel("TABLE").as_("table")
          .select("job", "table")
          .project("from", "to")
          .by(__.select("job").id_())
          .by(__.select("table").id_()),

        # TABLE -> JOB
        __.out("schedule")
          .out("has")
          .in_("consumed_by")
          .hasLabel("TABLE").as_("table")
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
          .hasLabel("JOB").as_("job")
          .select("table", "job")
          .project("from", "to")
          .by(__.select("table").id_())
          .by(__.select("job").id_()),

        # JOB → TABELA (produce)
        __.in_("produce")                # from EXECUTION_PLAN
          .inE("has").outV()             # to JOB_RUN
          .inE("schedule").outV()        # to JOB
          .hasLabel("JOB").as_("job")
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


def get_subgraph_of_the_job(
        g: GremlinUtils,
        job_name: str,
        level: int = 10
        ) -> list:
    job_select = project_job(
        g.V().has("JOB", "name", job_name)
    ).dedup().toList()

    ascendant = g.V().has("JOB", "name", job_name).as_("start") \
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
                     __.hasLabel("JOB"),
                     project_job(__.identity())
                 ),
                 __.out("schedule")
                 .out("has")
                 .in_("consumed_by")
                   .choose(
                       __.hasLabel("TABLE"),
                       project_table(__.identity())
                   )
             ).dedup()
        ).dedup().toList()

    descendant = g.V().has("JOB", "name", job_name).as_("start") \
        .union(
            # Caminho original do job
            project_table(
                __.out("schedule").out("has").out("produce")
            ),
            # Caminho dos descendentes
            __.repeat(
                __.out("schedule")
                .out("has")
                .out("produce")
                .outE("consumed_by").inV()
                .inE("has").outV()
                .inE("schedule").outV()
            ).emit().times(level)
             .dedup()
             .union(
                 __.choose(
                     __.hasLabel("JOB"),
                     project_job(__.identity())
                 ),
                 __.out("schedule")
                 .out("has")
                 .out("produce")
                   .choose(
                       __.hasLabel("TABLE"),
                       project_table(__.identity())
                   )
             ).dedup()
        ).dedup().toList()

    raw_edges = g.V().has("JOB", "name", job_name).as_("job") \
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

    valid_ids = set()
    for node in job_select + ascendant + descendant:
        valid_ids.add(node["id"])

    edges = [
        edge for edge in raw_edges
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
        "edges": edges
    }


def get_subgraph_of_the_table(
        g: GremlinUtils,
        table_name: str,
        level: int = 10
        ) -> list:

    table_select = project_table(
        g.V().has("TABLE", "table_name", table_name)
    ).dedup().toList()

    source_job = project_job(
        g.V().has("TABLE", "table_name", table_name)
        .inE("produce").outV()
        .inE("has").outV()
        .inE("schedule").outV()
    ).dedup().toList()

    ascendant = g.V().has("TABLE", "table_name", table_name) \
        .repeat(
                __.inE("produce").outV()
                  .inE("has").outV()
                  .inE("schedule").outV()
                  .out("schedule").out("has").in_("consumed_by")
            ).emit().times(level) \
             .dedup() \
             .union(
                 __.choose(
                     __.hasLabel("TABLE"),
                     project_table(__.identity())
                 ),
                 __.inE("produce").outV()
                   .inE("has").outV()
                   .inE("schedule").outV()
                   .choose(
                       __.hasLabel("JOB"),
                       project_job(__.identity())
                   )
             ).dedup().toList()

    descendant = g.V().has("TABLE", "table_name", table_name).as_("start") \
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
                     __.hasLabel("TABLE"),
                     project_table(__.identity())
                 ),
                 __.outE("consumed_by").inV()
                   .inE("has").outV()
                   .inE("schedule").outV()
                   .choose(
                       __.hasLabel("JOB"),
                       project_job(__.identity())
                   )
             ).dedup()
        ).dedup().toList()

    raw_edges = g.V().has("TABLE", "table_name", table_name).as_("table") \
        .union(
            get_edge_projections_from_table(),
            __.repeat(asc_path_table()).emit().times(level)
            .as_("table")
            .union(get_edge_projections_from_table()),
            __.repeat(desc_path_table()).emit().times(level)
            .as_("table")
            .union(get_edge_projections_from_table())
        ).dedup().toList()

    # IDs válidos para filtrar edges
    valid_ids = set()
    for node in table_select + ascendant + descendant + source_job:
        valid_ids.add(node["id"])

    edges = [
        edge for edge in raw_edges
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
        "edges": edges
    }
