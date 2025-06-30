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
        "edges": []
    }


def get_the_subgraph_of_the_table(
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
        "descendantTables": descendant_tables
    }
