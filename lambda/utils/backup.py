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


def get_the_subgraph_of_the_table(
        g: GremlinUtils,
        table_name: str,
        level: int = 10
        ) -> list:
    logger.info("Getting descendant chain of a table")

    source_job = project_job(
        g.V().has("TABLE", "table_name", table_name)
        .inE("produce").outV()
        .inE("has").outV()
        .inE("schedule").outV()
    ).dedup().toList()

    ascendant = g.V().has("TABLE", "table_name", table_name) \
        .union(
            __.repeat(
                __.inE("produce").outV()
                .inE("has").outV()
                .inE("schedule").outV()
                .out("schedule").out("has").in_("consumed_by")
            ).times(level).emit(),
            __.repeat(
                # Chega no Job que produz a tabela
                __.inE("produce").outV()
                .inE("has").outV()
                .inE("schedule").outV()
                # Chega nas tabelas consumidas
                # pelo Job que produz a tabela pesquisada
                .out("schedule").out("has").in_("consumed_by")
                # Chega nos Jobs que consomem as tabelas subsequentes
                .inE("produce").outV()
                .inE("has").outV()
                .inE("schedule").outV()
            ).times(level).emit()
        ) \
        .dedup() \
        .choose(
            __.hasLabel("TABLE"),
            project_table(__.identity()),
            project_job(__.identity())
        ) \
        .toList()

    descendant = g.V().has("TABLE", "table_name", table_name) \
        .union(
            __.repeat(
                __.outE("consumed_by").inV()
                .inE("has").outV()
                .inE("schedule").outV()
            ).times(level).emit(),
            __.repeat(
                __.outE("consumed_by").inV()
                .inE("has").outV()
                .inE("schedule").outV()
                .out("schedule").out("has").out("produce")
            ).times(level).emit(),
            __.repeat(
                __.outE("consumed_by").inV()
                .inE("has").outV()
                .inE("schedule").outV()
                .out("schedule").out("has").out("produce")
                .outE("consumed_by").inV()
                .inE("has").outV()
                .inE("schedule").outV()
            ).times(level).emit()
        ) \
        .dedup() \
        .choose(
            __.hasLabel("TABLE"),
            project_table(__.identity()),
            project_job(__.identity())
        ) \
        .toList()

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
        "sourceJob": source_job,
        "ascendantJobs": ascendant_jobs,
        "ascendantTables": ascendant_tables,
        "descendantJobs": descendant_jobs,
        "descendantTables": descendant_tables
    }
