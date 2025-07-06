import json
import logging
import os
from neptune_python_utils.gremlin_utils import GremlinUtils, Endpoints
from gremlin_python.process.traversal import T
from gremlin_python.process.graph_traversal import __

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def create_connection(neptune_endpoint: str, neptune_port: str) -> tuple:
    GremlinUtils.init_statics(globals=globals())
    endpoints = Endpoints(
        neptune_endpoint=neptune_endpoint,
        neptune_port=neptune_port
    )
    gremlin_utils = GremlinUtils(endpoints=endpoints)
    conn = gremlin_utils.remote_connection()
    return gremlin_utils, conn


def create_mock() -> dict:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_dir, "data.json")
    with open(file_path, "r") as file:
        data = json.load(file)
    return data


def bulk_load_data(g: GremlinUtils, load_mock: bool = False) -> None:
    if load_mock:
        data = create_mock()
        g.V().drop().iterate()
        g.E().drop().iterate()
        nodes = []
        keys = list(data['nodes'].keys())
        for key in keys:
            for v in data['nodes'][key]:
                nodes.append(v)

        logger.info(f"Loading {len(nodes)} nodes and \
                    {len(data['edges'])} edges into the graph.")

        for node in nodes:
            try:
                v = g.addV(node['~label']).property(T.id, node['~id'])
                for key, value in node.items():
                    if key not in ['~label', '~id']:
                        safe_key = key.split(":")[0]
                        v = v.property(safe_key, value)
                v.next()
            except Exception as e:
                logger.error(f"Erro ao inserir nó {node['~id']}: {e} - {node}")
                raise e

        for edge in data['edges']:
            g.V(edge['~from']) \
                .addE(edge['~label']) \
                .to(__.V(edge['~to'])) \
                .next()

        logger.info("Data loaded successfully.")
    else:
        logger.info("No mock data provided, skipping bulk load.")


def merge_table_data(
    g,
    table_name: str,
    table_merged: str
) -> dict:
    try:
        # Buscar os IDs das tabelas
        id_table = g.V().has("TABLE", "table_name", table_name) \
            .project("id").by(T.id).toList()[0]["id"]
        id_table_merged = g.V().has("TABLE", "table_name", table_merged) \
            .project("id").by(T.id).toList()[0]["id"]
    except IndexError:
        return {
                "statusCode": 404,
                "body": json.dumps({
                    "message": "One or both tables not found."
                }),
            }

    logger.info(
        f"Migrando edges da tabela '{table_merged}' para '{table_name}'"
    )

    # Redireciona arestas "consumed_by" para a nova tabela
    g.V(id_table_merged).outE("consumed_by").as_("e").inV() \
        .addE("consumed_by").from_(__.V(id_table)) \
        .select("e").drop().iterate()

    # Redireciona arestas "produce" para a nova tabela
    g.V(id_table_merged).inE("produce").as_("e").outV() \
        .addE("produce").to(__.V(id_table)) \
        .select("e").drop().iterate()

    # Remove a tabela antiga
    g.V(id_table_merged).drop().iterate()

    logger.info(
        f"Tabela '{table_merged}' mesclada com sucesso na '{table_name}'"
        )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": f"Table '{table_merged}' merged into '{table_name}'."
        })
    }
