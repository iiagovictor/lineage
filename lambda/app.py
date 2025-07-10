import json
import os
import logging
from utils.utils import create_connection, bulk_load_data, merge_table_data
from utils.data_reader import \
    get_subgraph_of_the_table, \
    get_subgraph_of_the_job

logger = logging.getLogger()
logger.setLevel(logging.INFO)

NEPTUNE_PORT = os.getenv("NEPTUNE_PORT")
NEPTUNE_ENDPOINT = os.getenv("NEPTUNE_ENDPOINT_WRITER")


def lambda_handler(event, context):

    logger.info(f"Received event: {event}")
    parameters = event.get("queryStringParameters", {})
    logger.info(f"Parameters: {parameters}")
    load_mock_str = parameters.get("load_mock", "false")
    load_mock = str(load_mock_str).lower() == "true"
    logger.info(f"Load mock data: {load_mock}")
    node_type = parameters.get("node_type")
    node_name = parameters.get("node_name")
    merge_table_str = parameters.get("merge_table", "false")
    merge_table = str(merge_table_str).lower() == "true"
    logger.info(f"Merge table data: {merge_table}")
    table_merged = parameters.get("table_merged", None)

    gremlin_utils, conn = create_connection(
        neptune_endpoint=NEPTUNE_ENDPOINT,
        neptune_port=NEPTUNE_PORT
        )
    g = gremlin_utils.traversal_source(connection=conn)
    bulk_load_data(g=g, load_mock=load_mock)

    if merge_table:
        if not node_name or not table_merged:
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "message": "Please provide either 'node_name' and \
                        'table_merged' for merging."
                }),
            }

        if node_type != "TABLE":
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "message": "Invalid node type for merging. Use 'TABLE'."
                }),
            }

        response = merge_table_data(
            g=g,
            tableId=node_name,
            table_merged=table_merged
            )
        return response
    else:
        if node_type == "TABLE":
            response = get_subgraph_of_the_table(
                g=g,
                tableId=node_name,
                level=50
                )
        elif node_type == "JOB":
            response = get_subgraph_of_the_job(
                g=g,
                jobId=node_name,
                level=50
                )
        else:
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "message": "Invalid node type. Use 'TABLE' or 'JOB'."
                }),
            }
    conn.close()
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Successfully retrieved subgraph.",
            "data": response,
        }),
    }
