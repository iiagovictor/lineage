from gremlin_python.process.traversal import T
from gremlin_python.process.graph_traversal import __, coalesce


def project_job(transversal):
    return transversal.project(
            "id",
            "label",
            "name",
            "account_id",
            "sigla",
            "tool",
            "status",
            "dt_update"
            ) \
        .by(T.id) \
        .by(T.label) \
        .by(coalesce(
            __.values("name"), __.constant("N/A")
        )) \
        .by(coalesce(
            __.values("account_id"), __.constant("N/A")
        )) \
        .by(coalesce(
            __.values("sigla"), __.constant("N/A")
        )) \
        .by(coalesce(
            __.values("tool"), __.constant("N/A")
        )) \
        .by(coalesce(
            __.values("status"), __.constant("N/A")
        )) \
        .by(coalesce(
            __.values("dt_update"), __.constant("N/A")
        ))


def project_table(transversal):
    return transversal.project(
            "id",
            "label",
            "table_name",
            "db_name",
            "location_path",
            "status",
            "dt_update"
            ) \
        .by(T.id) \
        .by(T.label) \
        .by(coalesce(
            __.values("table_name"), __.constant("N/A")
        )) \
        .by(coalesce(
            __.values("db_name"), __.constant("N/A")
        )) \
        .by(coalesce(
            __.values("location_path"), __.constant("N/A")
        )) \
        .by(coalesce(
            __.values("status"), __.constant("N/A")
        )) \
        .by(coalesce(
            __.values("dt_update"), __.constant("N/A")
        ))
