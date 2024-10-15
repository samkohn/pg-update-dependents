import argparse
import dataclasses
import pprint
from typing import Literal

import networkx as nx

import sql


@dataclasses.dataclass(frozen=True)
class SQLObject:
    schema: str
    name: str
    kind: str
    oid: int

    def to_string(self) -> str:
        return self.schema + "." + self.name


@dataclasses.dataclass
class Action:
    action_type: Literal["drop"] | Literal["create"] | Literal["create-target"]
    obj: SQLObject

    def to_string(self) -> str:
        return self.action_type + " " + self.obj.to_string()


def raw_to_edge(raw_row: list) -> tuple[SQLObject, SQLObject]:
    dep_schema, dep_name, dep_oid, dep_kind, s_schema, s_name, s_oid, s_kind = raw_row
    return (
        SQLObject(s_schema, s_name, s_kind, s_oid),
        SQLObject(dep_schema, dep_name, dep_kind, dep_oid),
    )


def create_edge_list(raw_sql_results: list) -> list[tuple[SQLObject, SQLObject]]:
    return [raw_to_edge(row) for row in raw_sql_results]


def order(graph, target) -> list[Action]:
    subgraph = graph.subgraph(nx.descendants(graph, target))
    topo_order = list(nx.topological_sort(subgraph))
    drops = [Action("drop", node) for node in reversed(topo_order)]
    target_actions = [Action("drop", target), Action("create-target", target)]
    creates = [Action("create", node) for node in topo_order]
    return drops + target_actions + creates


def main(args):
    conn = sql.get_conn(args.user, args.host, args.db, args.port, args.password)
    raw = sql.get_sql_objects_raw(conn)
    edge_list = create_edge_list(raw)
    graph = nx.DiGraph(edge_list)
    test_val = SQLObject("public", "person_current_sr", "v", 667242)
    # pprint.pprint([action.to_string() for action in order(graph, test_val)])
    pprint.pprint(
        sql.retrieve_definitions(
            conn,
            [
                action.obj
                for action in order(graph, test_val)
                if "create" in action.action_type
            ][:5]
        )
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--user", required=True)
    parser.add_argument("-t", "--host", required=True)
    parser.add_argument("-d", "--db", required=True)
    parser.add_argument("-p", "--port", type=int, required=True)
    parser.add_argument("-w", "--password", required=True)
    args = parser.parse_args()
    main(args)
