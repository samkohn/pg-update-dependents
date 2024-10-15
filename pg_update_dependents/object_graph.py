import argparse
import dataclasses
import pprint
import typing

import pg8000.native
import networkx as nx


@dataclasses.dataclass(frozen=True)
class SQLObject:
    schema: str
    name: str
    kind: str
    oid: int

    def to_string(self) -> str:
        return self.schema + "." + self.name


def get_sql_objects_raw(user, host, database, port, password) -> list[list[str]]:
    conn = pg8000.native.Connection(user, host, database, port, password)
    raw_sql_results = conn.run(
        """WITH RECURSIVE view_deps AS (
SELECT DISTINCT dependent_ns.nspname as dependent_schema
, dependent_view.relname as dependent_view
, dependent_view.oid as dependent_oid
, dependent_view.relkind as dependent_relkind
, source_ns.nspname as source_schema
, source_table.relname as source_table
, source_table.oid as source_oid
, source_table.relkind as source_relkind
FROM pg_depend
JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
JOIN pg_class as dependent_view ON pg_rewrite.ev_class = dependent_view.oid
JOIN pg_class as source_table ON pg_depend.refobjid = source_table.oid
JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent_view.relnamespace
JOIN pg_namespace source_ns ON source_ns.oid = source_table.relnamespace
WHERE NOT (dependent_ns.nspname = source_ns.nspname AND dependent_view.relname = source_table.relname)
UNION
SELECT DISTINCT dependent_ns.nspname as dependent_schema
, dependent_view.relname as dependent_view
, dependent_view.oid as dependent_oid
, dependent_view.relkind as dependent_relkind
, source_ns.nspname as source_schema
, source_table.relname as source_table
, source_table.oid as source_oid
, source_table.relkind as source_relkind
FROM pg_depend
JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
JOIN pg_class as dependent_view ON pg_rewrite.ev_class = dependent_view.oid
JOIN pg_class as source_table ON pg_depend.refobjid = source_table.oid
JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent_view.relnamespace
JOIN pg_namespace source_ns ON source_ns.oid = source_table.relnamespace
INNER JOIN view_deps vd
    ON vd.dependent_schema = source_ns.nspname
    AND vd.dependent_view = source_table.relname
    AND NOT (dependent_ns.nspname = vd.dependent_schema AND dependent_view.relname = vd.dependent_view)
)
SELECT *
FROM view_deps
ORDER BY source_schema, source_table;
"""
    )

    return raw_sql_results


def raw_to_edge(raw_row: list) -> tuple[SQLObject, SQLObject]:
    dep_schema, dep_name, dep_oid, dep_kind, s_schema, s_name, s_oid, s_kind = raw_row
    return (
        SQLObject(s_schema, s_name, s_kind, s_oid),
        SQLObject(dep_schema, dep_name, dep_kind, dep_oid),
    )


def create_edge_list(raw_sql_results: list) -> list[tuple[SQLObject, SQLObject]]:
    return [raw_to_edge(row) for row in raw_sql_results]

def main(args):
    raw = get_sql_objects_raw(args.user, args.host, args.db, args.port, args.password)
    edge_list = create_edge_list(raw)
    graph = nx.DiGraph(edge_list)
    test_val = SQLObject('public', 'person_current_sr', 'v', 667242)
    pprint.pprint(dict(graph[test_val]))
    pprint.pprint(list(graph.successors(test_val)))
    pprint.pprint(list(graph.predecessors(test_val)))
    print(graph.in_degree(test_val))
    print(graph.out_degree(test_val))
    print(nx.is_directed_acyclic_graph(graph))
    subgraph = graph.subgraph(nx.descendants(graph, test_val))
    pprint.pprint(list(nx.topological_sort(subgraph)))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--user", required=True)
    parser.add_argument("-t", "--host", required=True)
    parser.add_argument("-d", "--db", required=True)
    parser.add_argument("-p", "--port", type=int, required=True)
    parser.add_argument("-w", "--password", required=True)
    args = parser.parse_args()
    main(args)
