import argparse
import dataclasses
import getpass
import pprint
from typing import Literal

import networkx as nx

import sql


@dataclasses.dataclass(frozen=True)
class SQLObject:
    schema: str
    name: str
    kind: Literal["v"] | Literal["m"]
    oid: int

    def __str__(self) -> str:
        return self.schema + "." + self.name


@dataclasses.dataclass
class Action:
    action_type: Literal["drop"] | Literal["create"]
    obj: SQLObject
    definition: str | None = None

    def __str__(self) -> str:
        return self.action_type + " " + str(self.obj)

    def to_sql(self) -> str:
        if self.obj.kind == "v":
            kind_str = "VIEW"
            create_str = f"CREATE OR REPLACE VIEW {self.obj}\nAS"
        elif self.obj.kind == "m":
            kind_str = "MATERIALIZED VIEW"
            create_str = f"CREATE MATERIALIZED VIEW {self.obj}\nAS"
        if self.action_type == "drop":
            return f"DROP {kind_str} {self.obj}"
        elif self.definition is None:
            raise ValueError(f"Unknown CREATE definition for {self}")
        elif self.action_type == "create":
            return create_str + " " + self.definition


def raw_to_edge(raw_row: list) -> tuple[SQLObject, SQLObject]:
    dep_schema, dep_name, dep_oid, dep_kind, s_schema, s_name, s_oid, s_kind = raw_row
    return (
        SQLObject(s_schema, s_name, s_kind, s_oid),
        SQLObject(dep_schema, dep_name, dep_kind, dep_oid),
    )


def create_edge_list(raw_sql_results: list) -> list[tuple[SQLObject, SQLObject]]:
    return [raw_to_edge(row) for row in raw_sql_results]


def order(graph, target) -> dict[str, list[Action]]:
    subgraph = graph.subgraph(nx.descendants(graph, target))
    topo_order = list(nx.topological_sort(subgraph))
    drops = [Action("drop", node) for node in reversed(topo_order)]
    target_actions = [Action("drop", target), Action("create", target)]
    creates = [Action("create", node) for node in topo_order]
    return {
        "drops": drops,
        "target_actions": target_actions,
        "creates": creates,
    }


def main(args):
    conn = sql.get_conn(args.user, args.host, args.db, args.port, args.password)
    raw = sql.get_sql_objects_raw(conn)
    edge_list = create_edge_list(raw)
    all_objects = set(e[0] for e in edge_list) | set(e[1] for e in edge_list)
    target_schema, target_name = args.target.split(".")
    target_obj = [
        obj
        for obj in all_objects
        if obj.schema == target_schema and obj.name == target_name
    ]
    if len(target_obj) == 1:
        target_obj = target_obj[0]
    else:
        raise ValueError(
            f"There must be exactly one db object matching the target {args.target}, "
            f"but I found {len(target_obj)}: {target_obj}"
        )
    graph = nx.DiGraph(edge_list)
    steps = order(graph, target_obj)
    all_steps = steps["drops"] + steps["target_actions"] + steps["creates"]
    reverse_steps = list(reversed(all_steps))
    definitions = sql.retrieve_definitions(
        conn, [action.obj for action in reverse_steps if action.action_type == "create"]
    )
    for step, definition in zip(reverse_steps, definitions):
        step.definition = "\n".join(definition).strip()
    print(reverse_steps[0].definition[:30])
    with open(args.outfile, "w") as f:
        f.write(
            f"""-- This file was auto-generated by pg-update-dependents.
-- First, all dependent relations are dropped.
-- The target relation `{test_val!s}` is preceded by the comment

    -- TARGET RELATION --

-- After the target relation, all the CREATE statements
-- for dependent relations are included.
-- The target's definition can be modified, and any dependent relations
-- can also be modified. As long as the schema is not modified between
-- the generation and execution of this file, it is guaranteed to
-- contain all dependent views, materialized views, and functions.

    -- DROP DEPENDENT RELATIONS --

"""
        )
        f.write("\n\n".join(step.to_sql() for step in steps["drops"]))
        f.write("\n\n    -- TARGET RELATION --\n\n")
        f.write("\n\n".join(step.to_sql() for step in steps["target_actions"]))
        f.write("\n\n    -- CREATE DEPENDENT RELATIONS --\n\n")
        f.write("\n\n".join(step.to_sql() for step in steps["creates"]))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--user", required=True)
    parser.add_argument("-t", "--host", required=True)
    parser.add_argument("-d", "--db", required=True)
    parser.add_argument("-p", "--port", type=int, required=True)
    parser.add_argument(
        "-w", "--password", help="Will prompt for password if not provided"
    )
    parser.add_argument("-o", "--outfile", required=True)
    parser.add_argument("target")
    args = parser.parse_args()
    if not args.password:
        args.password = getpass.getpass()
    main(args)
