from pprint import pprint

import pg8000.native

def safe_obj_name(schema, name):
    return pg8000.native.identifier(schema) + '.' + pg8000.native.identifier(name)

def get_conn(user, host, database, port, password) -> pg8000.native.Connection:
    return pg8000.native.Connection(user, host, database, port, password)

def get_sql_objects_raw(conn) -> list[list[str]]:
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


def definition_query(obj) -> str:
    if obj.kind == "m":
        return f"""SELECT
    'CREATE MATERIALIZED VIEW {obj.str_safe()}'
    || E'\\nAS' || definition
    || CASE
        WHEN indexes.indexdefs IS NULL THEN ''
        ELSE E'\\n\\n' || indexes.indexdefs
    END
    || E'\\n\\nALTER TABLE ' || mat.schemaname || '.' || mat.matviewname
    || ' OWNER TO ' || mat.matviewowner || E';\\n'
    || grants.grants
FROM
    pg_matviews mat
LEFT JOIN (
    SELECT
        schemaname,
        tablename,
        string_agg(indexdef || ';', E'\\n') AS indexdefs
    FROM
        pg_indexes
    GROUP BY
        schemaname,
        tablename
    ) as indexes
    ON
    mat.schemaname = indexes.schemaname
    AND
    mat.matviewname = indexes.tablename
CROSS JOIN (
    SELECT
        STRING_AGG('GRANT ' || privilege_type || ' ON TABLE ' || table_schema || '.' || table_name
            || ' TO ' || grantee || ';', E'\\n') as grants
    FROM
        information_schema.table_privileges
    WHERE
        table_schema= {pg8000.native.literal(obj.schema)}
        AND
        table_name = {pg8000.native.literal(obj.name)}
) as grants
WHERE
    mat.schemaname = {pg8000.native.literal(obj.schema)}
    AND
    mat.matviewname = {pg8000.native.literal(obj.name)};"""
    elif obj.kind == "v":
        return f"""SELECT
        'CREATE OR REPLACE VIEW {obj.str_safe()}'
        || E'\\nAS' || pg_get_viewdef({pg8000.native.literal(obj.oid)}, true)
        || E'\\n\\nALTER TABLE ' || schemaname || '.' || viewname
        || ' OWNER TO ' || viewowner || E';\\n'
        || grants.grants
FROM
    pg_views
CROSS JOIN (
    SELECT
        STRING_AGG('GRANT ' || privilege_type || ' ON TABLE ' || table_schema || '.' || table_name
            || ' TO ' || grantee || ';', E'\\n') as grants
    FROM
        information_schema.table_privileges
    WHERE
        table_schema= {pg8000.native.literal(obj.schema)}
        AND
        table_name = {pg8000.native.literal(obj.name)}
) as grants
WHERE
    schemaname = {pg8000.native.literal(obj.schema)}
    AND
    viewname = {pg8000.native.literal(obj.name)};"""
    else:
        raise ValueError(f"Invalid SQLObject kind: {obj!r}")


def retrieve_definitions(conn: pg8000.native.Connection, objs) -> list[str]:
    query = [definition_query(obj) for obj in objs]
    result = conn.run("\n".join(query))
    return result
