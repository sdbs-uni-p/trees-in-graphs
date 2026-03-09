# SPDX-License-Identifier: GPL-3.0-only

import os
import time
from abc import abstractmethod, ABC
import re

try:
    import psycopg2
except ImportError:
    psycopg2 = None

try:
    import kuzu
except ImportError:
    kuzu = None

try:
    from neo4j import GraphDatabase
except ImportError:
    GraphDatabase = None

class Executor(ABC):
    @abstractmethod
    def execute_query(self, query_string):
        pass

    @abstractmethod
    def update_db(self, new_dbname):
        pass

    @abstractmethod
    def collect_query_plan(self, query_string):
        pass

    @abstractmethod
    def collect_id(self, annotation: str | int, node_type: str, graph_name : str):
        pass

    @abstractmethod
    def create_ir_index(self, node_type: str, graph_name: str):
        pass

    @abstractmethod
    def create_s_index(self, node_type: str, graph_name: str):
        pass

    @abstractmethod
    def drop_ir_index(self, node_type: str, graph_name: str):
        pass

    @abstractmethod
    def drop_s_index(self, node_type: str, graph_name: str):
        pass

    @abstractmethod
    def execute_command(self, command_string: str):
        """Execute a command that doesn't return results (SET, CREATE, etc.)"""
        pass

    def set_graph(self, graph_name: str):
        """Switch to a different graph. No-op for backends where graph
        selection happens inside the query (e.g. AGE)."""
        pass


COST_RE = re.compile(
    r"cost=(\d+(?:\.\d+)?)\.\.(\d+(?:\.\d+)?)"
)

def extract_total_cost(explain_line: str) -> float:
    match = COST_RE.search(explain_line)
    if not match:
        raise ValueError(f"No cost found in line: {explain_line}")
    return float(match.group(2))

class ApacheExecutor(Executor):
    # def __init__(self, dbname, user='danielarturi', password='', host='localhost', port=5432):
    def __init__(self, dbname, user : str, password : str, host : str, port : int):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port

        self.conn = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )
        self.conn.autocommit = True

        self.cursor = self.conn.cursor()

        # Load AGE extension
        self.cursor.execute("CREATE EXTENSION IF NOT EXISTS age;")
        self.cursor.execute("LOAD 'age';")
        self.cursor.execute('SET search_path = ag_catalog, "$user", public;')

    def execute_query(self, query_string : str):
        start = time.perf_counter()
        self.cursor.execute(query_string)
        result = self.cursor.fetchall()
        end = time.perf_counter()

        return (end - start) * 1000, result

    def update_db(self, new_dbname : str):
        old_dbname = self.dbname
        self.dbname = new_dbname

        self.conn = psycopg2.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )
        self.conn.autocommit = True

        self.cursor = self.conn.cursor()

        # Load AGE extension
        self.cursor.execute("LOAD 'age';")
        self.cursor.execute('SET search_path = ag_catalog, "$user", public;')

        print(f"updated connection from {old_dbname} to {new_dbname}")

    def collect_query_plan(self, query_string : str):
        _, plan = self.execute_query(f"EXPLAIN ANALYZE {query_string}")
        time_elapsed, query_results = self.execute_query(query_string)

        est_cost = extract_total_cost(plan[0][0])

        return time_elapsed, plan, est_cost, query_results

    def collect_id(self, annotation: str | int, node_type: str, graph_name : str) -> int:
        if type(annotation) is str:
            annotation_type = "string_id"
            annotation_value = f"'{annotation}'"
        else:
            annotation_type = "integer_id"
            annotation_value = str(annotation)

        query_string = f"""SELECT id::bigint
FROM cypher('{graph_name}', $$
    MATCH (n:{node_type})
    WHERE n.{annotation_type} = {annotation_value}
    RETURN id(n)
$$) AS (id agtype);"""

        _, result = self.execute_query(query_string)

        return result[0][0]

    def create_ir_index(self, node_type: str, graph_name: str):
        # Direct SQL
#        ir_index_command = f"""CREATE UNIQUE INDEX treenode_integer_id_unique_idx
#ON {graph_name}."{node_type}"
#USING BTREE (
#  agtype_access_operator(VARIADIC ARRAY[properties, '"integer_id"'::agtype])
#);
#"""
#        self.execute_query(ir_index_command)

#        # Through cypher
#        ir_index_command = f"""SELECT *
#FROM cypher('{graph_name}', $$
#    CREATE INDEX IF NOT EXISTS treenode_integer_id_idx
#    ON :{node_type}(integer_id);
#$$) AS (ignored agtype);"""
        ir_index_command = f"""CREATE INDEX IF NOT EXISTS treenode_integer_id_btree
ON {graph_name}."{node_type}"
USING BTREE (
  agtype_access_operator(VARIADIC ARRAY[properties, '"integer_id"'::agtype])
);
"""

        self.execute_command(ir_index_command)

        self.execute_command(f"ANALYZE {graph_name}.\"{node_type}\"")

    def create_id_index(self, node_type: str, graph_name: str):
        id_index_command = f"""CREATE INDEX IF NOT EXISTS id_btree
ON {graph_name}."{node_type}"
USING BTREE (
  agtype_access_operator(VARIADIC ARRAY[properties, '"__id__"'::agtype])
);
    """

        self.execute_command(id_index_command)

        self.execute_command(f"ANALYZE {graph_name}.\"{node_type}\"")

    def create_s_index(self, node_type: str, graph_name: str):
#        s_setup = "CREATE EXTENSION IF NOT EXISTS pg_trgm;"

#        self.execute_query(s_setup)

        # Trigram
#        s_index_command = f"""CREATE INDEX treenode_string_id_trgm_idx
#ON {graph_name}."{node_type}"
#USING GIN (
#  (agtype_access_operator(VARIADIC ARRAY[properties, '"string_id"'::agtype])) gin_trgm_ops
#);"""

        # SQL w/ Textops
#        s_index_command = f"""CREATE INDEX treenode_string_id_prefix_idx
#ON {graph_name}."{node_type}"
#USING BTREE (
# (agtype_access_operator(VARIADIC ARRAY[properties, '"string_id"'::agtype])) text_pattern_ops
#);"""
#        self.execute_query(s_index_command)
        # Cypher
#        s_index_command = f"""SELECT *
#FROM cypher('{graph_name}', $$
#    CREATE INDEX treenode_string_id_idx
#    ON :{node_type}(string_id);
#$$) AS (ignored agtype);"""

        s_index_command = f"""CREATE INDEX IF NOT EXISTS treenode_string_id_prefix_idx
ON {graph_name}."{node_type}"
USING BTREE (
  (agtype_access_operator(VARIADIC ARRAY[properties, '"string_id"'::agtype])::text) text_pattern_ops
);
"""

        self.execute_command(s_index_command)
        self.execute_command(f"ANALYZE {graph_name}.\"{node_type}\"")

    def drop_id_index(self, node_type: str, graph_name: str):
        self.execute_command(
            f'DROP INDEX IF EXISTS {graph_name}.id_btree;'
        )
        self.execute_command(f"ANALYZE {graph_name}.\"{node_type}\"")

    def drop_ir_index(self, node_type: str, graph_name: str):
        self.execute_command(
            f'DROP INDEX IF EXISTS {graph_name}.treenode_integer_id_unique_idx;'
        )

        self.execute_command(
            f'DROP INDEX IF EXISTS {graph_name}.treenode_integer_id_idx;'
        )
        self.execute_command(f"ANALYZE {graph_name}.\"{node_type}\"")

    def drop_s_index(self, node_type: str, graph_name: str):
        self.execute_command(
            f'DROP INDEX IF EXISTS {graph_name}.treenode_string_id_prefix_idx;'
        )
        #self.execute_command(
        #    f'DROP INDEX IF EXISTS {graph_name}.treenode_string_id_idx;'
        #)
        self.execute_command(f"ANALYZE {graph_name}.\"{node_type}\"")

    def execute_command(self, command_string: str):
        """Execute a command that doesn't return results (SET, CREATE, etc.)"""
        self.cursor.execute(command_string)

class KuzuExecutor(Executor):
    def __init__(self, db_base_path: str):
        if kuzu is None:
            raise ImportError("kuzu package is required for KuzuExecutor")
        self.db_base_path = db_base_path
        self.db = None
        self.conn = None

    def set_graph(self, graph_name: str):
        db_path = os.path.join(self.db_base_path, graph_name)
        self.db = kuzu.Database(db_path)
        self.conn = kuzu.Connection(self.db)

    def execute_query(self, query_string: str):
        executable_query = self._strip_leading_sql_comments(query_string)
        if not executable_query:
            executable_query = query_string

        start = time.perf_counter()
        result = self.conn.execute(executable_query)
        end = time.perf_counter()
        rows = []
        while result.has_next():
            rows.append(result.get_next())
        return (end - start) * 1000, rows

    def update_db(self, new_dbname: str):
        self.set_graph(new_dbname)

    @staticmethod
    def _strip_leading_sql_comments(query_string: str) -> str:
        """Remove leading comments so PROFILE sees a statement token first."""
        q = query_string.lstrip()
        while True:
            line_comment = re.match(r"^--[^\n]*(?:\n|$)", q)
            if line_comment:
                q = q[line_comment.end():].lstrip()
                continue

            block_comment = re.match(r"^/\*.*?\*/", q, flags=re.DOTALL)
            if block_comment:
                q = q[block_comment.end():].lstrip()
                continue

            break
        return q

    def collect_query_plan(self, query_string: str):
        profile_query = self._strip_leading_sql_comments(query_string)

        # PROFILE executes the query and returns the plan with timing info
        profile_result = self.conn.execute(f"PROFILE {profile_query}")
        plan = profile_result.get_as_df().to_string(index=False).strip()

        # Execute again for wall-clock timing
        time_elapsed, query_results = self.execute_query(query_string)

        # Kuzu does not have PostgreSQL-style cost estimates
        est_cost = 0.0

        return time_elapsed, plan, est_cost, query_results

    def collect_id(self, annotation: str | int, node_type: str, graph_name: str):
        # In Kuzu the primary key IS the annotation value itself
        # (string_id for dewey, integer_id for prepost, id for plain)
        return annotation

    def create_ir_index(self, node_type: str, graph_name: str):
        # integer_id is the primary key in prepost databases, already indexed
        pass

    def create_s_index(self, node_type: str, graph_name: str):
        # string_id is the primary key in dewey databases, already indexed
        pass

    def drop_ir_index(self, node_type: str, graph_name: str):
        pass

    def drop_s_index(self, node_type: str, graph_name: str):
        pass

    def execute_command(self, command_string: str):
        """Execute a command. Silently ignores PostgreSQL-specific commands
        (e.g. SET enable_seqscan) that have no Kuzu equivalent."""
        try:
            self.conn.execute(command_string)
        except Exception:
            pass

class Neo4jExecutor(Executor):
    def __init__(self, uri: str, user: str, password: str):
        if GraphDatabase is None:
            raise ImportError("neo4j package is required for Neo4jExecutor")
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.current_db = None

    @staticmethod
    def _serialize(val):
        """Convert neo4j driver types (Node, Relationship, Path) to
        JSON-serializable Python objects."""
        if hasattr(val, 'items'):
            # Node or dict-like: return its properties
            return dict(val.items()) if hasattr(val, 'labels') else val
        if hasattr(val, 'nodes'):
            # Path: return list of node property dicts
            return [dict(n.items()) for n in val.nodes]
        if isinstance(val, list):
            return [Neo4jExecutor._serialize(v) for v in val]
        return val

    def set_graph(self, graph_name: str):
        """Switch to a Neo4j database. Converts underscores to dots
        since Neo4j database names only allow [a-z0-9.-]."""
        self.current_db = graph_name.replace("_", ".")

    def execute_query(self, query_string: str):
        executable_query = self._strip_leading_sql_comments(query_string)
        if not executable_query:
            executable_query = query_string

        with self.driver.session(database=self.current_db) as session:
            start = time.perf_counter()
            result = session.run(executable_query)
            records = [
                [self._serialize(v) for v in record.values()]
                for record in result
            ]
            end = time.perf_counter()
            return (end - start) * 1000, records

    def update_db(self, new_dbname: str):
        self.set_graph(new_dbname)

    @staticmethod
    def _strip_leading_sql_comments(query_string: str) -> str:
        """Remove leading comments so PROFILE sees a statement token first."""
        q = query_string.lstrip()
        while True:
            line_comment = re.match(r"^--[^\n]*(?:\n|$)", q)
            if line_comment:
                q = q[line_comment.end():].lstrip()
                continue

            block_comment = re.match(r"^/\*.*?\*/", q, flags=re.DOTALL)
            if block_comment:
                q = q[block_comment.end():].lstrip()
                continue

            break
        return q

    def collect_query_plan(self, query_string: str):
        profile_query = self._strip_leading_sql_comments(query_string)

        # PROFILE executes the query and returns the plan with timing info
        with self.driver.session(database=self.current_db) as session:
            result = session.run(f"PROFILE {profile_query}")
            list(result)  # consume records
            summary = result.consume()
            plan = str(summary.profile) if summary.profile else ""

        # Execute again for wall-clock timing and results
        time_elapsed, query_results = self.execute_query(query_string)

        # Neo4j does not have PostgreSQL-style cost estimates
        est_cost = 0.0

        return time_elapsed, plan, est_cost, query_results

    def collect_id(self, annotation: str | int, node_type: str, graph_name: str):
        # Neo4j queries use property values directly (id, string_id, integer_id)
        return annotation

    def create_ir_index(self, node_type: str, graph_name: str):
        db_name = graph_name.replace("_", ".")
        with self.driver.session(database=db_name) as session:
            session.run(
                f"CREATE INDEX treenode_integer_id_idx IF NOT EXISTS "
                f"FOR (n:{node_type}) ON (n.integer_id)"
            )

    def create_s_index(self, node_type: str, graph_name: str):
        db_name = graph_name.replace("_", ".")
        with self.driver.session(database=db_name) as session:
            session.run(
                f"CREATE INDEX treenode_string_id_idx IF NOT EXISTS "
                f"FOR (n:{node_type}) ON (n.string_id)"
            )

    def drop_ir_index(self, node_type: str, graph_name: str):
        db_name = graph_name.replace("_", ".")
        with self.driver.session(database=db_name) as session:
            session.run("DROP INDEX treenode_integer_id_idx IF EXISTS")

    def drop_s_index(self, node_type: str, graph_name: str):
        db_name = graph_name.replace("_", ".")
        with self.driver.session(database=db_name) as session:
            session.run("DROP INDEX treenode_string_id_idx IF EXISTS")

    def execute_command(self, command_string: str):
        """Execute a command. Silently ignores PostgreSQL-specific commands
        (e.g. SET enable_seqscan) that have no Neo4j equivalent."""
        try:
            with self.driver.session(database=self.current_db) as session:
                session.run(command_string).consume()
        except Exception:
            pass
