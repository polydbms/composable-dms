from src.substrait_producer.parser import Parser
from src.substrait_producer.optimizer import Optimizer
import duckdb
import json
from src.errors import ProductionError


class DuckDBProducer(Parser, Optimizer):

    def __init__(self):
        self.db_connection = duckdb.connect()
        self.db_connection.execute("INSTALL substrait")
        self.db_connection.execute("LOAD substrait")


    def to_substrait(self, native_query: str) -> str:
        try:
            json_plan = self.db_connection.get_substrait_json(native_query).fetchone()[0]
            python_json = json.loads(json_plan)
            return json.dumps(python_json, indent=2)

        except Exception as e:

            # Reconnect after Exception
            self.db_connection.close()
            self.db_connection = duckdb.connect()
            self.db_connection.execute("INSTALL substrait")
            self.db_connection.execute("LOAD substrait")
            self.db_connection.execute(f"CALL dbgen(sf=0.1)")

            raise ProductionError(repr(e))


    def optimize_substrait(self, substrait_query: str) -> str:
        return substrait_query


    def register_table(self, table: str) -> None:
        # TODO: Register other data than tpc-h
        self.db_connection.execute(f"CALL dbgen(sf=0.1)")


    def get_name(self) -> str:
        return "DuckDB"