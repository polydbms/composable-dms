from src.substrait_producer.parser import Parser
from src.substrait_producer.optimizer import Optimizer
from src.db_context import DBContext
import duckdb
import json
import os
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

            for table in os.listdir(f"{DBContext.current_data_path}"):
                if table.endswith(".parquet") or table.endswith(".csv"):
                    self.register_table(table)

            raise ProductionError(repr(e))


    def optimize_substrait(self, substrait_query: str) -> str:
        return substrait_query


    def register_table(self, table: str) -> None:
        view_name = table.split('.')[0]

        self.db_connection.execute(f"DROP VIEW IF EXISTS {view_name}")
        if table.endswith(".parquet"):
            self.db_connection.execute(
                f"CREATE VIEW {view_name} AS SELECT * FROM read_parquet('{DBContext.current_data_path}/{table}')"
            )
        else:
            self.db_connection.execute(
                f"CREATE VIEW {view_name} AS SELECT * FROM read_csv('{DBContext.current_data_path}/{table}')"
            )


    def get_name(self) -> str:
        return "DuckDB"

    def print_db(self):
        self.db_connection.sql("SELECT * FROM information_schema.tables").show()