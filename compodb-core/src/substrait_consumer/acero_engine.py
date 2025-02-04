import os
import string
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
import pyarrow.substrait as substrait
from pyarrow import Table
import time
from typing import Optional
from src.substrait_consumer.execution_engine import ExecutionEngine
from src.errors import ConsumptionError


class AceroEngine(ExecutionEngine):

    def __init__(self):
        self.tables = {}
        self.table_provider = lambda names, schema: self.tables[names[0].lower()]


    def register_table(self, table: str):
        table_name = Path(table).stem
        table_name = table_name.translate(
            str.maketrans("", "", string.punctuation)
        )
        if table.endswith(".parquet"):
            self.tables[table_name] = pq.read_table(f"/app/data/parquet/{table}")
        else:
            self.tables[table_name] = csv.read_csv( f"/app/data/csv/{table}")


    def run_substrait(self, substrait_query) -> Optional[Table]:
        try:
            encoded_substrait = substrait_query.encode()
            substrait_query = pa._substrait._parse_json_plan(encoded_substrait)
            reader = substrait.run_query(
                substrait_query, table_provider=self.table_provider
            )
            return reader.read_all()

        except Exception as e:
            raise ConsumptionError(repr(e))


    def get_name(self) -> str:
        return "Acero"