from src.substrait_producer.datafusion_producer import DataFusionProducer
from src.substrait_producer.duckdb_producer import DuckDBProducer
from src.substrait_producer.ibis_producer import IbisProducer
from src.substrait_producer.isthmus_producer import IsthmusProducer
from src.compodb import CompoDB
from typing import List
import os


class DBContext:

    csv_path: str = ""
    parquet_path: str = ""
    csv_tables: List[str] = []
    parquet_tables: List[str] = []
    input_format: str = None

    '''
    Registers a table reference (csv/parquet) with the current CompoDB instances
    '''
    @classmethod
    def register_table(cls, table: str) -> None:
        try:
            for compodb in CompoDB.get_compodb_instances():
                compodb.parser.register_table(table)
                if compodb.optimizer:
                    for opt in compodb.optimizer:
                        opt.register_table(table)
                compodb.execution_engine.register_table(table)
        except Exception as e:
            raise e

        if table.endswith(".parquet"):
            cls.parquet_tables.append(table)
        else:
            cls.csv_tables.append(table)

    @classmethod
    def register_tables(cls, input_format):
        if input_format == "csv":
            for table in os.listdir(cls.csv_path):
                if table.endswith(".csv"):
                    # TODO: write_isthmus_schema()
                    cls.register_table(table)
        else:
            for table in os.listdir(cls.parquet_path):
                if table.endswith(".parquet"):
                    # TODO: write_isthmus_schema()
                    cls.register_table(table)


    @classmethod
    def deregister_tables(cls):
        # TODO
        pass

    @classmethod
    def get_parquet_tables(cls) -> List[str]:
        return cls.parquet_tables

    @classmethod
    def get_csv_tables(cls) -> List[str]:
        return cls.csv_tables

