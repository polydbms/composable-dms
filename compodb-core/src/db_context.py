from typing import List
import os


class DBContext:

    csv_path: str = ""
    parquet_path: str = ""
    current_data_path: str = ""
    csv_tables: List[str] = []
    parquet_tables: List[str] = []
    compodb_instances = []
    benchmark = None

    '''
    Registers a table reference (csv/parquet) with the current CompoDB instances
    '''
    @classmethod
    def register_table(cls, table: str) -> None:
        try:
            for compodb in cls.compodb_instances:
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
    def register_tables(cls, input_format, benchmark=None):
        cls.benchmark = benchmark
        if benchmark:
            if input_format == "csv":
                cls.current_data_path = cls.csv_path / benchmark
            else:
                cls.current_data_path = cls.parquet_path / benchmark
        else:
            if input_format == "csv":
                cls.current_data_path = cls.csv_path
            else:
                cls.current_data_path = cls.parquet_path

        for table in os.listdir(cls.current_data_path):
            if table.endswith(".parquet") or table.endswith(".csv"):
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

