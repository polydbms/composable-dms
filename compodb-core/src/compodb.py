from src.substrait_producer.duckdb_producer import DuckDBProducer
from src.substrait_producer.datafusion_producer import DataFusionProducer
from src.substrait_producer.ibis_producer import IbisProducer
from src.substrait_producer.isthmus_producer import IsthmusProducer
from src.substrait_consumer.duckdb_engine import DuckDBEngine
from src.substrait_consumer.datafusion_engine import DataFusionEngine
from src.substrait_consumer.acero_engine import AceroEngine
from src.substrait_producer.optimizer import Optimizer
from typing import Union
from typing import List


class CompoDB:

    _instances: List["CompoDB"] = []

    def __init__(self, parser: str, optimizer: List[str], execution_engine: str):

        # Parser selection
        if parser == 'Calcite':
            self.parser = IsthmusProducer()
            self.parser.compodb = self
        elif parser == 'DuckDB':
            self.parser = DuckDBProducer()
            self.parser.compodb = self
        elif parser == 'Ibis':
            self.parser = IbisProducer()
            self.parser.compodb = self
        elif parser == 'DataFusion':
            self.parser = DataFusionProducer()
            self.parser.compodb = self
        else:
            raise ValueError('Invalid parser type')

        # Optimizer selection
        self.optimizer_names: List[str] = optimizer
        self.optimizer: List[Optimizer] = []
        for opt in optimizer:
            if opt == 'Calcite':
                opt_obj = IsthmusProducer()
                opt_obj.compodb = self
                self.optimizer.append(opt_obj)
            elif opt == 'DuckDB':
                opt_obj = DuckDBProducer()
                opt_obj.compodb = self
                self.optimizer.append(opt_obj)
            elif opt == 'DataFusion':
                opt_obj = DataFusionProducer()
                opt_obj.compodb = self
                self.optimizer.append(opt_obj)
            else:
                raise ValueError('Invalid optimizer type')

        # Execution Engine selection
        if execution_engine == 'DuckDB':
            self.execution_engine = DuckDBEngine()
            self.execution_engine.compodb = self
        elif execution_engine == 'DataFusion':
            self.execution_engine = DataFusionEngine()
            self.execution_engine.compodb = self
        elif execution_engine == 'Acero':
            self.execution_engine = AceroEngine()
            self.execution_engine.compodb = self
        else:
            raise ValueError('Invalid engine type')

        CompoDB._instances.append(self)


    def get_optimizer_names(self) -> List[str]:
        return self.optimizer_names

    @classmethod
    def get_compodb_instances(cls) -> List["CompoDB"]:
        return CompoDB._instances

    @classmethod
    def clear_instances(cls) -> None:
        cls._instances.clear()


