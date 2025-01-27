from src.substrait_producer.duckdb_producer import DuckDBProducer
from src.substrait_producer.datafusion_producer import DataFusionProducer
from src.substrait_producer.ibis_producer import IbisProducer
from src.substrait_producer.isthmus_producer import IsthmusProducer
from src.substrait_consumer.duckdb_engine import DuckDBConsumer
from src.substrait_consumer.datafusion_engine import DataFusionConsumer
from src.substrait_consumer.acero_engine import AceroConsumer
from typing import Union
from typing import List


class CompoDB:

    _instances: List["CompoDB"] = []

    def __init__(self, producer, consumer):
        if producer == 'calcite':
            self.producer = IsthmusProducer()
            self.producer.compodb = self
        elif producer == 'duckdb':
            self.producer = DuckDBProducer()
            self.producer.compodb = self
        elif producer == 'ibis':
            self.producer = IbisProducer()
            self.producer.compodb = self
        elif producer == 'datafusion':
            self.producer = DataFusionProducer()
            self.producer.compodb = self
        else:
            raise ValueError('Invalid producer type')

        if consumer == 'duckdb':
            self.consumer = DuckDBConsumer()
            self.consumer.compodb = self
        elif consumer == 'datafusion':
            self.consumer = DataFusionConsumer()
            self.consumer.compodb = self
        elif consumer == 'acero':
            self.consumer = AceroConsumer()
            self.consumer.compodb = self
        else:
            raise ValueError('Invalid consumer type')

        CompoDB._instances.append(self)


    @classmethod
    def get_compodb_instances(cls) -> List["CompoDB"]:
        return CompoDB._instances


