from substrait_producer import duckdb_producer, ibis_producer, isthmus_producer, datafusion_producer
from substrait_consumer import duckdb_engine, datafusion_engine, acero_engine


class CompoDB:
    def __init__(self, compiler, optimizer, engines):

        # Internal configuration
        self.ibis_comp = False
        self.duckdb_opt = False
        self.datafusion_opt = False
        self.calcite_opt = False
        self.duckdb_eng = False
        self.datafusion_eng = False
        self.acero_eng = False

        for comp in compiler:
            if comp == 'Ibis':
                self.ibis_comp = True
                self.ibis_compiler = ibis_producer.IbisProducer()

        for opt in optimizer:
            if opt == 'DuckDB':
                self.duckdb_opt = True
                self.duckdb_optimizer = duckdb_producer.DuckDBProducer()
            if opt == 'DataFusion':
                self.datafusion_opt = True
                self.datafusion_optimizer = datafusion_producer.DataFusionProducer()
            if opt == 'Calcite':
                self.calcite_opt = True
                self.calcite_optimizer = isthmus_producer.IsthmusProducer()

        for eng in engines:
            if eng == 'DuckDB':
                self.duckdb_eng = True
                self.duckdb_engine = duckdb_engine.DuckDBConsumer()
            if eng == 'DataFusion':
                self.datafusion_eng = True
                self.datafusion_engine = datafusion_engine.DataFusionConsumer()
            if eng == 'Acero':
                self.acero_eng = True
                self.acero_engine = acero_engine.AceroConsumer()



