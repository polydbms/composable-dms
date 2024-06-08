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
        print(" ")
        for comp in compiler:
            if comp == 'Ibis':
                self.ibis_comp = True
                self.ibis_compiler = ibis_producer.IbisProducer()
                print("> Ibis compiler created ..\n>")

        for opt in optimizer:
            if opt == 'DuckDB':
                self.duckdb_opt = True
                self.duckdb_optimizer = duckdb_producer.DuckDBProducer()
                print("> DuckDB optimizer created ..\n>")
            if opt == 'DataFusion':
                self.datafusion_opt = True
                self.datafusion_optimizer = datafusion_producer.DataFusionProducer()
                print("> DataFusion optimizer created ..\n>")
            if opt == 'Calcite':
                self.calcite_opt = True
                self.calcite_optimizer = isthmus_producer.IsthmusProducer()
                print("> Calcite optimizer created ..\n>")

        for eng in engines:
            if eng == 'DuckDB':
                self.duckdb_eng = True
                self.duckdb_engine = duckdb_engine.DuckDBConsumer()
                print("> DuckDB engine created ..\n>")
            if eng == 'DataFusion':
                self.datafusion_eng = True
                self.datafusion_engine = datafusion_engine.DataFusionConsumer()
                print("> DataFusion engine created ..\n>")
            if eng == 'Acero':
                self.acero_eng = True
                self.acero_engine = acero_engine.AceroConsumer()
                print("> Acero engine created ..\n>")



