import duckdb
import ibis
import json
from queries.tpch_ibis_original import q1, q2, q3, q4, q5, q6, q7, q8, q9, q10, q11, q12, q13, q14, q15, q16, q17, q18, q19, q20, q21, q22
from google.protobuf import json_format
from ibis_substrait.compiler.core import SubstraitCompiler

class IbisProducer():

    def __init__(self):
        self._db_connection = ibis.duckdb.connect()
        self._db_connection.con.execute("INSTALL substrait")
        self._db_connection.con.execute("LOAD substrait")
        self._db_connection.con.execute(f"CALL dbgen(sf=1)")

    def produce_substrait(self, query, q_set):
        try:
            if q_set != 'tpch_sql_original':
                ibis_expr = self.get_ibis_expr(query.split('.')[0])
                compiler = SubstraitCompiler()
                tpch_proto_bytes = compiler.compile(ibis_expr)
                substrait_json = json_format.MessageToJson(tpch_proto_bytes)
            else:
                with open(f"/queries/tpch_ibis_json/{query.split('.')[0]}.json", "r") as f:
                    substrait_plan = json.loads(f.read())
                substrait_json = json.dumps(substrait_plan, indent=2)
            #file_name = f"/data/substrait_plans/substrait_{q_set.split('_')[2]}_ibis_{query.split('.')[0]}.json"
            #with open(file_name, "w") as outfile:
            #    outfile.write(substrait_json)

            print(f"PROD Ibis\t\tPROD SUCCESS")

            return substrait_json

        except Exception as e:
            print(f"PROD Ibis\t\tPROD EXCEPTION: Ibis SubstraitCompiler not working on {q_set} {query.split('.')[0]}: {repr(e)}")

            # Reconnect after Exception
            self._db_connection.con.close()
            self._db_connection = ibis.duckdb.connect()
            self._db_connection.con.execute("INSTALL substrait")
            self._db_connection.con.execute("LOAD substrait")
            self._db_connection.con.execute(f"CALL dbgen(sf={self.sf})")

            return None

    def get_ibis_expr(self, query):
        if query == 'q1':
            return q1.tpc_h01(self._db_connection)
        elif query == 'q2':
            return q2.tpc_h02(self._db_connection)
        elif query == 'q3':
            return q3.tpc_h03(self._db_connection)
        elif query == 'q4':
            return q4.tpc_h04(self._db_connection)
        elif query == 'q5':
            return q5.tpc_h05(self._db_connection)
        elif query == 'q6':
            return q6.tpc_h06(self._db_connection)
        elif query == 'q7':
            return q7.tpc_h07(self._db_connection)
        elif query == 'q8':
            return q8.tpc_h08(self._db_connection)
        elif query == 'q9':
            return q9.tpc_h09(self._db_connection)
        elif query == 'q10':
            return q10.tpc_h10(self._db_connection)
        elif query == 'q11':
            return q11.tpc_h11(self._db_connection)
        elif query == 'q12':
            return q12.tpc_h12(self._db_connection)
        elif query == 'q13':
            return q13.tpc_h13(self._db_connection)
        elif query == 'q14':
            return q14.tpc_h14(self._db_connection)
        elif query == 'q15':
            return q15.tpc_h15(self._db_connection)
        elif query == 'q16':
            return q16.tpc_h16(self._db_connection)
        elif query == 'q17':
            return q17.tpc_h17(self._db_connection)
        elif query == 'q18':
            return q18.tpc_h18(self._db_connection)
        elif query == 'q19':
            return q19.tpc_h19(self._db_connection)
        elif query == 'q20':
            return q20.tpc_h20(self._db_connection)
        elif query == 'q21':
            return q21.tpc_h21(self._db_connection)
        elif query == 'q22':
            return q22.tpc_h22(self._db_connection)
        else:
            return None
