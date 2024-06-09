import json

import duckdb

class DuckDBProducer:
    def __init__(self):
        self.db_connection = duckdb.connect()
        self.db_connection.execute("INSTALL substrait")
        self.db_connection.execute("LOAD substrait")
        self.db_connection.execute(f"CALL dbgen(sf=1)")

    def produce_substrait(self, query, filename, q_set):
        try:
            json_plan = self.db_connection.get_substrait_json(query).fetchone()[0]
            python_json = json.loads(json_plan)
            #file_name = f"/data/substrait_plans/substrait_{q_set.split('_')[2]}_duckdb_{filename.split('.')[0]}.json"
            #with open(file_name, "w") as outfile:
            #    outfile.write(json.dumps(python_json, indent=2))
            print(f"PROD DuckDB\t\tPROD SUCCESS")
            return json.dumps(python_json, indent=2)

        except Exception as e:
            print(f"PROD DuckDB\t\tPROD EXCEPTION: get_substrait_json() not working on {filename.split('.')[0]}: {repr(e)}")

            # Reconnect after Exception
            self.db_connection.close()
            self.db_connection = duckdb.connect()
            self.db_connection.execute("INSTALL substrait")
            self.db_connection.execute("LOAD substrait")
            self.db_connection.execute(f"CALL dbgen(sf=1)")

            return None