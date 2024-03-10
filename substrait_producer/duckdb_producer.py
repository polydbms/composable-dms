import json

import duckdb

class DuckDBProducer:
    def __init__(self, sf, db_connection=None):
        if db_connection is not None:
            self.db_connection = db_connection
        else:
            self.db_connection = duckdb.connect()
            self.db_connection.execute("INSTALL substrait")
            self.db_connection.execute("LOAD substrait")
            self.db_connection.execute(f"CALL dbgen(sf={sf})")
            self.sf = sf

    def produce_substrait(self, query, filename):
        try:
            json_plan = self.db_connection.get_substrait_json(query).fetchone()[0]
            python_json = json.loads(json_plan)
            file_name = f"/data/substrait/duckdb/{filename.split('.')[0]}.json"
            with open(file_name, "w") as outfile:
                outfile.write(json.dumps(python_json, indent=4))
            print(f"PROD DuckDB\t\tPROD SUCCESS")

            return json.dumps(python_json, indent=2)

        except Exception as e:
            print(f"PROD DuckDB\t\tPROD EXCEPTION: get_substrait_json() not working on {filename.split('.')[0]}: {repr(e)}")

            # Reconnect after Exception
            self.db_connection.close()
            self.db_connection = duckdb.connect()
            self.db_connection.execute("INSTALL substrait")
            self.db_connection.execute("LOAD substrait")
            self.db_connection.execute(f"CALL dbgen(sf={self.sf})")

            return None