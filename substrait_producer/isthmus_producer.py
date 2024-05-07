import json
import jpype.imports
import substrait_producer.java_definitions as java
from com.google.protobuf.util import JsonFormat as json_formatter


class IsthmusProducer:
    def __init__(self, sf, ):
            self.sf = sf

    def produce_substrait(self, schema_list, query):
        try:
            java_schema_list = self.get_java_schema(schema_list)
            json_plan = self.produce_isthmus_substrait(query, java_schema_list)
            python_json = json.loads(json_plan)
            file_name = f"/data/substrait_isthmus_{query.split('.')[0]}.json"
            with open(file_name, "w") as outfile:
                outfile.write(json.dumps(python_json, indent=2))
            #    print(f"substrait plan written to: {file_name}")
            print(f"PROD Isthmus\t\tPROD SUCCESS")

            return json.dumps(python_json, indent=2)

        except Exception as e:
            print(f"PROD Isthmus\t\tPROD EXCEPTION: Isthmus not working: {repr(e)}")
            return None

    def get_java_schema(self, schema_list):
        arr = java.ArrayListClass()

        for create_table in schema_list:
            #print(create_table)
            java_obj = jpype.JObject @ jpype.JString(create_table)
            arr.add(java_obj)

        return java.ListClass @ arr

    def produce_isthmus_substrait(self, sql_string, schema_list):
        sql_to_substrait = java.SqlToSubstraitClass()
        java_sql_string = jpype.java.lang.String(sql_string)
        plan = sql_to_substrait.execute(java_sql_string, schema_list)
        json_plan = json_formatter.printer().print_(plan)
        return json_plan
