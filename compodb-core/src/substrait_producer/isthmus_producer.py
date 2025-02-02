import json
import jpype.imports
from src.substrait_producer.parser import Parser
from src.substrait_producer.optimizer import Optimizer
import src.substrait_producer.java_definitions as java
from com.google.protobuf.util import JsonFormat as json_formatter
from src.errors import ProductionError
import os


class IsthmusProducer(Parser, Optimizer):

    def __init__(self):
        self.java_schema_list = None

    def to_substrait(self, native_query: str) -> str:
        try:
            json_plan = self.produce_isthmus_substrait(native_query, self.java_schema_list)
            python_json = json.loads(json_plan)

            return json.dumps(python_json, indent=2)

        except Exception as e:
            raise ProductionError(repr(e))


    def optimize_substrait(self, substrait_query: str) -> str:
        return substrait_query


    def get_isthmus_schema(self):
        isthmus_schema = []
        for file in os.listdir("/app/src/substrait_producer/isthmus_kit"):
            if file.endswith(".sql"):
                with open(f'/app/src/substrait_producer/isthmus_kit/{file}') as create_file:
                    create_sql = create_file.read()
                isthmus_schema.append(create_sql)
        # print(isthmus_schema)
        return isthmus_schema


    def get_java_schema(self, schema_list):
        arr = java.ArrayListClass()

        for create_table in schema_list:
            java_obj = jpype.JObject @ jpype.JString(create_table)
            arr.add(java_obj)

        return java.ListClass @ arr


    def produce_isthmus_substrait(self, sql_string, schema_list):
        sql_to_substrait = java.SqlToSubstraitClass()
        java_sql_string = jpype.java.lang.String(sql_string)
        plan = sql_to_substrait.execute(java_sql_string, schema_list)
        json_plan = json_formatter.printer().print_(plan)
        return json_plan





    def register_table(self, table: str) -> None:
        self.java_schema_list = self.get_java_schema(self.get_isthmus_schema())


    def get_name(self) -> str:
        return "Calcite"