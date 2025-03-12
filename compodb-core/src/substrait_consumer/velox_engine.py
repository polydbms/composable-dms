import os
#import velox_wrapper
import subprocess
from src.substrait_consumer.execution_engine import ExecutionEngine

class VeloxEngine(ExecutionEngine):

    def __init__(self):
        self.tables = []
        self.query_dir = '/app/tmp'
        self.query = os.path.join(self.query_dir, 'velox_query_plan.json')
        if not os.path.exists(self.query_dir):
            os.makedirs(self.query_dir)
        self.VELOX_CONTAINER = "velox-container"
        self.VELOX_EXECUTABLE = "/opt/velox/bin/velox_substrait"

    def run_substrait(self, substrait_query):
        with open(self.query, 'w') as temp_file:
            temp_file.write(substrait_query)

        command = f"docker exec {VELOX_CONTAINER} {VELOX_EXECUTABLE} --plan {self.query} --files " + " ".join(self.tables)
        result = subprocess.run(command, shell=True, capture_output=True, text=True)

        if result.returncode == 0:
            os.remove(self.query)
            print(result.stdout)
            return result.stdout
        else:
            os.remove(self.query)
            return f"Error: {result.stderr}"


    def register_table(self, table: str):
        if table.endswith(".parquet"):
            self.tables.append(f'/app/data/parquet/{table}')
            self.tables = list(dict.fromkeys(self.tables))
        else:
            self.tables.append(f'/app/data/csv/{table}')
            self.tables = list(dict.fromkeys(self.tables))

    def get_name(self) -> str:
        return "Velox"