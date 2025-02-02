from abc import ABC, abstractmethod

class ExecutionEngine(ABC):

    @abstractmethod
    def run_substrait(self, substrait_query: str):
        pass

    @abstractmethod
    def register_table(self, table_path: str) -> None:
        pass

    @abstractmethod
    def get_name(self) -> str:
        pass
