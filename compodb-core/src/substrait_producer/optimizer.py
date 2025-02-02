from abc import ABC, abstractmethod

class Optimizer(ABC):

    @abstractmethod
    def optimize_substrait(self, substrait_query: str) -> str:
        pass

    @abstractmethod
    def register_table(self, table_path: str) -> None:
        pass

    @abstractmethod
    def get_name(self) -> str:
        pass
