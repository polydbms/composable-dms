from abc import ABC, abstractmethod

class Consumer(ABC):

    @abstractmethod
    def run_substrait(self, query: str):
        pass

    @abstractmethod
    def register_table(self, table_path: str) -> None:
        pass

    @abstractmethod
    def get_name(self) -> str:
        pass
