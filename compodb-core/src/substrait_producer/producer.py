from abc import ABC, abstractmethod

class Producer(ABC):

    @abstractmethod
    def produce_substrait(self, query: str) -> str:
        pass

    @abstractmethod
    def register_table(self, table_path: str) -> None:
        pass

    @abstractmethod
    def get_name(self) -> str:
        pass
