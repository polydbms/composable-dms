from abc import ABC, abstractmethod

class Parser(ABC):

    @abstractmethod
    def to_substrait(self, native_query: str) -> str:
        pass

    @abstractmethod
    def register_table(self, table_path: str) -> None:
        pass

    @abstractmethod
    def get_name(self) -> str:
        pass
