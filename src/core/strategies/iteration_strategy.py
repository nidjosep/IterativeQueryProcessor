from abc import ABC, abstractmethod
from typing import TypeVar

T = TypeVar('T')


class IterationStrategy(ABC):

    @abstractmethod
    def base(self, query) -> T:
        pass

    @abstractmethod
    def handle(self, data) -> T:
        pass

    @abstractmethod
    def process_result(self, edges) -> T:
        pass
