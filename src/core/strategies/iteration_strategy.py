from abc import ABC, abstractmethod
from typing import TypeVar

T = TypeVar('T')


class IterationStrategy(ABC):
    @abstractmethod
    def handle(self, data: T) -> T:
        pass
