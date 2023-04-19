from abc import ABC, abstractmethod
import dask.dataframe as dd


class IterationStrategy(ABC):

    @abstractmethod
    def base(self, query) -> dd.DataFrame:
        pass

    @abstractmethod
    def handle(self, base, data) -> dd.DataFrame:
        pass

    @abstractmethod
    def process_result(self, edges) -> dd.DataFrame:
        pass
