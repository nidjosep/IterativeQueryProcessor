from typing import TypeVar

import dask.dataframe as dd

from src.core.strategies.iteration_strategy import IterationStrategy

T = TypeVar('T')


class IterativeQueryProcessor:
    def __init__(self, data: dd.DataFrame):
        self.data = data

    def iterative_query_processing(self, compute_iteration: IterationStrategy) -> dd.DataFrame:
        changes = True
        result = self.data.copy()

        while changes:
            updated_result = compute_iteration.handle(result)

            changes = len(result) != len(updated_result)
            result = updated_result

        return result
