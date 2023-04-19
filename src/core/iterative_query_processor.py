from typing import TypeVar

import dask.dataframe as dd

from src.core.strategies.iteration_strategy import IterationStrategy

T = TypeVar('T')


class IterativeQueryProcessor:
    def __init__(self, query_context):
        self.query_context = query_context

    def iterative_query_processing(self, strategy: IterationStrategy) -> dd.DataFrame:
        changes = True
        data = strategy.base(self.query_context)

        while changes:
            new_data = strategy.handle(data)
            updated_result = dd.concat([data, new_data]).drop_duplicates()

            changes = len(data) != len(updated_result)
            data = updated_result

        return strategy.process_result(data)
