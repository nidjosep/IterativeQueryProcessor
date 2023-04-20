import dask.dataframe as dd

from src.core.strategies.iteration_strategy import IterationStrategy


class IterativeQueryProcessor:
    def __init__(self, query_context):
        self.query_context = query_context

    def iterative_query_processing(self, strategy: IterationStrategy) -> dd.DataFrame:
        changes = True
        base = strategy.base(self.query_context)

        while changes:
            new_data = strategy.handle(base, self.query_context.data)

            if len(new_data) == 0:  # if no new records identified, then exit the iteration
                break

            base = dd.concat([base, new_data]).drop_duplicates()

        return strategy.process_result(base)
