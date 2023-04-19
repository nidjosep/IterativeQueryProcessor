from typing import TypeVar, Type

import dask.dataframe as dd
import time

from src import DEFAULT_PARTITION_COUNT
from src.core.iterative_query_processor import IterativeQueryProcessor
from src.core.query_context import QueryContext
from src.core.strategies.iteration_strategy import IterationStrategy
from src.core.strategies.shortest_path import ShortestPath
from src.core.strategies.transitive_closure import TransitiveClosure

T = TypeVar('T')


def get_strategy(problem_type) -> Type[IterationStrategy]:
    if problem_type == 'transitive_closure':
        return TransitiveClosure
    elif problem_type == 'shortest_path':
        return ShortestPath
    else:
        raise ValueError(f'Invalid problem type: {problem_type}')


def get_query_context(problem_type, data):
    query_context = QueryContext()
    query_context.data = data
    if problem_type == 'transitive_closure':
        query_context.source = 1
        query_context.columns = ['source', 'target']
    elif problem_type == 'shortest_path':
        query_context.source = 1
        query_context.target = 3
        query_context.columns = ['source', 'target', 'distance']
    return query_context


def main(problem_type, data_path, number_of_partitions):
    strategy = get_strategy(problem_type)()

    print("Loading data as dask dataframes..")
    data = dd.read_csv(data_path, number_of_partitions)
    print("Completed loading!")

    start_time = time.time()

    query_context = get_query_context(problem_type, data)

    print("Initiating iterative query processor..")
    processor = IterativeQueryProcessor(query_context)
    result = processor.iterative_query_processing(strategy)

    print("Execution completed. Final Result:")
    result = result.reset_index(drop=True)
    print(result.compute())

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"\nTime taken : {elapsed_time:.2f} seconds")


if __name__ == '__main__':
    problem = 'transitive_closure'  # ['transitive_closure', 'shortest_path']
    path = f'../test_data/{problem}.csv'
    main(problem, path, DEFAULT_PARTITION_COUNT)
