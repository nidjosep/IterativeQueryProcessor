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


def get_query_context(problem_type, data, source, target):
    query_context = QueryContext()
    query_context.data = data
    query_context.columns = data.compute().columns.tolist()
    if problem_type == 'transitive_closure':
        query_context.source = source
    elif problem_type == 'shortest_path':
        query_context.source = source
        query_context.target = target
    return query_context


def main(problem_type, data_path, number_of_partitions, source, target):
    strategy = get_strategy(problem_type)()

    print("Loading data as dask dataframes..")
    data = dd.read_csv(data_path, number_of_partitions)
    print("Completed loading!")
    query_context = get_query_context(problem_type, data, source, target)

    print("Invoking iterative query processor..")
    start_time = time.time()
    processor = IterativeQueryProcessor(query_context)
    result = processor.iterative_query_processing(strategy)

    print("Execution completed. Final Result:")
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"\nTime taken : {elapsed_time:.2f} seconds")

    result = result.reset_index(drop=True)
    print(result.compute())


if __name__ == '__main__':
    index = int(input("Enter problem type [1. transitive_closure, 2. shortest_path]: "))
    problem = "transitive_closure"
    if index == 2:
        problem = "shortest_path"
    path = f'../test_data/{problem}.csv'
    source = int(input("Enter the source node value: "))
    target = 0

    if problem == 'shortest_path':
        target = int(input("Enter the target node value: "))

    main(problem, path, DEFAULT_PARTITION_COUNT, source, target)