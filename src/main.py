from typing import TypeVar, Type

import dask.dataframe as dd

from src.core.iterative_query_processor import IterativeQueryProcessor
from src.core.strategies.iteration_strategy import IterationStrategy
from src.core.strategies.transitive_closure import TransitiveClosure

T = TypeVar('T')


def get_strategy(problem_type) -> Type[IterationStrategy]:
    if problem_type == 'transitive_closure':
        return TransitiveClosure
    else:
        raise ValueError(f'Invalid problem type: {problem_type}')


def main(problem_type, data_path, number_of_partitions):
    data = dd.read_csv(data_path, number_of_partitions)

    processor = IterativeQueryProcessor(data)
    strategy = get_strategy(problem_type)()

    result = processor.iterative_query_processing(strategy)
    print(result.compute())


if __name__ == '__main__':
    problem = 'transitive_closure'
    path = '../test_data/transitive_closure.csv'
    partitions = 4
    main(problem, path, partitions)
