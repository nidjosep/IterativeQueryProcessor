from typing import TypeVar

import dask.dataframe as dd

from src.core.iterative_query_processor import IterativeQueryProcessor
from src.core.strategies.transitive_closure import TransitiveClosure

T = TypeVar('T')


def main():
    data_path = '../test_data/transitive_closure.csv'
    number_of_partitions = 4

    # Load data from file using Dask
    data = dd.read_csv(data_path, number_of_partitions)

    processor = IterativeQueryProcessor(data)
    transitive_closure = TransitiveClosure()

    result = processor.iterative_query_processing(transitive_closure)
    print(result.compute())


if __name__ == '__main__':
    main()
