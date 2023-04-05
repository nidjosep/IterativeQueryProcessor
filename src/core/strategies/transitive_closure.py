import dask.dataframe as dd

from src.core.strategies.iteration_strategy import IterationStrategy


class TransitiveClosure(IterationStrategy):
    def __init__(self) -> None:
        pass

    def handle(self, edges: dd.DataFrame) -> dd.DataFrame:
        joined = edges.merge(edges, left_on='dst', right_on='src', suffixes=('', '_new'))
        new_edges = joined.drop(columns=['dst', 'src_new']).rename(columns={'dst_new': 'dst'})
        new_edges = new_edges[new_edges['src'] != new_edges['dst']].drop_duplicates()

        return dd.concat([edges, new_edges]).drop_duplicates()
