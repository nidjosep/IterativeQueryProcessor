import dask.dataframe as dd

from src.core.strategies.iteration_strategy import IterationStrategy


class ShortestPath(IterationStrategy):
    def __init__(self) -> None:
        pass

    def handle(self, edges: dd.DataFrame) -> dd.DataFrame:
        joined = edges.merge(edges, left_on='dst', right_on='src', suffixes=('_x', '_y'))

        new_edges = joined[joined['dst_x'] == joined['src_y']]

        new_edges['distance'] = new_edges['distance_x'] + new_edges['distance_y']

        new_edges = new_edges[['src_x', 'dst_y', 'distance']]

        new_edges = new_edges.rename(columns={'src_x': 'src', 'dst_y': 'dst'})

        shortest_paths = dd.concat([edges, new_edges]).groupby(['src', 'dst'])['distance'].min().reset_index()

        return shortest_paths

