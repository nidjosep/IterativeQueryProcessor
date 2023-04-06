import dask.dataframe as dd

from src.core.strategies.iteration_strategy import IterationStrategy


class ShortestPath(IterationStrategy):
    def __init__(self) -> None:
        pass

    def handle(self, edges: dd.DataFrame) -> dd.DataFrame:
        # Get all possible paths
        joined = edges.merge(edges, left_on='dst', right_on='src', suffixes=('_x', '_y'))

        # Select edges where there is a path of length 2
        new_edges = joined[joined['dst_x'] == joined['src_y']]

        # Calculate the distance of the new edges
        new_edges['distance'] = new_edges['distance_x'] + new_edges['distance_y']

        # Select only the necessary columns
        new_edges = new_edges[['src_x', 'dst_y', 'distance']]

        # Rename the columns
        new_edges = new_edges.rename(columns={'src_x': 'src', 'dst_y': 'dst'})


        # Compute the minimum distance between each pair of nodes
        shortest_paths = dd.concat([edges, new_edges]).groupby(['src', 'dst'])['distance'].min().reset_index()

        return shortest_paths

