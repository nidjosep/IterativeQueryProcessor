import dask.dataframe as dd

from src.core.strategies.iteration_strategy import IterationStrategy

"""
-- An example iterative query to find the shortest path:

WITH RECURSIVE shortest_path AS (
    -- Base case: base method is responsible for executing this query
    SELECT src, dst, distance
    FROM graph
    WHERE source = 1

    UNION ALL

    -- Recursive case: handle method is responsible to execute this query
    SELECT sp.src, g.dst, sp.distance || g.target, sp.total_weight + g.weight
    FROM graph g
    JOIN shortest_path sp ON g.src = sp.dst
    WHERE NOT g.dst = ANY(sp.distance)  -- Avoid cycles
)

-- process_result method is responsible to execute this query at the end
SELECT path, total_weight
FROM shortest_path
WHERE target = 5  -- Replace 5 with the desired destination node
ORDER BY total_weight
LIMIT 1;

"""


class ShortestPath(IterationStrategy):

    def __init__(self) -> None:
        self.query_context = None
        self.columns = None
        self.target = None
        self.source = None

    def base(self, query_context) -> dd.DataFrame:
        self.query_context = query_context
        self.columns = query_context.columns
        self.source = query_context.source
        self.target = query_context.target
        edges = self.query_context.data
        if self.source is not None:
            edges = edges.query(f"{self.columns[0]} == {self.source}")
        return edges

    def handle(self, base, edges) -> dd.DataFrame:
        joined = base.merge(edges, left_on=self.columns[1], right_on=self.columns[0], suffixes=('', '_new'))
        new_edges = joined[joined[self.columns[1]] == joined[self.columns[0] + '_new']]

        new_edges[self.columns[2]] = new_edges[self.columns[2]] + new_edges[self.columns[2] + '_new']

        new_edges = new_edges[[self.columns[0], self.columns[1] + '_new', self.columns[2]]]

        new_edges = new_edges.rename(columns={self.columns[0]: self.columns[0], self.columns[1] + '_new': self.columns[1]})

        # remove cyclic dependencies
        new_edges = new_edges[~new_edges[self.columns[1]].isin(set(base[self.columns[1]].drop_duplicates()))]

        return new_edges

    def process_result(self, edges: dd.DataFrame) -> dd.DataFrame:
        print(edges.compute())
        edges = edges.groupby([self.columns[0], self.columns[1]])[self.columns[2]].min().reset_index()
        if self.target is not None:
            edges = edges.query(f"{self.columns[1]} == {self.target}")
        return edges
