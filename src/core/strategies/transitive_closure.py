import dask.dataframe as dd

from src.core.strategies.iteration_strategy import IterationStrategy

"""
WITH RECURSIVE transitive_closure AS (
    -- Base case: the init method will execute this first iteration
    SELECT source, target
    FROM graph
    WHERE source = 1

    UNION

    -- Recursive case: the handle method will be responsible for iterative execution
    SELECT tc.source, g.target
    FROM graph g
    JOIN transitive_closure tc ON g.source = tc.target
)

-- the process_result method will handle this final query
SELECT * FROM transitive_closure;

"""


class TransitiveClosure(IterationStrategy):

    def __init__(self) -> None:
        self.query_context = None
        self.source = None
        self.columns = None

    def base(self, query_context) -> dd.DataFrame:
        self.query_context = query_context
        self.columns = query_context.columns
        self.source = query_context.source
        edges = query_context.data
        return edges.query(f"{self.columns[0]} == {self.source}")

    def handle(self, base: dd.DataFrame, edges: dd.DataFrame) -> dd.DataFrame:
        joined = base.merge(edges, left_on=self.columns[1], right_on=self.columns[0], suffixes=('', '_new'))
        new_edges = joined.drop(columns=[self.columns[1], self.columns[0] + '_new']).rename(columns={self.columns[1] + '_new': self.columns[1]})
        new_edges = new_edges[new_edges[self.columns[0]] != new_edges[self.columns[1]]].drop_duplicates()

        # remove cyclic dependencies
        visited_nodes = set(base[self.columns[0]].unique()) | set(base[self.columns[1]].unique())
        new_edges = new_edges[~new_edges[self.columns[1]].isin(visited_nodes)]

        return new_edges

    def process_result(self, edges: dd.DataFrame) -> dd.DataFrame:
        return edges
