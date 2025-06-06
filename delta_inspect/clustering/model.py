from deltalake import DeltaTable
from pydantic import Field

from delta_inspect.util.model import BaseDistribution


class Clustering(BaseDistribution):
    """
    Represents clustering health of a Delta Lake table for specified columns.
    
    A good clustering health is given when min/max values of underlying parquet
    files are not overlapping. In this case, data pruning and file skipping is
    ideal because only a single parquet needs to be scanned for a given point
    query.

    In contrast, poor clustering health is given when min/max values of parquets
    are often overlapping which prevents efficient data pruning.
    """

    dt: DeltaTable

    analyzed_columns: list[str]
    partition_columns: list[str] = Field(default_factory=list)
    clustering_columns: list[str] = Field(default_factory=list)
    zorder_columns: list[str] = Field(default_factory=list)
    
    count_no_overlap: int
    count_with_overlap: int
    count_without_min_max: int
