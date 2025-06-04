from deltalake import DeltaTable
from pydantic import BaseModel


class ClusteringHealth(BaseModel):
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
    columns: list[str]

    count_files_total: int
    count_files_no_overlap: int
    count_files_with_overlap: int

    min: int
    q05: int
    q25: int
    q50: int
    q75: int
    q95: int
    max: int
    mean: float
    std: float

    hist_bins: list[int]
    hist_cnts: list[int]