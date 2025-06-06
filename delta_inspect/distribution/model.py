from enum import Enum
from deltalake import DeltaTable
from pydantic import BaseModel

from delta_inspect.util.model import BaseDistribution


class DistributionMetric(Enum):
    FILE_SIZE = "file_size"
    NUM_RECORDS = "num_records"


class ItemDistribution(BaseDistribution):
    min_item: str
    max_item: str


class Distribution(BaseModel):
    """
    Represents distribution in a Delta table. If partitioning
    is used, additionally stores file sizes aggregated over partition values.
    """

    dt: DeltaTable
    metric: DistributionMetric

    distribution_files: ItemDistribution
    distribution_partitions: ItemDistribution | None = None
