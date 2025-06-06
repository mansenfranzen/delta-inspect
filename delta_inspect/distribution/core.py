from typing import Literal
import polars as pl

from deltalake import DeltaTable
from delta_inspect.distribution.model import (
    DistributionMetric,
    ItemDistribution,
    Distribution,
)
from delta_inspect.util.statistics import (
    compute_distribution_metrics,
    compute_histogram_metrics,
)

BINS = (
    list(range(0, 16 + 1, 4))
    + list(range(24, 32 + 1, 8))
    + list(range(64, 128 + 1, 32))
    + list(range(128 + 32, 256 + 1, 32))
    + list(range(256 + 64, 512 + 1, 64))
)


def _get_min_max_items(
    df: pl.DataFrame, column_key: str, column_by: str
) -> dict[str, str]:
    """Get the minimum and maximum items from a DataFrame."""
    return df.select(
        pl.col(column_key).top_k_by(by=column_by, k=1, reverse=False).alias("min_item"),
        pl.col(column_key).top_k_by(by=column_by, k=1, reverse=True).alias("max_item"),
    ).row(0, named=True)


def _get_distribution(
    df: pl.DataFrame, column_key: str, metric: str
) -> ItemDistribution:
    metrics_distribution = compute_distribution_metrics(df=df, column=metric)
    metrics_hist = compute_histogram_metrics(df=df, column=metric, bins=BINS)
    values_min_max = _get_min_max_items(df=df, column_key=column_key, column_by=metric)

    return ItemDistribution(hist=metrics_hist, **values_min_max, **metrics_distribution)


def distribution(
    path: str, metric: DistributionMetric = DistributionMetric.FILE_SIZE
) -> Distribution:
    dt = DeltaTable(path)
    df_files = pl.DataFrame(dt.get_add_actions())


    if metric == DistributionMetric.FILE_SIZE:
        metric_column = "size_bytes"
        df_files = df_files.with_columns(
            pl.col("size_bytes") / 1024 / 1024  # Convert to MB
        )
    elif metric == DistributionMetric.NUM_RECORDS:
        metric_column = "num_records"

    distribution_files = _get_distribution(
        df=df_files, column_key="path", metric=metric_column
    )

    if dt.metadata().partition_columns:
        df_partition = df_files.group_by(
            pl.col("partition_values").struct.json_encode()
        ).agg(
            pl.col("size_bytes").sum().alias("size_bytes"),
            pl.col("num_records").sum().alias("num_records"),
        )
        distribution_partitions = _get_distribution(
            df=df_partition, column_key="partition_values", metric=metric_column
        )
    else:
        distribution_partitions = None

    return Distribution(
        dt=dt,
        metric=metric,
        distribution_files=distribution_files,
        distribution_partitions=distribution_partitions,
    )
