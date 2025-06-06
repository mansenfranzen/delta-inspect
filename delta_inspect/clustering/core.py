from deltalake import DeltaTable
import polars as pl
import polars.selectors as cs

from typing import Tuple
from rtree import index

from delta_inspect.clustering.model import Clustering
from delta_inspect.util.history import extract_operation_params
from delta_inspect.util.statistics import (
    compute_distribution_metrics,
    compute_histogram_metrics,
)
from delta_inspect.util.table_loader import load_table

BINS = list(range(17)) + [32, 64, 128]


def fill_min_max_values(dt: DeltaTable, columns: list[str]) -> pl.DataFrame:
    """
    Fill missing minimum and maximum values in case of partitioned columns.
    Partitioned columns don't have min/max values in the DataFrame, so we
    extract them from the partition values.
    """

    df = pl.DataFrame(dt.get_add_actions())
    partition_columns = dt.metadata().partition_columns

    expr = []
    for column in columns:
        if column in partition_columns:
            cpartition = pl.col("partition_values").struct.field(column)
            expr.extend(
                [cpartition.alias(f"{column}_min"), cpartition.alias(f"{column}_max")]
            )
        else:
            expr.extend(
                [
                    pl.col("min").struct.field(column).alias(f"{column}_min"),
                    pl.col("max").struct.field(column).alias(f"{column}_max"),
                ]
            )

    return df.select("path", *expr)


def get_dictionary_encoding(
    df: pl.DataFrame, columns: list[str]
) -> Tuple[pl.Series, pl.Series]:
    """
    Get a dictionary encoding for the specified columns in the DataFrame. This encoding
    is used to convert categorical or string columns into numerical values for spatial indexing.
    """

    values = [df.select(pl.col(column).unique()).to_series() for column in columns]

    df_encoding = (
        pl.concat(items=values).unique().sort().to_frame("key").with_row_index("value")
    )
    return df_encoding["key"], df_encoding["value"]


def apply_numerical_encoding(df: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    """
    Apply numerical encoding to specified columns in the DataFrame. This is required
    for the spatial index to work correctly.

    Args:
        df (pl.DataFrame): The input DataFrame.
        columns (list[str]): List of column names to encode.

    Returns:
        pl.DataFrame: DataFrame with encoded columns.
    """

    expr = []
    for column in columns:
        col_min = f"{column}_min"
        col_max = f"{column}_max"

        col_encoded_min = f"{col_min}_encoded"
        col_encoded_max = f"{col_max}_encoded"

        pcol_min = pl.col(col_min)
        pcol_max = pl.col(col_max)

        assert df[col_min].dtype == df[col_max].dtype
        dtype = df[col_min].dtype

        if dtype in (pl.String, pl.Categorical):
            key, value = get_dictionary_encoding(df, columns=[col_min, col_max])
            expr.extend(
                [
                    pcol_min.replace_strict(
                        old=key, new=value, return_dtype=pl.Int64
                    ).alias(col_encoded_min),
                    pcol_max.replace_strict(
                        old=key, new=value, return_dtype=pl.Int64
                    ).alias(col_encoded_max),
                ]
            )

        elif dtype.is_temporal():
            expr.extend(
                [
                    pcol_min.cast(pl.Int64).alias(col_encoded_min),
                    pcol_max.cast(pl.Int64).alias(col_encoded_max),
                ]
            )
        else:
            expr.extend(
                [
                    pcol_min.cast(pl.Float64).alias(col_encoded_min),
                    pcol_max.cast(pl.Float64).alias(col_encoded_max),
                ]
            )

    return df.select("path", *expr)


def remove_nulls(df: pl.DataFrame) -> pl.DataFrame:
    """
    Remove rows with null values from the DataFrame because R-tree index
    does not support null values in the bounding box.

    Args:
        df (pl.DataFrame): The input DataFrame.

    Returns:
        pl.DataFrame: DataFrame with null values removed.
    """
    return df.drop_nulls()


def create_rtree_index(df_encoded: pl.DataFrame) -> index.Index:
    """
    Create an R-tree index from the encoded DataFrame.

    Args:
        df_encoded (pl.DataFrame): The encoded DataFrame.

    Returns:
        index.Index: An R-tree index.
    """

    rindex = index.Index()
    rindex.interleaved = False

    for idx, row in enumerate(df_encoded.iter_rows()):
        rindex.insert(idx, row[1:], row[0])

    return rindex


def get_overlapping_partitions_count(
    rindex: index.Index, df_non_nulls: pl.DataFrame
) -> pl.DataFrame:
    """
    Get overlapping partitions count from the R-tree index.

    Args:
        rindex (index.Index): The R-tree index.
        df_encoded (pl.DataFrame): The encoded DataFrame.

    Returns:
        pl.DataFrame: DataFrame with overlapping partitions.
    """

    bbox_column_indices = range(1, df_non_nulls.shape[1])
    df_bbox = df_non_nulls.select(cs.by_index(bbox_column_indices))
    overlap_counts = [rindex.count(row) - 1 for row in df_bbox.iter_rows()]

    return df_non_nulls.with_columns(overlap_count=pl.Series(overlap_counts))


def compute_overlap_metrics(df_overlap_count: pl.DataFrame) -> dict[str, int | float]:
    """
    Compute clustering metrics from the DataFrame with overlapping min/max ranges.

    Args:
        df_overlap_count (pl.DataFrame): DataFrame with overlap counts.

    Returns:
        pl.DataFrame: DataFrame with clustering metrics.
    """

    pcol = pl.col("overlap_count")
    expr = [
        pcol.eq(0).sum().alias("count_no_overlap"),
        pcol.gt(0).sum().alias("count_with_overlap"),
    ]

    return df_overlap_count.select(*expr).row(0, named=True)


def clustering_health(path: str, columns: list[str]) -> Clustering:
    """
    Get an R-tree index for the specified columns in the DataFrame.

    Args:
        df_add_actions (pl.DataFrame): The DataFrame containing add actions.
        columns (list[str]): List of column names to index.

    Returns:
        index.Index: An R-tree index.
    """

    dt = load_table(path)

    df_filled = fill_min_max_values(
        dt=dt,
        columns=columns,
    )
    df_encoded = apply_numerical_encoding(df_filled, columns)
    null_counts = df_encoded.select(pl.any_horizontal(pl.all().is_null())).sum().item()
    df_non_nulls = remove_nulls(df_encoded)

    rindex = create_rtree_index(df_non_nulls)
    df_overlap_count = get_overlapping_partitions_count(
        rindex=rindex, df_non_nulls=df_non_nulls
    )

    metrics_overlap = compute_overlap_metrics(df_overlap_count)
    metrics_distribution = compute_distribution_metrics(
        df=df_overlap_count, column="overlap_count"
    )

    hist = compute_histogram_metrics(
        df=df_overlap_count, column="overlap_count", bins=BINS
    )

    history = dt.history()
    partition_columns = dt.metadata().partition_columns
    clustering_columns = extract_operation_params(history=history, param_key="clusterBy")
    zorder_columns = extract_operation_params(history=history, param_key="zOrderBy")

    return Clustering(
        dt=dt,
        analyzed_columns=columns,
        partition_columns=partition_columns,
        clustering_columns=clustering_columns,
        zorder_columns=zorder_columns,
        hist=hist,
        count_without_min_max=null_counts,
        **metrics_distribution,
        **metrics_overlap,
    )
