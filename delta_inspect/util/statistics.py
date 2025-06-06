import polars as pl

from delta_inspect.util.model import Histogram


def compute_distribution_metrics(
    df: pl.DataFrame, column: str
) -> dict[str, int | float]:
    """
    Compute distribution metrics from the DataFrame.
    """

    pcol = pl.col(column)
    expr = [
        pl.len().alias("count"),
        pcol.min().alias("min"),
        pcol.quantile(0.05).alias("q05"),
        pcol.quantile(0.25).alias("q25"),
        pcol.quantile(0.5).alias("q50"),
        pcol.quantile(0.75).alias("q75"),
        pcol.quantile(0.95).alias("q95"),
        pcol.max().alias("max"),
        pcol.mean().alias("mean"),
        pcol.std().alias("std"),
    ]
    return df.select(*expr).row(0, named=True)


def compute_histogram_metrics(
    df: pl.DataFrame, column: str, bins: list[float | int]
) -> Histogram:
    
    df_hist = df[column].hist(bins=bins)
    hist_bins = df_hist["breakpoint"].to_list()
    hist_cnts = df_hist["count"].to_list()

    # polars histogram does not include values greater than highest bin
    # hence we have do add it manually
    max_value = df[column].max()
    greater_than_max_bin = df.filter(pl.col(column) >= hist_bins[-1]).shape[0]
    hist_bins.append(max_value)
    hist_cnts.append(greater_than_max_bin)

    lower_than_min_bin = df.filter(pl.col(column) < bins[0]).shape[0]
    hist_cnts[0] += lower_than_min_bin

    return Histogram(bins=hist_bins, cnts=hist_cnts)
