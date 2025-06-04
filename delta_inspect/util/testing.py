import datetime
from enum import IntEnum
from pathlib import Path
from typing import Literal, Tuple

import polars as pl
from deltalake import write_deltalake
from pydantic import BaseModel, Field


class ColumnType(IntEnum):
    """
    An enumeration for different types of columns in a Delta Table.
    """

    INTEGER = 1
    FLOAT= 2
    TIMESTAMP = 3
    DATE = 4
    STRING = 5


DEFUALT_COLUMN_TYPE = {
    "integer": ColumnType.INTEGER,
    "float": ColumnType.FLOAT,
    "date": ColumnType.DATE,
    "timestamp": ColumnType.TIMESTAMP,
    "string": ColumnType.STRING,
}

DEFAULT_NULL_COUNTS = {
    "integer": 0,
    "float": 1,
    "date": 0,
    "timestamp": 0,
    "string": 1,
}


class TestDeltaTable(BaseModel):
    """
    Conveniently generate Delta Lake test data with sensible defaults.

    Data values are generated based on column type and row index. For
    example, a column of dtype `String` and row index 7 would generate the value
    "string_7". An `Integer` column would simply generate the corresponding row index value.
    Temporal columns such as `Date` or `Timestamp` add a temporal interval 
    equivalent to the row index to a fixed starting point in time.

    To produce duplicates, you may set replication higher than 0. In this case,
    the same data values are duplicated by the replication factor. 

    """

    path: Path | str = "deltatabletest"
    name: str | None = "DeltaLake Test Table"
    description: str | None = "An artifical Delta Lake Test Table used for testing."
    configuration: dict[str, str] | None = {"logRetentionDuration": "7"}

    schema_: dict[str, ColumnType] = Field(
        default=DEFUALT_COLUMN_TYPE,
        description="Provide a simplistic schema as dict with keys corresponding to column names and value refering to dtypes.",
    )
    row_index: int | Tuple[int, int] = Field(
        default=5,
        description="Define number of rows. If tuple, first value refers to start idx and second value to end idx.",
    )
    null_count: dict[str, int] | int = Field(
        default=DEFAULT_NULL_COUNTS,
        description="Indicate the existence of null values. If int, the number of null values is identical for all columns. Use dict to having varying null count per column, keys refer to columns and values to null counts.",
    )
    replication: int = 0
    write_mode: Literal["error", "append", "overwrite", "ignore"] = "append"
    partition_by: list[str] | None = None

    @property
    def _row_start(self) -> int:
        if isinstance(self.row_index, int):
            return 0
        else:
            return self._row_start

    @property
    def _row_end(self) -> int:
        if isinstance(self.row_index, int):
            return self.row_index
        else:
            return self._row_end

    def _generate_integer_column(self, name: str) -> pl.Series:
        """
        Generate a column of integers for the Delta Table.
        """
        return pl.Series(name, range(self._row_start, self._row_end))    
    
    def _generate_float_column(self, name: str) -> pl.Series:
        """
        Generate a column of integers for the Delta Table.
        """
        return self._generate_integer_column(name).cast(pl.Float32)

    def _generate_date_column(self, name: str) -> pl.Series:
        """
        Generate a column of dates for the Delta Table.
        """
        init = datetime.date(2023, 1, 1)
        start = init + datetime.timedelta(days=self._row_start)
        end = init + datetime.timedelta(days=self._row_end - 1)
        return pl.Series(
            name, pl.date_range(start=start, end=end, interval="1d", eager=True)
        )

    def _generate_timestamp_column(self, name: str) -> pl.Series:
        """
        Generate a column of timestamps for the Delta Table.
        """
        init = datetime.datetime(2023, 1, 1, 0, 0, 0)
        start = init + datetime.timedelta(hours=self._row_start)
        end = init + datetime.timedelta(hours=self._row_end - 1)
        return pl.Series(
            name, pl.datetime_range(start=start, end=end, interval="1h", eager=True)
        )

    def _generate_string_column(self, name: str) -> pl.Series:
        """
        Generate a column of strings for the Delta Table.
        """
        return pl.Series(
            name, [f"string_{i}" for i in range(self._row_start, self._row_end)]
        )

    def generate(self) -> pl.DataFrame:
        """
        Generate a DataFrame based on the defined columns and rows.

        Returns:
            pl.DataFrame: A DataFrame with the specified columns and rows.
        """
        data = {}
        for col_name, col_type in self.schema_.items():
            if col_type == ColumnType.INTEGER:
                data[col_name] = self._generate_integer_column(col_name)
            elif col_type == ColumnType.FLOAT:
                data[col_name] = self._generate_float_column(col_name)
            elif col_type == ColumnType.DATE:
                data[col_name] = self._generate_date_column(col_name)
            elif col_type == ColumnType.TIMESTAMP:
                data[col_name] = self._generate_timestamp_column(col_name)
            elif col_type == ColumnType.STRING:
                data[col_name] = self._generate_string_column(col_name)
            else:
                raise ValueError("Invalid dtype encountered.")

        if isinstance(self.null_count, dict):
            for col_name, count in self.null_count.items():
                indices = list(range(0, count))
                data[col_name][indices] = None

        elif isinstance(self.null_count, int):
            indices = list(range(0, self.null_count))
            for col_name in self.schema_.keys():
                data[col_name][indices] = None
        
        df = pl.DataFrame(data)

        if self.replication:
            duplication_factor = self.replication + 1
            df = pl.concat([df]*duplication_factor)

        return df

    def write(self):
        df = self.generate()
        write_deltalake(
            table_or_uri=self.path,
            data=df,
            name=self.name,
            description=self.description,
            configuration=self.configuration,
            mode=self.write_mode,
            partition_by=self.partition_by,
        )
