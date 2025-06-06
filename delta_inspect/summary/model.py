import datetime
from typing import Annotated, Any
from pydantic import BaseModel, BeforeValidator, ConfigDict, Field
from deltalake.table import ProtocolVersions

def _unix_ms_to_datetime(value: int) -> datetime.datetime:
    """Convert a Unix timestamp in milliseconds to a datetime object."""
    return datetime.datetime.fromtimestamp(value / 1000)

class TableStatistics(BaseModel):
    num_files: int
    num_records: int
    num_partitions: int
    total_size_bytes: int

class ColumnStatistics(BaseModel):
    min: Any
    max: Any
    null_count: int

class SchemaField(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    name: str
    type: Annotated[str, BeforeValidator(str)]
    nullable: bool = True
    metadata: dict = Field(default_factory=dict)

class TableMetadata(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str | None = None
    name: str | None = None
    description: str | None = None
    partition_columns: list[str] = Field(default_factory=list)
    created_time: Annotated[datetime.datetime, BeforeValidator(_unix_ms_to_datetime)]
    configuration: dict[str, str] = Field(default_factory=dict)

class TableSummary(BaseModel):
    schema_: list[SchemaField] = Field(alias='schema')
    metadata: TableMetadata

    version: int
    protocol: ProtocolVersions

    last_commit_timestamp: datetime.datetime
    last_vacuum_timestamp: datetime.datetime | None = None
    last_optimize_timestamp: datetime.datetime | None = None

    clustering_columns: list[str] = Field(default_factory=list)
    zorder_columns: list[str] = Field(default_factory=list)

    table_statistics: TableStatistics
    column_statistics: dict[str, ColumnStatistics]