import pytest
import polars as pl
from deltalake import write_deltalake, DeltaTable
import datetime
from delta_inspect.summary.core import summarize_table
from delta_inspect.summary.model import TableSummary, TableStatistics, ColumnStatistics, SchemaField, TableMetadata


@pytest.fixture
def simple_unpartitioned_table(tmp_path):
    """Create a simple unpartitioned Delta table for testing."""
    df = pl.DataFrame({
        "id": [1, None, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "value": [100, 200, 300]
    })
    
    write_deltalake(str(tmp_path), df.to_arrow(), mode="overwrite")
    return str(tmp_path)


@pytest.fixture
def partitioned_table(tmp_path):
    """Create a partitioned Delta table for testing."""
    df = pl.DataFrame({
        "id": [1, 2, 3, 4, 5, 6],
        "region": ["US", "US", "EU", "EU", "APAC", "APAC"],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank"],
        "value": [100, 200, 300, 400, 500, 600]
    })
    
    write_deltalake(str(tmp_path), df.to_arrow(), mode="overwrite", partition_by=["region"])
    return str(tmp_path)


@pytest.fixture
def table_with_multiple_versions(tmp_path):
    """Create a Delta table with multiple versions."""
    # Initial data (version 0)
    df1 = pl.DataFrame({
        "id": [1, 2, None, 4],
        "name": ["Alice", "Bob", "Charlie", "Diana"],
        "value": [100, 200, 300, 400]
    })
    write_deltalake(str(tmp_path), df1.to_arrow(), mode="overwrite")
    
    # Add more data (version 1)
    df2 = pl.DataFrame({
        "id": [5, None],
        "name": ["Eve", "Frank"],
        "value": [500, 600]
    })
    write_deltalake(str(tmp_path), df2.to_arrow(), mode="append")
    
    # Overwrite some data (version 2)
    df3 = pl.DataFrame({
        "id": [1, 7],
        "name": ["Alice_Updated", "Grace"],
        "value": [150, 700]
    })
    write_deltalake(str(tmp_path), df3.to_arrow(), mode="append")
    
    return str(tmp_path)


@pytest.fixture
def table_with_deletes(tmp_path):
    """Create a Delta table with deleted records."""
    # Initial data
    df1 = pl.DataFrame({
        "id": [1, 2, 3, None, 5],
        "name": ["Alice", None, "Charlie", "Diana", "Eve"],
        "value": [100, 200, None, 400, 500]
    })
    write_deltalake(str(tmp_path), df1.to_arrow(), mode="overwrite")
    
    # Add more data
    df2 = pl.DataFrame({
        "id": [6, 7],
        "name": ["Frank", "Grace"],
        "value": [600, 700]
    })
    write_deltalake(str(tmp_path), df2.to_arrow(), mode="append")
    
    # Delete some records by rewriting table without them
    df3 = pl.DataFrame({
        "id": [1, 3, 6, 7, 8],
        "name": ["Alice", "Charlie", "Frank", "Grace", "Henry"],
        "value": [100, 300, 600, 700, 800]
    })
    write_deltalake(str(tmp_path), df3.to_arrow(), mode="overwrite")
    
    return str(tmp_path)


@pytest.fixture
def partitioned_table_with_versions(tmp_path):
    """Create a partitioned Delta table with multiple versions."""
    # Initial data (version 0)
    df1 = pl.DataFrame({
        "id": [1, 2, 3, 4],
        "region": ["US", "US", "EU", "EU"],
        "name": ["Alice", "Bob", "Charlie", "Diana"],
        "value": [100, 200, 300, 400]
    })
    write_deltalake(str(tmp_path), df1.to_arrow(), mode="overwrite", partition_by=["region"])
    
    # Add more data (version 1)
    df2 = pl.DataFrame({
        "id": [5, 6],
        "region": ["APAC", "APAC"],
        "name": ["Eve", "Frank"],
        "value": [500, 600]
    })
    write_deltalake(str(tmp_path), df2.to_arrow(), mode="append", partition_by=["region"])
    
    return str(tmp_path)


class TestSummarizeTable:
    """Test the summarize_table function with various Delta table scenarios."""

    def test_summarize_simple_unpartitioned_table(self, simple_unpartitioned_table):
        """Test summarizing a simple unpartitioned Delta table."""
        summary = summarize_table(simple_unpartitioned_table)
        
        # Check basic attributes
        assert isinstance(summary, TableSummary)
        assert summary.version == 0
        assert isinstance(summary.last_commit_timestamp, datetime.datetime)
        
        # Check table statistics
        assert summary.table_statistics.num_files == 1
        assert summary.table_statistics.num_records == 3
        assert summary.table_statistics.total_size_bytes > 0
        assert summary.table_statistics.num_partitions == 0
        
        # Check schema
        assert len(summary.schema_) == 3
        field_names = [field.name for field in summary.schema_]
        assert "id" in field_names
        assert "name" in field_names
        assert "value" in field_names
        
        # Check metadata
        assert isinstance(summary.metadata, TableMetadata)
        assert summary.metadata.id is not None
        assert summary.metadata.partition_columns == []
        assert isinstance(summary.metadata.created_time, datetime.datetime)
        
        # Check column statistics
        assert "id" in summary.column_statistics
        assert "name" in summary.column_statistics
        assert "value" in summary.column_statistics
        
        # Verify min/max values
        id_stats = summary.column_statistics["id"]
        assert id_stats.min == 1
        assert id_stats.max == 3
        assert id_stats.null_count == 1
        
        value_stats = summary.column_statistics["value"]
        assert value_stats.min == 100
        assert value_stats.max == 300
        assert value_stats.null_count == 0

        name_stats = summary.column_statistics["name"]
        assert name_stats.min == "Alice"
        assert name_stats.max == "Charlie"
        assert name_stats.null_count == 0

    def test_summarize_partitioned_table(self, partitioned_table):
        """Test summarizing a partitioned Delta table."""
        summary = summarize_table(partitioned_table)
        
        # Check basic attributes
        assert isinstance(summary, TableSummary)
        assert summary.version == 0
        
        # Check table statistics
        assert summary.table_statistics.num_files == 3  # One file per partition
        assert summary.table_statistics.num_records == 6
        assert summary.table_statistics.total_size_bytes > 0
        assert summary.table_statistics.num_partitions == 3  # 3 unique regions
        
        # Check partition columns
        assert summary.metadata.partition_columns == ["region"]
        
        # Check that we have statistics for all columns including partition column
        assert "region" in summary.column_statistics
        assert "id" in summary.column_statistics
        assert "name" in summary.column_statistics
        assert "value" in summary.column_statistics
        
        # Check partition column statistics (should come from partition values)
        region_stats = summary.column_statistics["region"]
        # Note: partition values are strings in the metadata
        assert region_stats.min == "APAC"
        assert region_stats.max == "US"
        assert region_stats.null_count == 0

    def test_summarize_table_with_multiple_versions(self, table_with_multiple_versions):
        """Test summarizing a table with multiple versions."""
        summary = summarize_table(table_with_multiple_versions)
        
        # Should reflect the latest version
        assert summary.version == 2
        
        # Check that it includes data from all active files
        assert summary.table_statistics.num_records == 8
        assert summary.table_statistics.total_size_bytes > 0
        
        # Check column statistics reflect the range across all active files
        id_stats = summary.column_statistics["id"]
        assert id_stats.min == 1
        assert id_stats.max == 7
        assert id_stats.null_count == 2
        
        value_stats = summary.column_statistics["value"]
        assert value_stats.min == 100
        assert value_stats.max == 700

    def test_summarize_table_with_deletes(self, table_with_deletes):
        """Test summarizing a table with deleted records (overwritten files)."""
        summary = summarize_table(table_with_deletes)
        
        # Should reflect the latest version after deletes
        assert summary.version == 2
        
        # Should only include active files, not deleted ones
        assert summary.table_statistics.num_records == 5  # Final state after overwrites
        assert summary.table_statistics.total_size_bytes > 0
        
        # Column statistics should reflect only active data
        id_stats = summary.column_statistics["id"]
        assert id_stats.min == 1
        assert id_stats.max == 8
        assert id_stats.null_count == 0  # No nulls in final state

        name_stats = summary.column_statistics["name"]
        assert name_stats.min == "Alice"
        assert name_stats.max == "Henry"
        assert name_stats.null_count == 0  # No nulls in final 
        
        value_stats = summary.column_statistics["value"]
        assert value_stats.min == 100
        assert value_stats.max == 800
        assert value_stats.null_count == 0  # No nulls in final state

    def test_summarize_partitioned_table_with_versions(self, partitioned_table_with_versions):
        """Test summarizing a partitioned table with multiple versions."""
        summary = summarize_table(partitioned_table_with_versions)
        
        # Should reflect the latest version
        assert summary.version == 1
        
        # Check partition columns
        assert summary.metadata.partition_columns == ["region"]
        
        # Should have more files due to partitioning and versions
        assert summary.table_statistics.num_files >= 3  # At least one per region
        assert summary.table_statistics.num_records == 6
        
        # Check that partition column statistics work correctly
        assert "region" in summary.column_statistics

        region_stats = summary.column_statistics["region"]
        assert region_stats.min == "APAC"
        assert region_stats.max == "US"
        assert region_stats.null_count == 0

    def test_summarize_table_schema_fields(self, simple_unpartitioned_table):
        """Test that schema fields are correctly extracted."""
        summary = summarize_table(simple_unpartitioned_table)
        
        # Check schema fields
        assert len(summary.schema_) == 3
        
        # Find each field
        id_field = next(f for f in summary.schema_ if f.name == "id")
        name_field = next(f for f in summary.schema_ if f.name == "name")
        value_field = next(f for f in summary.schema_ if f.name == "value")
        
        # Check field properties
        assert "long" in id_field.type
        assert id_field.nullable is True
        assert isinstance(id_field.metadata, dict)
        
        assert "string" in name_field.type
        assert name_field.nullable is True
        
        assert "long" in value_field.type
        assert value_field.nullable is True

    def test_summarize_table_metadata_structure(self, simple_unpartitioned_table):
        """Test that metadata is correctly structured."""
        summary = summarize_table(simple_unpartitioned_table)
        
        # Check metadata structure
        metadata = summary.metadata
        assert isinstance(metadata, TableMetadata)
        assert metadata.id is not None
        assert len(metadata.id) > 0
        assert metadata.name is None  # No table name set
        assert metadata.description is None  # No description set
        assert metadata.partition_columns == []
        assert isinstance(metadata.created_time, datetime.datetime)
        assert isinstance(metadata.configuration, dict)

    def test_summarize_table_timestamp_extraction(self, table_with_multiple_versions):
        """Test that timestamp extraction works correctly."""
        summary = summarize_table(table_with_multiple_versions)
        
        # Check that timestamp is extracted
        assert isinstance(summary.last_commit_timestamp, datetime.datetime)
        
        # Should be a reasonable timestamp (not too far in past/future)
        now = datetime.datetime.now()
        time_diff = abs((now - summary.last_commit_timestamp).total_seconds())
        assert time_diff < 60  # Less than 60 seconds difference

    def test_summarize_table_handles_empty_table_gracefully(self, tmp_path):
        """Test that summarize_table handles tables with no data gracefully."""
        # Create an empty table
        df = pl.DataFrame({
            "id": [],
            "name": [],
            "value": []
        }, schema={"id": pl.Int64, "name": pl.Utf8, "value": pl.Int64})
        
        write_deltalake(str(tmp_path), df.to_arrow(), mode="overwrite")
        
        summary = summarize_table(str(tmp_path))
        
        # Should handle empty table without errors
        assert isinstance(summary, TableSummary)
        assert summary.version == 0
        assert summary.table_statistics.num_records == 0
        assert summary.table_statistics.num_files == 0
        assert summary.table_statistics.total_size_bytes == 0
        
        # Schema should still be present
        assert len(summary.schema_) == 3
