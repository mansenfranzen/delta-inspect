import pytest
import polars as pl
from deltalake import write_deltalake, DeltaTable
from delta_inspect.util.table_loader import load_table


@pytest.fixture
def simple_delta_table(tmp_path):
    """Create a simple Delta table for testing."""
    df = pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "value": [100, 200, 300]
    })
    
    write_deltalake(str(tmp_path), df.to_arrow(), mode="overwrite")
    return str(tmp_path)


@pytest.fixture
def delta_table_with_updates(tmp_path):
    """Create a Delta table with multiple versions."""
    # Initial data (version 0)
    df1 = pl.DataFrame({
        "id": [1, 2, 3, 4],
        "name": ["Alice", "Bob", "Charlie", "Diana"],
        "value": [100, 200, 300, 400]
    })
    write_deltalake(str(tmp_path), df1.to_arrow(), mode="overwrite")
    
    # Add more data (version 1)
    df2 = pl.DataFrame({
        "id": [5, 6],
        "name": ["Eve", "Frank"],
        "value": [500, 600]
    })
    write_deltalake(str(tmp_path), df2.to_arrow(), mode="append")
    
    return str(tmp_path)


class TestLoadTable:
    """Test our load_table utility function."""
    
    def test_loads_delta_table_successfully(self, simple_delta_table):
        """Test that load_table returns a DeltaTable instance."""
        delta_table = load_table(simple_delta_table)
        assert isinstance(delta_table, DeltaTable)
    
    def test_raises_error_for_invalid_path(self):
        """Test that load_table raises an error for invalid paths."""
        with pytest.raises(Exception):
            load_table("nonexistent/path")
    
    def test_raises_error_for_non_delta_directory(self, tmp_path):
        """Test that load_table raises an error for directories that aren't Delta tables."""
        with pytest.raises(Exception):
            load_table(str(tmp_path))
    
    def test_load_table_returns_consistent_results(self, simple_delta_table):
        """Test that loading the same table multiple times returns the same instance type."""
        table1 = load_table(simple_delta_table)
        table2 = load_table(simple_delta_table)
        
        # Both should be DeltaTable instances
        assert isinstance(table1, DeltaTable)
        assert isinstance(table2, DeltaTable)
        assert table1.version() == table2.version()