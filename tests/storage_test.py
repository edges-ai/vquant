# tests/test_storage.py
import pytest
import pandas as pd
import duckdb
import os
import shutil
from vquant.storage import DuckDBFactorStorage

# Test fixtures
@pytest.fixture
def storage(tmp_path):
    """Create a DuckDBFactorStorage instance with temp directory."""
    base_path = os.path.join(tmp_path, "data", "dim")
    os.makedirs(base_path, exist_ok=True)
    return DuckDBFactorStorage(base_path=base_path)

@pytest.fixture
def sample_ohlcv_df():
    """Sample OHLCV DataFrame."""
    return pd.DataFrame({
        'date': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03']),
        'open': [100.0, 101.0, 102.0],
        'close': [101.0, 102.0, 103.0],
        'high': [102.0, 103.0, 104.0],
        'low': [99.0, 100.0, 101.0],
        'volume': [1000, 1100, 1200]
    })

@pytest.fixture
def sample_rsi_df():
    """Sample RSI DataFrame."""
    return pd.DataFrame({
        'date': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03']),
        'rsi_14': [70.0, 71.0, 72.0],
        'rsi_20': [65.0, 66.0, 67.0]
    })

# Test cases
def test_init(storage):
    """Test storage initialization."""
    assert os.path.exists(storage.base_path)
    assert isinstance(storage.conn, duckdb.DuckDBPyConnection)

def test_get_file_path(storage):
    """Test file path construction."""
    path = storage._get_file_path("stocks_vn", "AAA", "1d", "ohlcv")
    expected = os.path.join(storage.base_path, "stocks_vn", "instrument", "AAA", "1d", "ohlcv.parquet")
    assert path == expected

def test_load_factor(storage, sample_ohlcv_df):
    """Test loading factor data from parquet."""
    # Save sample data first
    file_path = storage._get_file_path("stocks_vn", "AAA", "1d", "ohlcv")
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    sample_ohlcv_df.to_parquet(file_path)
    
    # Test loading
    df = storage.load_factor("stocks_vn", "AAA", "1d", "ohlcv", ["close", "volume"])
    
    # Verify returned DataFrame
    pd.testing.assert_frame_equal(
        df,
        sample_ohlcv_df[['date', 'close', 'volume']],
        check_dtype=False
    )

def test_join_factors(storage, sample_ohlcv_df, sample_rsi_df):
    """Test joining multiple factor files."""
    # Save sample data files
    ohlcv_path = storage._get_file_path("stocks_vn", "AAA", "1d", "ohlcv")
    rsi_path = storage._get_file_path("stocks_vn", "AAA", "1d", "technical")
    os.makedirs(os.path.dirname(ohlcv_path), exist_ok=True)
    sample_ohlcv_df.to_parquet(ohlcv_path)
    sample_rsi_df.to_parquet(rsi_path)
    
    # Test joining
    df = storage.join_factors(
        "stocks_vn",
        "AAA",
        "1d",
        [("ohlcv", "close"), ("technical", "rsi_14")]
    )
    
    # Verify result
    assert 'close' in df.columns
    assert 'rsi_14' in df.columns
    assert len(df) == 3
    pd.testing.assert_series_equal(
        df['close'],
        sample_ohlcv_df['close'],
        check_dtype=False
    )
    pd.testing.assert_series_equal(
        df['rsi_14'],
        sample_rsi_df['rsi_14'],
        check_dtype=False
    )

def test_save_factor_new_file(storage):
    """Test saving factor data when no file exists."""
    new_factor_df = pd.DataFrame({
        'date': pd.to_datetime(['2023-01-01', '2023-01-02']),
        'volatility_20': [0.5, 0.6]
    })
    
    storage.save_factor(new_factor_df, "stocks_vn", "AAA", "1d", "technical", "volatility_20")
    
    # Verify file exists and contains correct data
    file_path = storage._get_file_path("stocks_vn", "AAA", "1d", "technical")
    assert os.path.exists(file_path)
    
    saved_df = pd.read_parquet(file_path)
    pd.testing.assert_frame_equal(
        saved_df[['date', 'volatility_20']],
        new_factor_df,
        check_dtype=False
    )

def test_save_factor_existing_file(storage):
    """Test saving factor data to an existing file."""
    # Create initial file
    initial_df = pd.DataFrame({
        'date': pd.to_datetime(['2023-01-01', '2023-01-02']),
        'volatility_20': [0.5, 0.6]
    })
    file_path = storage._get_file_path("stocks_vn", "AAA", "1d", "technical")
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    initial_df.to_parquet(file_path)
    
    # Save new data
    new_factor_df = pd.DataFrame({
        'date': pd.to_datetime(['2023-01-02', '2023-01-03']),
        'volatility_20': [0.7, 0.8]
    })
    storage.save_factor(new_factor_df, "stocks_vn", "AAA", "1d", "technical", "volatility_20")
    
    # Verify merged result
    saved_df = pd.read_parquet(file_path)
    expected_df = pd.DataFrame({
        'date': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03']),
        'volatility_20': [0.5, 0.7, 0.8]
    })
    pd.testing.assert_frame_equal(
        saved_df[['date', 'volatility_20']],
        expected_df,
        check_dtype=False
    )

# Cleanup fixture
@pytest.fixture(autouse=True)
def cleanup(tmp_path):
    """Cleanup temp directory after each test."""
    yield
    shutil.rmtree(tmp_path, ignore_errors=True)

if __name__ == "__main__":
    pytest.main(["-v", __file__])