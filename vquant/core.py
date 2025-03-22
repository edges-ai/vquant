from typing import List, Optional, Callable, Union, Tuple
import pandas as pd
from .factors import Factor, StoredFactor, ComputedFactor
from .signals import Signal
from .storage import StorageAdapter, DefaultStorageAdapter
from .exceptions import FactorNotFoundError, DataLoadError

class Vquant:
    """Main class for quantitative data access and analysis."""
    
    def __init__(self, market: str, base_url: str, timeframe: str = "1d", 
                 storage_adapter: StorageAdapter = DefaultStorageAdapter()):
        self.market = market
        self.base_url = base_url
        self.timeframe = timeframe
        self.storage_adapter = storage_adapter
    
    def get_ohlcv(self, tickers: List[str], cols: List[str]) -> pd.DataFrame:
        """Fetch OHLCV data for given tickers and columns with multi-level columns."""
        # Load data for all tickers
        result = {}
        for ticker in tickers:
            path = self.storage_adapter.get_path(
                self.base_url, self.market, ticker, self.timeframe, "ohlcv", "ohlcv"
            )
            try:
                df = pd.read_parquet(path)[["date"] + cols].set_index("date")
                result[ticker] = df
            except Exception as e:
                raise DataLoadError(f"Failed to load OHLCV for {ticker}: {str(e)}")
        
        # Get unified date index
        all_dates = pd.concat([df.index.to_series() for df in result.values()]).unique()
        all_dates = pd.Index(sorted(all_dates))
        
        # Create MultiIndex columns: level 0 = factor (OHLCV type), level 1 = ticker
        multi_columns = pd.MultiIndex.from_product(
            [cols, tickers],
            names=['factor', 'ticker']
        )
        
        # Align data and construct final DataFrame
        aligned_data = pd.DataFrame(index=all_dates)
        for ticker in tickers:
            df = result[ticker].reindex(all_dates)
            for col in cols:
                aligned_data[(col, ticker)] = df[col]
        
        aligned_data.columns = multi_columns
        aligned_data.index.name = "date"
        
        return aligned_data
    
    def get_factors(self, tickers: List[str], factors: List[Union[str, Factor]]) -> pd.DataFrame:
        """Fetch or compute multiple factors for given tickers with multi-level columns.
        
        Returns a DataFrame where:
        - Level 1 (top level) columns are factor names
        - Level 2 columns are tickers/assets
        - Index is date
        """
        # Resolve factor specifications
        resolved_factors: List[Factor] = []
        for f in factors:
            if isinstance(f, str):
                category, name = f.split(".", 1) if "." in f else ("technical", f)
                resolved_factors.append(StoredFactor(name, category))
            elif isinstance(f, Factor):
                resolved_factors.append(f)
            else:
                raise ValueError(f"Invalid factor specification: {f}")
        
        # Create MultiIndex columns: level 0 = factor name, level 1 = ticker
        factor_names = [factor.full_name for factor in resolved_factors]
        multi_columns = pd.MultiIndex.from_product(
            [factor_names, tickers],
            names=['factor', 'ticker']
        )
        
        # Load base OHLCV data for all tickers
        base_data = {}
        for ticker in tickers:
            path = self.storage_adapter.get_path(
                self.base_url, self.market, ticker, self.timeframe, "ohlcv", "ohlcv"
            )
            try:
                base_data[ticker] = pd.read_parquet(path).set_index("date")
            except Exception as e:
                raise DataLoadError(f"Failed to load OHLCV for {ticker}: {str(e)}")
        
        # Get unified date index across all tickers
        all_dates = pd.concat([df.index.to_series() for df in base_data.values()]).unique()
        all_dates = pd.Index(sorted(all_dates))
        
        # Construct result DataFrame with multi-level columns
        result_df = pd.DataFrame(index=all_dates, columns=multi_columns)
        for ticker in tickers:
            base_df = base_data[ticker].reindex(all_dates)
            for factor in resolved_factors:
                try:
                    series = factor.fetch(self, ticker)
                    result_df[(factor.full_name, ticker)] = series.reindex(all_dates)
                except Exception as e:
                    raise FactorNotFoundError(f"Failed to compute factor {factor.full_name} for {ticker}: {str(e)}")
        
        result_df.index.name = "date"
        return result_df

    # Rest of the methods remain unchanged...
    
    def get_signals(self, tickers: List[str], signals: List[Signal]) -> pd.DataFrame:
        """Evaluate signals for given tickers."""
        result = []
        for ticker in tickers:
            base_df = self.get_ohlcv([ticker], ["close"])
            for signal in signals:
                series = signal.evaluate(self, ticker)
                base_df[signal.full_name] = series
            base_df["ticker"] = ticker
            result.append(base_df)
        
        return pd.concat(result).reset_index()
    
    def compute_factor(self, name: str, category: str, fn: Callable[[pd.DataFrame], pd.Series], 
                      dependencies: Optional[List[str]] = None) -> ComputedFactor:
        """Create a computed factor."""
        return ComputedFactor(name, category, fn, dependencies)
    
    def create_signal(self, name: str, factors: List[Union[str, Factor]], 
                     condition: Callable[[pd.DataFrame], pd.Series]) -> Signal:
        """Create a signal from factors and an entry condition."""
        return Signal(name, factors, condition)
    
    def study(self, tickers: List[str], factors: List[Union[str, Factor]] = [], 
             signals: List[Signal] = []) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Study factors and signals against price movements."""
        # Fetch base OHLCV data
        data = self.get_ohlcv(tickers, ["close"])
        data["daily_return"] = data.groupby(level=1, axis=1)["close"].pct_change().dropna()
        data = data.reset_index()
        
        # Add factors if provided
        if factors:
            factor_data = self.get_factors(tickers, factors)
            data = data.merge(factor_data.drop(columns=["close"]), 
                            on=["date", "ticker"], 
                            how="left")
        
        # Add signals if provided
        if signals:
            signal_data = self.get_signals(tickers, signals)
            data = data.merge(signal_data.drop(columns=["close"]), 
                            on=["date", "ticker"], 
                            how="left")
        
        # Compute correlations for factors
        factor_cols = [
            (f.full_name if isinstance(f, Factor) else StoredFactor(f.split(".", 1)[1] if "." in f else f, "technical").full_name)
            for f in (factors or [])
        ]
        signal_cols = [s.full_name for s in (signals or [])]
        
        correlations = (
            data.groupby("ticker")
            .apply(lambda x: x[["daily_return"] + factor_cols + signal_cols].corr().loc["daily_return"])
            .drop("daily_return")
            .rename(lambda x: f"corr_{x}")
            .reset_index()
        )
        
        return data, correlations
    
    def list_factors(self, category: Optional[str] = None) -> List[dict]:
        """List available factors."""
        return self.storage_adapter.list_factors(self.base_url, self.market, self.timeframe, category)