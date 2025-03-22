from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Callable, Optional
import pandas as pd
from .exceptions import FactorNotFoundError, DataLoadError

class Factor(ABC):
    """Base class for all factors."""
    
    def __init__(self, name: str, category: str):
        self.name = name
        self.category = category
    
    @abstractmethod
    def fetch(self, vquant: 'Vquant', ticker: str) -> pd.Series:
        """Fetch or compute factor data for a ticker."""
        pass
    
    @property
    def full_name(self) -> str:
        return f"{self.category}.{self.name}"

class StoredFactor(Factor):
    """Factor loaded from storage."""
    
    def fetch(self, vquant: 'Vquant', ticker: str) -> pd.Series:
        path = vquant.storage_adapter.get_path(
            vquant.base_url, vquant.market, ticker, vquant.timeframe, self.category, self.name
        )
        try:
            df = pd.read_parquet(path)
            return df["value"]  # Assumes a 'value' column
        except FileNotFoundError:
            raise FactorNotFoundError(f"Factor {self.full_name} not found for {ticker} at {path}")
        except Exception as e:
            raise DataLoadError(f"Failed to load {self.full_name} for {ticker}: {str(e)}")

class ComputedFactor(Factor):
    """Factor computed from OHLCV or other factors."""
    
    def __init__(self, name: str, category: str, fn: Callable[[pd.DataFrame], pd.Series], 
                 dependencies: Optional[list[str]] = None):
        super().__init__(name, category)
        self.fn = fn
        self.dependencies = dependencies or ["ohlcv.close"]  # Default dependency
    
    def fetch(self, vquant: 'Vquant', ticker: str) -> pd.Series:
        # Fetch required dependencies
        dep_data = vquant.get_factors([ticker], list(self.dependencies))
        return self.fn(dep_data)