from abc import ABC, abstractmethod
from typing import Optional

class StorageAdapter(ABC):
    @abstractmethod
    def get_path(self, base_url: str, market: str, instrument: str, timeframe: str, 
                 category: str, name: str) -> str:
        pass
    
    @abstractmethod
    def list_factors(self, base_url: str, market: str, timeframe: str, 
                    category: Optional[str] = None) -> list[dict]:
        pass

class DefaultStorageAdapter(StorageAdapter):
    def get_path(self, base_url: str, market: str, instrument: str, timeframe: str, 
                 category: str, name: str) -> str:
        # https://storage.googleapis.com/edges-quant-data/data/dim/stocks_vn/instrument/AAA/1d/ohlcv.parquet
        return f"{base_url}/data/dim/{market}/instrument/{instrument}/{timeframe}/{name}.parquet"
    
    def list_factors(self, base_url: str, market: str, timeframe: str, 
                    category: Optional[str] = None) -> list[dict]:
        return [{"name": "rsi_14", "category": "technical"}] if category in (None, "technical") else []