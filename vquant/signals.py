from __future__ import annotations
from typing import Callable, List, Union
import pandas as pd
from .factors import Factor, StoredFactor
from .exceptions import FactorNotFoundError, DataLoadError

class Signal:
    """A signal combining factors with an entry condition."""
    
    def __init__(self, name: str, factors: List[Union[str, Factor]], 
                 condition: Callable[[pd.DataFrame], pd.Series]):
        self.name = name
        self.factors = [
            StoredFactor(f.split(".", 1)[1], f.split(".", 1)[0]) if isinstance(f, str) else f
            for f in factors
        ]
        self.condition = condition
    
    def evaluate(self, vquant: 'Vquant', ticker: str) -> pd.Series:
        """Evaluate the signal for a ticker."""
        factor_data = vquant.get_factors([ticker], [f.full_name for f in self.factors])
        return self.condition(factor_data)
    
    @property
    def full_name(self) -> str:
        return f"signal.{self.name}"