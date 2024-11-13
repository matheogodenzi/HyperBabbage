from abc import ABC, abstractmethod
import re
from typing import List
import pandas as pd

class ColumnCleaningStrategy(ABC):

    @abstractmethod
    def fill(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

class Preprocessing:

    def __init__(self, strategies: List[ColumnCleaningStrategy]) -> None:
        self.strategies = strategies
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        cleaned_df = df.copy()
        for strategy in self.strategies:
            cleaned_df = strategy.fill(cleaned_df)
        return cleaned_df
        

class CleanNumericAtrributesStrategy(ColumnCleaningStrategy):
    def keep_just_numeric(self, value, new_class = -1):
        if type(value) != str:
            return new_class
        
        ## One or more non-digit charachters should be replaced
        cleaned_val = re.sub(r'[^\d.]+','', str(value)) ## There are random float / str in the dataset?? -> convert to str
        if(cleaned_val == ''): # It didn't contain any number?
            return new_class
        return float(cleaned_val)

    def fill(self, df: pd.DataFrame) -> pd.DataFrame:
        affinity_cols = ["Ki (nM)", "Kd (nM)"] ## Measures of binding affinity
        ec_ic = ["EC50 (nM)", "IC50 (nM)"] #### Measures of inhibitory (IC50) and effective concentrations (EC50)
        bind_unbind = ["kon (M-1-s-1)", "koff (s-1)"] ## Rates of binding / unbinding of ligands

        binding_ligand_efficency_cols = affinity_cols + ec_ic + bind_unbind
        filtered_df = df.copy()
        for af_col in binding_ligand_efficency_cols:
            filtered_df[af_col] = filtered_df[af_col].apply(lambda x: self.keep_just_numeric(x, np.NaN))
        
        return filtered_df
        
            
