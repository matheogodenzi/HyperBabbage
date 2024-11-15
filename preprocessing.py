from abc import ABC, abstractmethod
import re
from typing import List
import pandas as pd

"""
preprocessing.py contains every class that handles preprocessing the BindingDB or the merged dataframe with DrugBank

    -> ColumnCleaningStrategy: Used to collect strategies that are used to cleaning the features
        -> CleanNumericAtrributesStrategy: Checks numeric columns and does the following:
            -> Replaces NaN values with the given new_class (default: -1)
            -> The values represented in not only numbers but with characters are cleaned: >21 -> 21
            -> values are stored in float

    -> Preprocessing: Gets a list of strategies and executes them

"""
class ColumnCleaningStrategy(ABC):

    @abstractmethod
    def fill(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

class ColumnClean:
    def __init__(self, old_column_name: str, new_column_name: str, clean = lambda x: x) -> None:
        self.old_column_name = old_column_name
        self.new_column_name = new_column_name
        self.clean = clean
    def run(self, old_df, new_df):
        new_df[self.new_column_name] = old_df[self.old_column_name].apply(lambda x: self.clean(x))


class Preprocessing:
    def __init__(self, strategies: List[ColumnClean]) -> None:
        self.strategies = strategies
    
    def transform(self, old_df: pd.DataFrame) -> pd.DataFrame:
        new_df = pd.DataFrame(index=old_df.index)
        for stragegy in self.strategies:
            stragegy.run(old_df, new_df)
        return new_df
    
    def get_used_old_columns(self):
        return [strategy.old_column_name for strategy in self.strategies]

        

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
            filtered_df[af_col] = filtered_df[af_col].apply(lambda x: self.keep_just_numeric(x))
        
        return filtered_df
        
            
