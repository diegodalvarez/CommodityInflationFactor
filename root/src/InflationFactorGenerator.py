# -*- coding: utf-8 -*-
"""
Created on Tue Nov 12 13:13:53 2024

@author: Diego
"""

import os
import numpy as np
import pandas as pd
from   InflationFactorDataPreprocess import DataPrepocess

class FactorModel(DataPrepocess):
    
    def __init__(self):
        
        super().__init__()
        
        self.deciles      = 5
        self.keep_deciles = ["D1", "D5"]
        self.factor_path  = os.path.join(self.data_path, "FactorRtn")
        
        if os.path.exists(self.factor_path) == False: os.makedirs(self.factor_path)
        
    def _get_decile(self, df: pd.DataFrame, d: int) -> pd.DataFrame: 
            
        df_out = (df.assign(
            decile = lambda x: pd.qcut(
                x      = x.beta, 
                q      = d, 
                labels = ["D{}".format(i + 1) for i in range(d)])))
    
        return df_out
    
    def _shift_decile(self, df: pd.DataFrame) -> pd.DataFrame:
        
        df_out = (df.sort_values(
            "date").
            assign(
                lag_beta   = lambda x: x.beta.shift(),
                lag_decile = lambda x: x.decile.shift()).
            dropna())
        
        return df_out
    
    def _get_weighting(self, df: pd.DataFrame) -> pd.DataFrame:
        
        print(df.name)
        df_lower = (df.query(
            "lag_decile == @self.keep_deciles[0]").
            sort_values("lag_beta"))
        
        df_upper = (df.query(
            "lag_decile == @self.keep_deciles[1]").
            sort_values("lag_beta"))
        
        n = len(df_lower)
        lower_weights = np.exp(np.log(0.5) / n * np.arange(n)) / np.exp(np.log(0.5) / n * np.arange(n)).sum() / 2
        
        n = len(df_upper)
        upper_weights = np.exp(np.log(0.5) / n * np.arange(n)) / np.exp(np.log(0.5) / n * np.arange(n)).sum() / 2
        
        df_tmp = (pd.concat([
            df_lower.assign(weight = -lower_weights),
            df_upper.assign(weight = upper_weights)]))
        
        return df_tmp
        
    def generate_factor(self) -> pd.DataFrame: 
        
        file_path = os.path.join(self.factor_path, "IndividualSecRtn.parquet")
        try:
            
            df_weighting = pd.read_parquet(path = file_path, engine = "pyarrow")
            
        except: 
            
            df_prep = (self.get_rolling_beta().drop(
                columns = ["group_var"]).
                assign(group_var = lambda x: x.name + " " + x.variable))
            
            ## due to holidays we need to remove days in which the data is poor
            ## and a holiday occurs and remove days in which less than 10 
            ## securities occur
            
            bad_dates = (df_prep[
                ["security", "date"]].
                groupby(["security", "date"]).
                head(1).
                groupby("date").
                agg("count").
                sort_values("security").
                query("security < 10").
                index.
                to_list())
            
            df_decile = (df_prep.query(
                "date != @bad_dates").
                groupby(["group_var", "date"]).
                apply(self._get_decile, self.deciles).
                reset_index(drop = True).
                assign(tmp_var = lambda x: x.security + " " + x.group_var).
                groupby("tmp_var").
                apply(self._shift_decile).
                reset_index(drop = True).
                drop(columns = ["tmp_var", "decile"]).
                query("lag_decile == @self.keep_deciles"))
            
            df_weighting = (df_decile.groupby([
                "date", "group_var"]).
                apply(self._get_weighting).
                reset_index(drop = True).
                assign(factor_rtn = lambda x: x.weight * x.PX_rtn))
            
            df_weighting.to_parquet(path = file_path, engine = "pyarrow")
        
        return df_weighting
    
    def generate_factor_rtn(self) -> pd.DataFrame:
        
        file_path = os.path.join(self.factor_path, "InflationReturns.parquet")
        try:
            
            df_factor = pd.read_parquet(path = file_path, engine = "pyarrow")
            
        except:
            
            df_factor = (self.generate_factor()[
                ["date", "factor_rtn", "group_var"]].
                groupby(["date", "group_var"]).
                agg("sum").
                reset_index())
            
            df_factor.to_parquet(path = file_path, engine = "pyarrow")
        
        return df_factor
    
def main():
    
    FactorModel().generate_factor()
    FactorModel().generate_factor_rtn()
    
if __name__ == "__main__": main()