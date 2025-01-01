#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 31 20:46:34 2024

@author: diegoalvarez
"""
import os
import numpy as np
import pandas as pd
from   sklearn.decomposition import PCA

import statsmodels.api as sm
from   statsmodels.regression.rolling import RollingOLS

from InflationFactorDataCollector import InflationDataManager

class ForwardInflationPCA(InflationDataManager):
    
    def __init__(self) -> None: 
        
        super().__init__()
        self.forward_path = os.path.join(self.data_path, "ForwardPCABetaFactor")
        if os.path.exists(self.forward_path) == False: os.makedirs(self.forward_path)
        
    def get_forward_inflation_pca(self, verbose: bool = False) -> pd.DataFrame: 
        
        file_path = os.path.join(self.forward_path, "PCAFittedValues.parquet")
        try:
            
            if verbose == True: print("Trying to find PCA fitted values")
            df_out = pd.read_parquet(path = file_path, engine = "pyarrow")
            if verbose == True: print("Found data\n")
            
        except: 
        
            if verbose == True: print("Couldn't find PCA data collecting it now")
            df_wider = (self.get_five_year_forward_inflation().assign(
                security = lambda x: x.security.str.split(" ").str[0]).
                pivot(index = "date", columns = "security", values = "value").
                dropna())
            
            df_out = (pd.DataFrame(
                data    = PCA(n_components = len(df_wider.columns)).fit_transform(df_wider),
                columns = ["PC{}".format(i + 1) for i in range(len(df_wider.columns))],
                index   = df_wider.index).
                dropna())
            
            if verbose == True: print("Saving data")
            df_out.to_parquet(path = file_path, engine = "pyarrow")
        
        return df_out
    
    def _get_ols(self, df: pd.DataFrame, window: int, verbose: bool) -> pd.DataFrame: 
        
        if verbose == True: print("Working on", df.name)
        df_out = (RollingOLS(
            endog  = df.PX_rtn,
            exog   = sm.add_constant(df.fitted_value),
            window = window).
            fit().
            params.
            rename(columns = {
                "const"       : "alpha",
                "fitted_value": "beta"}).
            assign(
                date     = df.date,
                lag_beta = lambda x: x.beta.shift()).
            dropna().
            merge(right = df, how = "inner", on = ["date"]))
        
        return df_out
    
    
    def get_pca_regression(self, window: int = 30, verbose: bool = False) -> pd.DataFrame: 
        
        file_path = os.path.join(self.forward_path, "PCARegressedValues.parquet")
        try:
            
            if verbose == True: print("Looking for PCA Regression OLS data")
            df_out = pd.read_parquet(path = file_path, engine = "pyarrow")
            if verbose == True: print("Found data\n")
            
        except: 
        
            if verbose == True: print("Couldn't find data, collecting it now")    
        
            df_out = (self.get_forward_inflation_pca().diff().dropna().reset_index().melt(
                id_vars = "date").
                rename(columns = {"value": "fitted_value"}).
                assign(date = lambda x: pd.to_datetime(x.date)).
                merge(right = self.get_commodity_futures(), how = "inner", on = ["date"]).
                drop(columns = ["PX_LAST"]).
                assign(group_var = lambda x: x.variable + "_" + x.security).
                groupby("group_var").
                apply(self._get_ols, window, verbose).
                reset_index(drop = True))
            
            if verbose == True: print("Saving data\n")
            df_out.to_parquet(path = file_path, engine = "pyarrow")
            
        return df_out
    
    def _get_quartiles(self, df: pd.DataFrame, q: int) -> pd.DataFrame: 
        
        df_out = (df.assign(
            quartile = lambda x: pd.qcut(
                x      = x.beta, 
                q      = q, 
                labels = ["D{}".format(i + 1) for i in range(q)])))
        
        return df_out
    
    def _lag_decile(self, df: pd.DataFrame) -> pd.DataFrame: 
        
        df_out = (df.sort_values(
            "date").
            assign(lag_decile = lambda x: x.quartile.shift()).
            dropna())
        
        return df_out
    
    def get_deciles(self, n_tiles: int = 5, verbose: bool = False) -> pd.DataFrame: 
        
        file_path = os.path.join(self.forward_path, "SecurityDeciles.parquet")
        try:
            
            if verbose == True: print("Looking for OLS Regression Decile Data")
            df_out = pd.read_parquet(path = file_path, engine = "pyarrow")
            if verbose == True: print("Found data\n")
            
        except: 
        
            if verbose == True: print("Couldn't find data, collecting it now")
            df_tmp = (self.get_pca_regression().rename(columns = {
                "variable": "pc"}))
            
            good_dates = (df_tmp[
                ["date", "pc"]].
                groupby("date").
                agg("count").
                query("pc >= 10").
                index.
                to_list())
            
            df_out = (df_tmp.query(
                "date == @good_dates").
                groupby(["date", "pc"]).
                apply(self._get_quartiles, n_tiles).
                reset_index(drop = True).
                groupby("group_var").
                apply(self._lag_decile).
                reset_index(drop = True))
            
            if verbose == True: print("Saving data\n")
            df_out.to_parquet(path = file_path, engine = "pyarrow")
            
        return df_out
    
    def generate_daily_factor(self, verbose: bool = False) -> pd.DataFrame: 
        
        file_path = os.path.join(self.forward_path, "DailyFactor.parquet")
        try:
            
            if verbose == True: print("Looking for weighting data")
            df_out = pd.read_parquet(path = file_path, engine = "pyarrow")
            if verbose == True: print("Found data\n")
            
        except: 
        
            if verbose == True: print("Couldn't find data, collecting it now")
        
            df_out = (self.get_deciles().assign(
                lag_decile = lambda x: x.lag_decile.astype(str)).
                query("lag_decile == ['D1', 'D5']")
                [["date", "pc", "PX_rtn", "lag_decile"]].
                groupby(["date", "pc", "lag_decile"]).
                agg("mean").
                reset_index().
                pivot(index = ["date", "pc"], columns = "lag_decile", values = "PX_rtn").
                reset_index())
            
            if verbose == True: print("Saving data\n")
            df_out.to_parquet(path = file_path, engine = "pyarrow")
        
        return df_out
    
    def _get_last_day(self, df: pd.DataFrame) -> pd.DataFrame: 
        
        df_out = (df.query(
            "date == date.max()"))
        
        return df_out
    
    def generate_monthly_factor(self, verbose: bool = False) -> pd.DataFrame: 
        
        file_path = os.path.join(self.forward_path, "MonthlyFactor.parquet")
        try:
            
            if verbose == True: print("Looking for monthly weighting data")
            df_out = pd.read_parquet(path = file_path, engine = "pyarrow")
            if verbose == True: print("Found data\n")
            
        except: 
        
            if verbose == True: print("Couldn't find data, collecting it now")
        
            df_tmp = (self.get_deciles().assign(
                month_date = lambda x: x.date.dt.strftime("%Y-%m")))
            
            df_month_key = (df_tmp[
                ["month_date"]].
                sort_values("month_date").
                drop_duplicates().
                assign(next_month = lambda x: x.month_date.shift(-1)).
                dropna())
            
            df_sec = (df_tmp.groupby(
                ["pc", "month_date"]).
                apply(self._get_last_day).
                reset_index(drop = True).
                merge(right = df_month_key, how = "inner", on = ["month_date"])
                [["quartile", "security", "next_month", "pc"]].
                rename(columns = {"next_month": "month_date"}))
            
            df_out = (self.get_commodity_futures().drop(
                columns = ["PX_LAST"]).
                assign(month_date = lambda x: pd.to_datetime(x.date).dt.strftime("%Y-%m")).
                merge(right = df_sec, how = "inner", on = ["month_date", "security"]).
                drop(columns = ["security", "month_date"]).
                assign(quartile = lambda x: x.quartile.astype(str)).
                query("quartile == ['D1', 'D5']").
                groupby(["date", "quartile", "pc"]).
                agg("mean").
                reset_index().
                pivot(index = ["date", "pc"], columns = "quartile", values = "PX_rtn").
                reset_index())
            
            if verbose == True: print("Saving data\n")
            df_out.to_parquet(path = file_path, engine = "pyarrow")
        
        return df_out
        
    
def main() -> None: 
        
    df = ForwardInflationPCA().get_forward_inflation_pca(verbose = True)
    df = ForwardInflationPCA().get_pca_regression(verbose = True)
    df = ForwardInflationPCA().get_deciles(verbose = True)
    df = ForwardInflationPCA().generate_daily_factor(verbose = True)
    df = ForwardInflationPCA().generate_monthly_factor(verbose = True)
    
if __name__ == "__main__": main()