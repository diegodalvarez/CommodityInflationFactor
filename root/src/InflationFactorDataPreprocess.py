# -*- coding: utf-8 -*-
"""
Created on Tue Nov 12 12:30:28 2024

@author: Diego
"""

import os
import pandas as pd

import statsmodels.api as sm
from   statsmodels.regression.rolling import RollingOLS

from sklearn.decomposition import PCA
from InflationFactorDataCollector import InflationDataManager

class DataPrepocess(InflationDataManager):
    
    def __init__(self) -> None:
        
        super().__init__()
        self.pca_fitted_path   = os.path.join(self.data_path, "FittedPCValues")
        self.pca_loadings_path = os.path.join(self.data_path, "PCLoadings")
        self.pca_exp_var_path  = os.path.join(self.data_path, "PCExpVar")
        self.ols_path          = os.path.join(self.data_path, "OLSPath")
        self.misc_data_path    = os.path.join(self.data_path, "MiscData")
        self.five_year_forward = os.path.join(self.data_path, "FiveYearForward")
        
        if os.path.exists(self.pca_fitted_path)   == False: os.makedirs(self.pca_fitted_path)
        if os.path.exists(self.pca_loadings_path) == False: os.makedirs(self.pca_loadings_path)
        if os.path.exists(self.pca_exp_var_path)  == False: os.makedirs(self.pca_exp_var_path)
        if os.path.exists(self.ols_path)          == False: os.makedirs(self.ols_path)
        if os.path.exists(self.misc_data_path)    == False: os.makedirs(self.misc_data_path)
        if os.path.exists(self.five_year_forward) == False: os.makedirs(self.five_year_forward)
    
        self.num_comps = 3
        self.window    = 20
        
    def _run_pca(self, df_wider: pd.DataFrame, name: str, verbose: bool = False):

        if verbose == True: print("Working on {}".format(name))
             
        fitted_path = os.path.join(self.pca_fitted_path, "{}.parquet".format(name))   
        try:
            
            if verbose == True: print("Trying to find fitted values")
            df_fitted = pd.read_parquet(path = fitted_path, engine = "pyarrow")
            if verbose == True: print("Found data\n")
            
        except:
            
            if verbose == True: print("Couldn't find data, now generating it")
            df_fitted = (pd.DataFrame(
                data    = PCA(n_components = self.num_comps).fit_transform(df_wider),
                columns = ["PC{}".format(i + 1) for i in range(self.num_comps)],
                index   = df_wider.index))
            
            if verbose == True: print("Saving the data\n")
            df_fitted.to_parquet(path = fitted_path, engine = "pyarrow")
            
        loadings_path = os.path.join(self.pca_loadings_path, "{}.parquet".format(name))      
        try:
            
            if verbose == True: print("Trying to find loadings values")
            df_loadings = pd.read_parquet(path = loadings_path, engine = "pyarrow")
            if verbose == True: print("Found data\n")    
            
        except: 
        
            if verbose == True: print("Couldn't find data, now generating it")
            df_loadings = (pd.DataFrame(
                data    = PCA(n_components = self.num_comps).fit(df_wider).components_,
                index   = ["PC{}".format(i + 1) for i in range(self.num_comps)],
                columns = df_wider.columns.to_list()))
            
            if verbose == True: print("Saving the data\n")
            df_loadings.to_parquet(path = loadings_path, engine = "pyarrow")
            
            
        var_path = os.path.join(self.pca_exp_var_path, "{}.parquet".format(name))   
        try:
            
            if verbose == True: print("Trying to find fitted values")
            df_var = pd.read_parquet(path = var_path, engine = "pyarrow")
            if verbose == True: print("Found data\n")
            
        except:
            
            if verbose == True: print("Couldn't find data, now generating it")
            df_var = (pd.DataFrame(
                data    = PCA(n_components = self.num_comps).fit(df_wider).explained_variance_ratio_ ,
                columns = ["exp_var"],
                index   = ["PC{}".format(i + 1) for i in range(self.num_comps)]))
        
            if verbose == True: print("Saving the data\n")
            df_var.to_parquet(path = var_path, engine = "pyarrow")
        
    def inflation_swap_pca(self, verbose: bool = False) -> pd.DataFrame: 
        
        df_wider = (self.get_inflation_swap().assign(
            security = lambda x: x.security.str.split(" ").str[0])
            [["date", "security", "value"]].
            pivot(index = "date", columns = "security", values = "value").
            dropna())

        self._run_pca(df_wider, "InflationSwaps", verbose)
        
    def breakeven_rate_pca(self, verbose: bool = False) -> pd.DataFrame: 
        
        df_wider = (self.get_breakeven().assign(
            security = lambda x: x.security.str.split(" ").str[0])
            [["date", "security", "value"]].
            pivot(index = "date", columns = "security", values = "value").
            fillna(0))
        
        self._run_pca(df_wider, "BreakevenRate", verbose)
        
    def _run_regression(self, df: pd.DataFrame, window: int, verbose: bool = False) -> pd.DataFrame: 
        
        if verbose == True: print("Working on {}".format(df.name))
        df_tmp = df.sort_values("date")
        model = (RollingOLS(
            endog  = df_tmp.PX_rtn,
            exog   = sm.add_constant(df_tmp.value),
            window = window).
            fit())
        
        df_out = (model.params.rename(
            columns = {
                "const": "alpha",
                "value": "beta"}).
            assign(date = df_tmp.date).
            merge(right = df_tmp, how = "inner", on = ["date"]).
            dropna())
        
        return df_out
        
    def get_rolling_beta(self, verbose: bool = False) -> pd.DataFrame: 
        
        file_path = os.path.join(self.ols_path, "OLSParams.parquet")
        try:
            
            if verbose == True: print("Looking for OLS Params")
            df_combined = pd.read_parquet(path = file_path, engine = "pyarrow")
            if verbose == True: print("Found data\n")
            
        except: 
        
            if verbose == True: print("Couldn't find data, running OLS")
            df_combined = (pd.concat([
                (pd.read_parquet(
                    path = os.path.join(self.pca_fitted_path, file), engine = "pyarrow").
                    assign(name = file.split(".")[0]))
                for file in os.listdir(self.pca_fitted_path)]).
                reset_index().
                melt(id_vars = ["date", "name"]).
                merge(right = self.get_commodity_futures(), how = "inner", on = ["date"]).
                drop(columns = ["PX_LAST"]).
                assign(group_var = lambda x: x.name + " " + x.variable + " " + x.security).
                groupby("group_var").
                apply(self._run_regression, self.window, verbose).
                reset_index(drop = True))
            
            if verbose == True: print("Saving data\n")
            df_combined.to_parquet(path = file_path, engine = "pyarrow")
        
        return df_combined
    
    def prep_inflation_data(self, window: int = 12 * 7, verbose: bool = False) -> pd.DataFrame: 
        
        file_path = os.path.join(self.misc_data_path, "CPI.parquet")
        try:
            
            if verbose == True: print("Searching for CPI data")
            df_out = pd.read_parquet(path = file_path, engine = "pyarrow")
            if verbose == True: print("Found data\n")
        
        except: 
        
            if verbose == True: print("Couldn't find data, collecting it")
            
            df_out = (self.get_cpi().reset_index().rename(
                columns = {
                    "DATE"    : "date",
                    "CPIAUCSL": "cpi_val"}).
                sort_values("date").
                assign(
                    date      = lambda x: pd.to_datetime(x.date).dt.date,
                    cpi_yoy   = lambda x: x.cpi_val.pct_change(periods = 12),
                    roll_mean = lambda x: x.cpi_yoy.rolling(window = window).mean(),
                    roll_std  = lambda x: x.cpi_yoy.rolling(window = window).std(),
                    z_score   = lambda x: (x.cpi_yoy - x.roll_mean) / x.roll_std).
                dropna())
            
            if verbose == True: print("Saving data")
            df_out.to_parquet(path = file_path, engine = "pyarrow")
        
        return df_out
    
    def prep_five_forward_inflation(
            self, 
            verbose: bool = False,
            n_comps: int = 1) -> pd.DataFrame: 
        
        file_path = os.path.join(self.five_year_forward, "FiveYearForwardPrep.parquet")
        try:
            
            if verbose == True: print("Trying to get 5y forward prep data")
            df_out = pd.read_parquet(path = file_path, engine = "pyarrow")
            if verbose == True: print("Found Data\n")
            
        except: 
            
            if verbose == True: print("Couldn't find data, collecting it")
            df_tmp = (self.get_five_year_forward_inflation().assign(
                security = lambda x: x.security.str.split(" ").str[0]).
                pivot(index = "date", columns = "security", values = "value"))
            
            df_mean = (df_tmp.mean(
                axis = 1).
                to_frame(name = "mean_val"))
            
            df_na = df_tmp.dropna()
            df_pc = (pd.DataFrame(
                data    = PCA(n_components = n_comps).fit_transform(df_na),
                columns = ["PC{}".format(i + 1) for i in range(n_comps)],
                index   = df_na.index))
            
            df_out = (df_mean.merge(
                right = df_pc, how = "outer", on = ["date"]).
                reset_index().
                melt(id_vars = "date").
                dropna())
            
            if verbose == True: print("Saving data")
            df_out.to_parquet(path = file_path, engine = "pyarrow")
            
        return df_out
    
    def get_five_forward_rolling_ols(
            self,
            verbose: bool = False) -> pd.DataFrame: 
        
        file_path = os.path.join(self.five_year_forward, "FiveYearForwardOLS.parquet")
        try:
            
            if verbose == True: print("Trying to find 5y forward rolling OLS")
            df_out = pd.read_parquet(path = file_path, engine = "pyarrow")
            if verbose == True: print("Found Data\n")
            
        except:
            
            if verbose == True: print("Couldn't find data, collecting it")
            df_commod = (self.get_commodity_futures().assign(
                date = lambda x: pd.to_datetime(x.date).dt.date))
            
            df_out = (self.prep_five_forward_inflation().assign(
                date = lambda x: pd.to_datetime(x.date).dt.date).
                merge(right = df_commod, how = "inner", on = ["date"]).
                drop(columns = ["PX_LAST"]).
                assign(group_var = lambda x: x.variable + " " + x.security).
                groupby("group_var").
                apply(self._run_regression, self.window, verbose).
                reset_index(drop = True))
            
            if verbose == True: print("Saving data\n")
            df_out.to_parquet(path = file_path, engine = "pyarrow")
            
        return df_out
    
        

def main():

    DataPrepocess().inflation_swap_pca(verbose = True)        
    DataPrepocess().breakeven_rate_pca(verbose = True)
    DataPrepocess().get_rolling_beta(verbose = True)
    DataPrepocess().prep_inflation_data(verbose = True)
    DataPrepocess().prep_five_forward_inflation(verbose = True)
    DataPrepocess().get_five_forward_rolling_ols(verbose = True)
    
if __name__ == "__main__": main()