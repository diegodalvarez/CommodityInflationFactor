# -*- coding: utf-8 -*-
"""
Created on Fri Oct 18 12:03:32 2024

@author: Diego
"""

import os
import pandas as pd
import pandas_datareader as web

class InflationDataManager:
    
    def __init__(self) -> None:
        
        self.root_path      = os.path.abspath(
            os.path.join((os.path.abspath(
                os.path.join(os.getcwd(), os.pardir))), os.pardir))
        
        self.data_path      = os.path.join(self.root_path, "data")
        self.raw_data_path  = os.path.join(self.data_path, "RawData")
        
        if os.path.exists(self.data_path) == False: os.makedirs(self.data_path)
        if os.path.exists(self.raw_data_path) == False: os.makedirs(self.raw_data_path)
        
        self.bbg_xlsx_path = r"C:\Users\Diego\Desktop\app_prod\BBGData\root\BBGTickers.xlsx"
        self.bbg_data_path = r"C:\Users\Diego\Desktop\app_prod\BBGData\data"
        self.df_tickers    = (pd.read_excel(
            io = self.bbg_xlsx_path, sheet_name = "tickers"))
        
        self.bbg_fut_path   = r"C:\Users\Diego\Desktop\app_prod\BBGFuturesManager\root\fut_tickers.xlsx"
        self.bbg_front_path = r"C:\Users\Diego\Desktop\app_prod\BBGFuturesManager\data\PXFront"
        self.df_fut_tickers = (pd.read_excel(
            io = self.bbg_fut_path, sheet_name = "px"))
        
        self.bad_breakevens = [
            "USGGBE01 Index", "USGGBE09 Index", "USGGBE06 Index", 
            "USGGBE08 Index", "USGGBE03 Index"]
        
    def _get_rtn(self, df: pd.DataFrame) -> pd.DataFrame:
        
        return(df.sort_values(
            "date").
            assign(PX_rtn = lambda x: x.PX_LAST.pct_change()).
            dropna())
        
    def get_commodity_futures(self, verbose = False) -> pd.DataFrame: 
        
        file_path = os.path.join(self.raw_data_path, "CommodFutures.parquet")
        try:
            
            if verbose == True: print("Trying to find commodity data")
            df_out = pd.read_parquet(path = file_path, engine = "pyarrow")
            if verbose == True: print("Found data\n")
            
        except: 
    
            if verbose == True: print("Getting the Commodity data")
            tickers = (self.df_fut_tickers.query(
                "kind == 'Commodity'").
                contract.
                drop_duplicates().
                sort_values().
                to_list())
            
            paths = [os.path.join(
                self.bbg_front_path, ticker + ".parquet")
                for ticker in tickers]
            
            df_out = (pd.read_parquet(
                path = paths, engine = "pyarrow").
                assign(security = lambda x: x.security.str.split(" ").str[0]).
                groupby("security").
                apply(self._get_rtn).
                reset_index(drop = True))
            
            if verbose == True: print("Saving data\n")
            df_out.to_parquet(path = file_path, engine = "pyarrow")
            
        return df_out
    
    def get_inflation_swap(self, verbose: bool = False) -> pd.DataFrame: 
        
        file_path = os.path.join(self.raw_data_path, "InflationSwaps.parquet")
        try:
            
            if verbose == True: print("Trying to find swap data")
            df_out = pd.read_parquet(path = file_path, engine = "pyarrow")
            if verbose == True: print("Found Data\n")
            
        except: 
        
            if verbose == True: print("Couldn't find data, getting it")
            df_tickers = (self.df_tickers.query(
                "Subcategory == 'Interest Rate Swaps'").
                assign(
                    first  = lambda x: x.Description.str.split(" ").str[0],
                    second = lambda x: x.Description.str.split(" ").str[1]).
                query("first == 'USD' & second == 'Inflation'")
                [["Security", "Description"]].
                rename(columns = {"Security": "security"}))
            
            tickers = (df_tickers.assign(
                tmp = lambda x: x.security.str.split(" ").str[0]).
                tmp.
                drop_duplicates().
                sort_values().
                to_list())
            
            files = [os.path.join(
                self.bbg_data_path, ticker + ".parquet")
                for ticker in tickers]
            
            df_out = (pd.read_parquet(
                path = files, engine = "pyarrow").
                drop(columns = ["variable"]).
                merge(right = df_tickers, how = "inner", on = ["security"]))
            
            if verbose == True: print("Saving data\n")
            df_out.to_parquet(path = file_path, engine = "pyarrow")
        
        return df_out
    
    def get_breakeven(self, verbose: bool = False) -> pd.DataFrame: 
        
        file_path = os.path.join(self.raw_data_path, "BreakevenRates.parquet")
        try:
            
            if verbose == True: print("Trying to find Breakeven data")
            df_out = pd.read_parquet(path = file_path, engine = "pyarrow")
            if verbose == True: print("Found Data\n")
        
        except: 
        
            if verbose == True: print("Couldn't find data, getting it")
            df_tickers = (self.df_tickers.query(
                "Subcategory == 'Miscellaneous Indices'").
                assign(
                    first  = lambda x: x.Description.str.split(" ").str[0],
                    second = lambda x: x.Description.str.split(" ").str[1]).
                query("first == 'US' & second == 'Breakeven'")
                [["Security", "Description"]].
                rename(columns = {"Security": "security"}))
            
            tickers = (df_tickers.assign(
                tmp = lambda x: x.security.str.split(" ").str[0]).
                tmp.
                drop_duplicates().
                sort_values().
                to_list())
            
            files = [os.path.join(
                self.bbg_data_path, ticker + ".parquet")
                for ticker in tickers]
            
            df_out = (pd.read_parquet(
                path = files, engine = "pyarrow").
                drop(columns = ["variable"]).
                merge(right = df_tickers, how = "inner", on = ["security"]).
                query("security != @self.bad_breakevens"))
            
            if verbose == True: print("Saving data\n")
            df_out.to_parquet(path = file_path, engine = "pyarrow")
        
        return df_out
    
    def get_cpi(self, verbose: bool = False) -> pd.DataFrame: 
        
        file_path = os.path.join(self.raw_data_path, "CPI.parquet")
        try:
            
            if verbose == True: print("Trying to find CPI data")
            df_cpi = pd.read_parquet(path = file_path, engine = "pyarrow")
            if verbose == True: print("Found data\n")
        
        except: 
        
            if verbose == True: print("Couldn't find data, collecting it")
            dates = (self.get_commodity_futures()[
                ["date"]].
                agg(["min", "max"]).
                assign(date = lambda x: pd.to_datetime(x.date).dt.date)
                ["date"].
                to_dict())
            
            start_date, end_date = dates["min"], dates["max"]
            tickers              = ["CPIAUCSL"]
            
            df_cpi = (web.DataReader(
                name        = tickers,
                data_source = "fred",
                start       = start_date,
                end         = end_date))
            
            if verbose == True: print("Saving data\n")
            df_cpi.to_parquet(path = file_path, engine = "pyarrow")
        
        return df_cpi
    
    def get_misc_indices(self, verbose: bool = False) -> pd.DataFrame:
        
        file_path = os.path.join(self.raw_data_path, "CommodityIndices.parquet")
        try:
            
            if verbose == True: print("Trying to find Commodity Indices data")
            df_out = pd.read_parquet(path = file_path, engine = "pyarrow")
            if verbose == True: print("Found data\n")
            
        except: 
            
            if verbose == True: print("Couldn't find data, collecting it")
            
            misc_indices = ["BCOM", "EWCI"]
            paths        = [os.path.join(
                self.bbg_data_path, misc_index + ".parquet")
                for misc_index in misc_indices]
            
            df_out = (pd.read_parquet(
                path = paths, engine = "pyarrow").
                rename(columns = {"security": "Security"}).
                merge(right = self.df_tickers, how = "inner", on = ["Security"]).
                drop(columns = ["Frequency", "Subcategory", "Category"]).
                rename(columns = {
                    "Security"   : "security",
                    "Description": "description"}).
                assign(security = lambda x: x.security.str.split(" ").str[0]).
                drop(columns = ["variable"]))
            
            if verbose == True: print("Saving data\n")
            df_out.to_parquet(path = file_path, engine = "pyarrow")
            
        return df_out
         
def main() -> None:
        
    InflationDataManager().get_inflation_swap(verbose = True)
    InflationDataManager().get_breakeven(verbose = True)
    InflationDataManager().get_commodity_futures(verbose = True)
    InflationDataManager().get_cpi(verbose = True)
    InflationDataManager().get_misc_indices(verbose = True)
    
#if __name__ == "__main__": main()