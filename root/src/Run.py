# -*- coding: utf-8 -*-
"""
Created on Wed Nov 13 09:10:59 2024

@author: Diego
"""

from InflationFactorDataCollector import InflationDataManager
from InflationFactorDataPreprocess import DataPrepocess
from InflationFactorGenerator import FactorModel

def main() -> None:
    
    verbose = True
    
    # data prep
    inflation_manager = InflationDataManager()
    inflation_manager.get_inflation_swap(verbose)
    inflation_manager.get_breakeven(verbose)
    inflation_manager.get_commodity_futures(verbose)
    
    # data pre-processing
    data_preprocess = DataPrepocess()
    data_preprocess.inflation_swap_pca(verbose)        
    data_preprocess.breakeven_rate_pca(verbose)
    data_preprocess.get_rolling_beta(verbose)
    
    # generating factor
    factor_model = FactorModel()
    factor_model.generate_factor()
    factor_model.generate_factor_rtn(
    factor_model.generate_monthly_factor()
    factor_model.generate_monthly_factor_rtn())
    
if __name__ == "__main__": main()