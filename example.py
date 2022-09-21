# -*- coding: utf-8 -*-
"""
Created on Thu Aug  4 13:58:11 2022

@author: Mitch
"""

import windc_data as wd


datasets = {
    #"bea_gsp":wd.BeaGsp,
    #"bea_pce":wd.BeaPce,
    #"bea_supply_det":wd.BeaSupplyDet,
    "bea_supply" : wd.BeaSupply,
    #"bea_use_det" : wd.BeaUseDet,
    #"bea_use" : wd.BeaUse,
    #"census_sgf" : wd.CensusSGF,
    #"cfs" : wd.CFS,
    #"eia_crude_price" : wd.EiaCrude,
    #"eia_emissions" : wd.EiaEmissions,
    #"eia_heatrate" : wd.EiaHeatrate,
    #"eia_seds" : wd.EiaSeds,
    #"nass" : wd.Nass,
    #"state_exim" : wd.StateExIm,
    #"emission_rate":wd.EmissionRate
    }



#data_dir = r"C:\Users\Mitch\Documents\WiNDC\windc_raw_data\windc_3_0_0"

#data_dir = r"C:\Users\mphillipson\Documents\WiNDC\windc_raw_data\windc_3_0_0"

data_dir = r"C:\Users\mphillipson\Documents\WiNDC\windc_raw_data\windc_2_1"


w1 = wd.WindcEnvironment(data_dir,
                         load = datasets,
                         verbose = True)
    