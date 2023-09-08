# -*- coding: utf-8 -*-
"""
Created on Wed Aug 17 10:57:03 2022

@author: Mitch
"""

import pandas as pd
import os




GAMS_MAPS_DIR = r"gams_maps"



class GamsMaps:
    
    def __init__(self,data_dir):
        
        self.data = {}
        for _,_,files in os.walk(os.path.join(data_dir,GAMS_MAPS_DIR)):
            for f in files:
                if f[-4:] == ".csv":
                    self.data[f[:-4]] = pd.read_csv(os.path.join(data_dir,GAMS_MAPS_DIR,f))
                    

    def __getitem__(self,key):
        return self.data[key]








if __name__ == "__main__":

    gams_maps = GamsMaps(r"C:\Users\Mitch\Documents\WiNDC\windc_raw_data\windc_3_0_0")                   