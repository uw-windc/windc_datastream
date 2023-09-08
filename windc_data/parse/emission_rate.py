# -*- coding: utf-8 -*-
"""
Created on Wed Aug 17 15:20:45 2022

@author: Mitch
"""

import pandas as pd


from .data_parser import Parser


class EmissionRate(Parser):
    
    
    def _load_data(self):
        emiss = {
            ("oil", "kilograms CO2 per million btu"): 70,
            ("col", "kilograms CO2 per million btu"): 95,
            ("gas", "kilograms CO2 per million btu"): 53,
            ("cru", "kilograms CO2 per million btu"): 70,
        }
        df = pd.DataFrame(data=emiss.keys(), columns=["fuel", "units"])
        df["value"] = emiss.values()
        
        self.data['emission_rate'] = df
        
        

    def _build_gdx_dict(self):
        gdx_dict = {}

        
        gdx_dict['co2perbtu_units'] = {"type":"parameter",
                                "domain":["*","*"],
                                "elements":self.df[['fuel','units','value']],
                                "text":"Carbon dioxide -- not CO2e -- content, with units as domain"}
    
        return gdx_dict
        