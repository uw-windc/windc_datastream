# -*- coding: utf-8 -*-
"""
Created on Sat Aug  6 19:23:49 2022

@author: Mitch
"""


import pandas as pd
import os

from .data_parser import Parser



# =============================================================================
#                 "eia_crude_price": {
#                     "year": str,
#                     "price": float,
#                     "units": str,
#                     "notes": str,
#                 },
#                 "eia_emissions": {
#                     "State": str,
#                     "year": str,
#                     "emissions": float,
#                     "units": str,
#                     "sector": str,
#                 },
#                 "eia_heatrate": {
#                     "year": str,
#                     "units": str,
#                     "variable": str,
#                     "value": float,
#                 },
#                 "eia_seds": {
#                     "Data_Status": str,
#                     "MSN": str,
#                     "StateCode": str,
#                     "Year": str,
#                     "Data": float,
#                     "full_description": str,
#                     "units": str,
#                     "source": str,
#                     "sector": str,
#                 },
#                 "emission_rate": {"fuel": str, "units": str, "value": float,},
# =============================================================================








class EiaCrude(Parser):
    
    def _load_data(self):
        

        self.data['crude'] = pd.read_csv(os.path.join(self.data_dir,self.data_info['data']['path']), skiprows=4)
    
    
    
    def clean(self):
        
        for key,t in self.data.items():
            t.rename(
                {
                    "Year": "year",
                    "U.S. Crude Oil Composite Acquisition Cost by Refiners Dollars per Barrel": "price",
                },
                axis="columns",
                inplace=True,
            )
        
            t["units"] = "us dollars (USD) per barrel"
            t["notes"] = "crude oil composite acquisition cost by refiners"
        
            # extract year
            #t["year"] = t["year"].dt.year
        
            # typing
            t["year"] = t["year"].map(str)
            t["price"] = t["price"].map(float)
            t["units"] = t["units"].map(str)
            t["notes"] = t["notes"].map(str)
        
            self.data[key] = t
    
    def build_notations(self):
      
        self.notation_link = [
            ("year", "year"),
            ("units", "units"),
        ]
         
    def transform(self):
        
        self.df = self.df.rename(columns = self.data_info['columns'])
    
    
    def _build_gdx_dict(self):
        gdx_dict = {}


        
        #df = self.df.merge(self.gams_maps['bea_gsp'],left_on = "ComponentName",right_on = "bea_code")
        df = self.df[['year','units','value']]
        df = df.rename(columns = {"year":"yr"})
        
        gdx_dict['crude_oil_price_units'] = {"type":"parameter",
                                "domain":["yr","*"],
                                "elements":df,
                                "text":"Crude oil composite acquisition cost by refiners, with units as domain"}
    
        return gdx_dict

            
if __name__ == "__main__":
    
    w1 = EiaCrude(r"C:\Users\Mitch\Documents\WiNDC\windc_raw_data")
        
        
        


















