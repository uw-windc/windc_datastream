# -*- coding: utf-8 -*-
"""
Created on Thu Aug  4 12:52:44 2022

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








class EiaHeatrate(Parser):
    
    def _load_data(self):
        


        self.data['heatrate'] = pd.read_csv(os.path.join(self.data_dir, self.data_info['data']['path']))

    

    
    def clean(self):
        
        for key,t in self.data.items():
        
            # add in units
            t["units"] = "btu per kWh generated"
    
            # reshape
            t = pd.melt(t, id_vars=["year", "units"])
            
            t["year"] = t["year"].map(str)
            
    
            self.data[key] = t
    
    
    
    def transform(self):
        
        out = [self.df]
        

        
        for i in self.windc_notation["year"]:
            if i not in self.df["year"].unique():

                t = self.df[
                        self.df["year"] == "2005"
                        ].copy()
                t["year"] = i
                out.append(t)

        self.df = pd.concat(out, ignore_index=True)
 
        self.df = self.df.rename(columns = self.data_info['columns'])
    
    
    def build_notations(self):
        
        
        self.notation_link = [
            ("year", "year"), 
            ("units", "units")
            ]





    def _build_gdx_dict(self):
        gdx_dict = {}


        
        df = self.df.merge(self.gams_maps['eia_gen'],left_on = "variable",right_on = "eia_technologies")

        
        gdx_dict['heatrate_units'] = {"type":"parameter",
                                "elements":df[['year','windc_label','units','value']],
                                "text":"Electricity generator (avg across tech) heat rate by fuel, with units as domain"}
    
        return gdx_dict

            
if __name__ == "__main__":
    
    w1 = EiaHeatrate(r"C:\Users\Mitch\Documents\WiNDC\windc_raw_data")
        
        
        


















