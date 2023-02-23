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




class EiaSeds(Parser):
    
    def _load_data(self):
        

        self.data['seds'] = pd.read_csv(
            os.path.join(self.data_dir, self.data_info['data']['path']), 
            index_col=None, 
            engine="c", 
            low_memory=False
        )
        
        
        # add in descriptions
        self.desc = pd.read_excel(
            os.path.join(self.data_dir, self.data_info['data']["codes"]),
            usecols="B:D",
            skiprows=10,
            sheet_name="MSN Descriptions",
        )






    
    def clean(self):
    
        
        for key,t in self.data.items():
    
            t["full_description"] = t["MSN"].map(dict(zip(self.desc["MSN"], self.desc["Description"])))
            t["units"] = t["MSN"].map(dict(zip(self.desc["MSN"], self.desc["Unit"])))
        
            # split out sources, sector and units from MSN code
            t["source"] = t["MSN"].str[0:2]
            t["sector"] = t["MSN"].str[2:4]
        
            # typing
            t["Data_Status"] = t["Data_Status"].map(str)
            t["MSN"] = t["MSN"].map(str)
            t["StateCode"] = t["StateCode"].map(str)
            t["Year"] = t["Year"].map(str)
            t["Data"] = t["Data"].map(float)
            t["full_description"] = t["full_description"].map(str)
            t["units"] = t["units"].map(str)
            t["source"] = t["source"].map(str)
    
            self.data[key] = t

    
    def build_notations(self):
        self.notation_link = [
            ("Year", "year"),
            ("StateCode", "region.abbv"),
            ("units", "units"),
        ]


    def transform(self):
        
        self.df = self.df.rename(columns = self.data_info['columns'])



    def _build_gdx_dict(self):
        gdx_dict = {}

        df = self.df[['source','sector','state','year','units','value']]
        df = df.rename(columns = {"state":"sr","year":"yr"})

        source = pd.DataFrame(df["source"].unique())
        source["Description"] = ""
        gdx_dict["source"] = {"type":"set",
                              "elements":source,
                              "text":"Dynamically created set from seds_units parameter, EIA SEDS source codes"
        }

        sector = pd.DataFrame(df["sector"].unique())
        sector["Description"] = ""
        gdx_dict["sector"] = {"type":"set",
                              "elements":sector,
                              "text":"Dynamically created set from seds_units parameter, EIA SEDS sector codes"
        }

        #seds_units(source,sector,sr,yr,*)
        gdx_dict['seds_units'] = {"type":"parameter",
                                "domain":["source","sector","sr","yr","*"],
                                "elements":df,
                                "text":"Complete EIA SEDS data, with units as domain"}
    
        return gdx_dict
          
if __name__ == "__main__":
    
    w1 = EiaSeds(r"C:\Users\Mitch\Documents\WiNDC\windc_raw_data")
        
        
        


















