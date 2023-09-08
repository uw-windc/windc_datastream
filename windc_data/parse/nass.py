# -*- coding: utf-8 -*-
"""
Created on Thu Aug  4 13:04:17 2022

@author: Mitch
"""

import pandas as pd
import os

from .data_parser import Parser







# =============================================================================
#                 "usda_nass": {
#                     "Program": str,
#                     "Year": str,
#                     "Period": str,
#                     "Geo Level": str,
#                     "State": str,
#                     "State ANSI": str,
#                     "watershed_code": str,
#                     "Commodity": str,
#                     "Data Item": str,
#                     "Domain": str,
#                     "Domain Category": str,
#                     "Value": float,
#                     "CV (%)": float,
#                     "units": str,
#                 },
# =============================================================================



class Nass(Parser):
    
    def _load_data(self):
        
        self.data['nass'] =  pd.read_csv(
            os.path.join(self.data_dir, self.data_info['data']['path']), 
            index_col=None
            )
        
    def clean(self):
        
        
        for key,t in self.data.items():
            t["Value"] = [t.loc[i, "Value"].replace(",", "") for i in t.index]
            t["Value"] = pd.to_numeric(t["Value"], errors="coerce")
            
            t["Value"].fillna(0, inplace=True)
            
            t["Domain Category"] = [
                t.loc[i, "Domain Category"].split(": (")[1] for i in t.index
            ]
            t["Domain Category"] = [t.loc[i, "Domain Category"].split(")")[0] for i in t.index]
            t["Domain Category"] = t["Domain Category"].map(str)
            
            t["CV (%)"] = pd.to_numeric(t["CV (%)"], errors="coerce")
            t["CV (%)"].fillna(0, inplace=True)
            
            # drop unused columns (all rows = nan)
            t.dropna(axis=1, how="all", inplace=True)
            
            t["units"] = "us dollars (USD)"
            
            # typing
            t["Program"] = t["Program"].map(str)
            t["Year"] = t["Year"].map(str)
            t["Period"] = t["Period"].map(str)
            t["Geo Level"] = t["Geo Level"].map(str)
            t["State"] = t["State"].map(str)
            t["State ANSI"] = t["State ANSI"].map(str)
            t["watershed_code"] = t["watershed_code"].map(str)
            t["Commodity"] = t["Commodity"].map(str)
            t["Data Item"] = t["Data Item"].map(str)
            t["Domain"] = t["Domain"].map(str)
            t["Domain Category"] = t["Domain Category"].map(str)
            t["Value"] = t["Value"].map(float)
            t["CV (%)"] = t["CV (%)"].map(float)
            t["units"] = t["units"].map(str)
            
            self.data[key] = t
            
            
            
    def transform(self):

        # change some units
        self.df["Value"] = self.df["Value"] * 1e-6
    
        self.df["units"] = "millions of us dollars (USD)"        
        
        self.df = self.df.rename(columns = self.data_info['columns'])
            
    def build_notations(self):
        
        self.windc_notation["usda_naics"] = {
            "11111",
            "11112",
            "11113",
            "11114",
            "11115",
            "11116",
            "11119",
            "1112",
            "1113",
            "1114",
            "1119",
            "11211",
            "11212",
            "1122",
            "1123",
            "1124",
            "1125",
            "1129",
        }
            
        
        self.notation_link = [
            ("Year", "year"),
            ("State", "region.fullname"),
            ("Domain Category", "usda_naics"),
            ("units", "units"),
        ]
            
        
    def _build_gdx_dict(self):
        gdx_dict = {}

        df = self.df[['state','year','DomainCategory','units','value']]
        df = df.rename( columns = {"state":"sr","year":"yr","DomainCategory":"nass_naics"})

        nass_naics = pd.DataFrame(df["nass_naics"].unique())
        nass_naics["Description"] = ""
        gdx_dict["nass_naics"] = {"type":"set",
                                  "elements":nass_naics,
                                  "text":"NIACS codes in the NASS dataset"
        }


        gdx_dict['nass_units'] = {"type":"parameter",
                                "domain":["sr","yr","nass_naics","*"],
                                "elements":df,
                                "text":"Mapped state level annual GDP, with units as domain"}
    
        return gdx_dict       
        
        
        

if __name__ == "__main__":
    
    w1 = Nass(r"C:\Users\Mitch\Documents\WiNDC\windc_raw_data")
        
        
        

