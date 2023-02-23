# -*- coding: utf-8 -*-
"""
Created on Thu Aug  4 13:08:31 2022

@author: Mitch
"""

import pandas as pd
import os

from .data_parser import Parser






# =============================================================================
#                 "state_exim": {
#                     "State": str,
#                     "Commodity": str,
#                     "Country": str,
#                     "Time": str,
#                     "value": float,
#                     "NAICS": str,
#                     "Commodity Description": str,
#                     "units": str,
#                     "flow": str,
#                 },
# =============================================================================




class StateExIm(Parser):
    
    
    def _load_data(self):
        
        for key,data in self.data_info['data'].items():
       
            self.data[key] = pd.read_csv(
                os.path.join(self.data_dir, data['path']), 
                skiprows=2, 
                engine="c"
                )


    def clean(self):
        
     
        for key,t in self.data.items():
     
            t.dropna(how="all", axis=1, inplace=True)
            
            
            #print(t.columns)
            
            # rename in order to aid in joining with import data later
            t.rename(columns={self.data_info['data'][key]['col_rename']: "value"}, inplace=True)
            
            # convert values to numeric
            t["value"] = t["value"].replace({",": ""}, regex=True)
            t["value"] = t["value"].map(float)
            
            # pull NAICS code out to new column
            t["NAICS"] = t["Commodity"].str.split(" ").str[0]
            t["NAICS"] = t["NAICS"].map(str)
            
            # pull NAICS description out
            t["Commodity Description"] = [
                t.loc[i, "Commodity"].split(str(t.loc[i, "NAICS"]) + " ")[1] for i in t.index
            ]
            
            # add units label
            t["units"] = "us dollars (USD)"
            t["flow"] = self.data_info['data'][key]['flow']
            
            # typing
            t["State"] = t["State"].map(str)
            t["Commodity"] = t["Commodity"].map(str)
            t["Country"] = t["Country"].map(str)
            t["Time"] = t["Time"].map(str)
            t["value"] = t["value"].map(float)
            t["NAICS"] = t["NAICS"].map(str)
            t["Commodity Description"] = t["Commodity Description"].map(str)
            t["units"] = t["units"].map(str)
            t["flow"] = t["flow"].map(str)
            
    def transform(self):
        self.df["value"] = self.df["value"] * 1e-6

        self.df["units"] = "millions of us dollars (USD)"

        self.df = self.df.rename(columns = self.data_info['columns'])
            
            
            
    def build_notations(self):
        
        self.windc_notation["exim_cnty"] = {"World Total"}
        
        self.notation_link = [
            ("State", "region.fullname"),
            ("Time", "year"),
            ("units", "units"),
            ("Country", "exim_cnty"),
        ]
        
    def _build_gdx_dict(self):
        gdx_dict = {}

        df = self.df[["state","NAICS","year","flow","units","value"]]
        df = df.rename(columns = {"state":"sr","NAICS":"n_usa","year":"yr","flow":"t"})

        n_usa = pd.DataFrame(df["n_usa"].unique())
        n_usa["Description"] = ""
        gdx_dict["n_usa"] = {"type":"set",
                             "elements":n_usa,
                             "text":"Dynamically created set from parameter usatrd, NAICS codes"
        }

        t = pd.DataFrame(df["t"].unique())
        t["Description"] = ""
        gdx_dict["t"] = {"type":"set",
                         "elements":t,
                         "text":"Dynamically create set from parameter usatrd, Trade type (import/export)"
        }

        #usatrd_units(sr,n,yr,t,*)
        gdx_dict['usatrd_units'] = {"type":"parameter",
                                    "domain":["sr","n_usa","yr","t","*"],
                                    "elements":df,
                                    "text":"USA trade data, with units as domain"
                                }
    
        return gdx_dict        
        
        
if __name__ == "__main__":
    
    w1 = StateExIm(r"C:\Users\Mitch\Documents\WiNDC\windc_raw_data")
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    