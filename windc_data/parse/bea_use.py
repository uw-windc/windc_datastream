# -*- coding: utf-8 -*-
"""
Created on Thu Aug  4 11:19:33 2022

@author: Mitch
"""

import pandas as pd
import os

from .data_parser import Parser



# =============================================================================
#                 "bea_use": {
#                     "IOCode": str,
#                     "Row_Name": str,
#                     "Commodities/Industries": str,
#                     "Column_Name": str,
#                     "value": float,
#                     "year": str,
#                     "units": str,
#                 },
# 
# =============================================================================



class BeaUse(Parser):
    
    def _load_data(self):
        
        for key in self.data_info['data']['years']:
       
            self.data[key] = pd.read_excel(
                os.path.join(self.data_dir, self.data_info['data']['path']),
                sheet_name = str(key),
                skiprows = self.data_info['data']['skip_rows'],
                na_values = ["..."]
                
            )


    def clean(self):
        
        for key,t in self.data.items():
            coldex = [t.loc[0, "Unnamed: 2":].tolist(), t.loc[1, "Unnamed: 2":].tolist()]
            coldex = list(zip(*coldex))
            coldex = pd.MultiIndex.from_tuples(
                coldex, names=["Commodities/Industries", "Column_Name"]
            )
            
            t.drop([0, 1], inplace=True)
            index = [t["Unnamed: 0"].tolist(), t["Unnamed: 1"].tolist()]
            index = list(zip(*index))
            index = pd.MultiIndex.from_tuples(index, names=["IOCode", "Row_Name"])
            
            t.set_index(["Unnamed: 0", "Unnamed: 1"], inplace=True)
            
            # create MultiIndex dataframe for melting
            tt = pd.DataFrame(data=t.values, index=index, columns=coldex)
            
            tt = pd.melt(tt.reset_index(drop=False), id_vars=["IOCode", "Row_Name"])
            tt.fillna(0, inplace=True)
            
            # add in year label
            tt["year"] = str(key)
            
            tt["units"] = "millions of us dollars (USD)"
            
            # typing
            tt["IOCode"] = tt["IOCode"].map(str)
            tt["Row_Name"] = tt["Row_Name"].map(str)
            tt["Commodities/Industries"] = tt["Commodities/Industries"].map(str)
            tt["Column_Name"] = tt["Column_Name"].map(str)
            tt["value"] = tt["value"].map(float)
            
            self.data[key] = tt
            
    def build_notations(self):
        self.notation_link = [("year", "year")]


    def transform(self):
        self.df = self.df.rename(columns = self.data_info['columns'])       
        
        
    def _build_gdx_dict(self):
        
        gdx_dict = {}
        
        df = self.df.merge(self.gams_maps['bea_all'],left_on = "IOCode",right_on="bea_code")
        
        df = df.merge(self.gams_maps['bea_all'],left_on = "Commodities_Industries",right_on="bea_code",suffixes=("","_col"))

        df = df[['year','windc_label','windc_label_col','units','value']]

        df = df.rename(columns = {"year":"yr","windc_label":"ir_use","windc_label_col":"jc_use"})
        

        ir_use = df["ir_use"].unique()
        ir_use = pd.DataFrame(ir_use)
        ir_use["Description"] = ""

        gdx_dict["ir_use"] = {"type":"set",
                                 "elements":ir_use,
                                 "text":"Dynamically created set domain of third dimension of use_units"
        }

        jc_use = df["jc_use"].unique()
        jc_use = pd.DataFrame(jc_use)
        jc_use["Description"] = ""

        gdx_dict["jc_use"] = {"type":"set",
                                 "elements":jc_use,
                                 "text":"Dynamically created set domain of third dimension of use_units"
        }


        gdx_dict['use_units'] = {'type':"parameter",
                                 "domain":["yr","ir_use","jc_use","*"],
                                'elements':df,
                                "text":"Mapped annual use tables, with units as domain"
                                        }
        
        
        
        return gdx_dict                  
        
if __name__ == "__main__":
    
    w1 = BeaUse(r"C:\Users\Mitch\Documents\WiNDC\windc_raw_data")
    
    
    
    
    
    
    
    
        
        
        
        
        
        
        
        
        
        
        