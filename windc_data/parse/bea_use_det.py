# -*- coding: utf-8 -*-
"""
Created on Thu Aug  4 11:18:54 2022

@author: Mitch
"""

import pandas as pd
import os

from .data_parser import Parser



# =============================================================================
#                 "bea_use_det": {
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





class BeaUseDet(Parser):
    
    def _load_data(self):
        
        for key in self.data_info['data']['years']:
       
            self.data[key] = pd.read_excel(
                os.path.join(self.data_dir, self.data_info['data']['path']),
                sheet_name = str(key),
                skiprows = self.data_info['data']['skip_rows'],
                nrows = 416
            )


    def clean(self):
        
        for key,t in self.data.items():

            coldex = [t.loc[0, "Unnamed: 2":].tolist(), t.loc[1, "Unnamed: 2":].tolist()]
            coldex = list(zip(*coldex))
            coldex = pd.MultiIndex.from_tuples(
                coldex, names=["Column_Name", "Commodities/Industries"]
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
            
            self.data[key] = tt[
                                [
                                    "IOCode",
                                    "Row_Name",
                                    "Commodities/Industries",
                                    "Column_Name",
                                    "value",
                                    "year",
                                    "units",
                                ]
                            ]


    def build_notations(self):
        self.notation_link = [("year", "year")]
            

    def transform(self):
                
        self.df = self.df.rename(columns = self.data_info['columns'])       
        
    def _build_gdx_dict(self):
        
        gdx_dict = {}
        
        df = self.df.merge(self.gams_maps['bea_all_det'],left_on = "IOCode",right_on="bea_code")
        
        df = df.merge(self.gams_maps['bea_all_det'],left_on = "Commodities_Industries",right_on="bea_code",suffixes=("","_col"))
        df = df[['year','windc_label','windc_label_col','units','value']]
        df = df.rename(columns = {"year":"yr","windc_label":"ir_use_det","windc_label_col":"jc_use_det"})

        ir_use_det = pd.DataFrame(df["ir_use_det"].unique())
        ir_use_det["Description"] = ""
        gdx_dict["ir_use_det"] = {"type":"set",
                                  "elements":ir_use_det,
                                  "text":"Dynamically created set from parameter use_det_units, identifiers for use table rows"
        }

        jc_use_det = pd.DataFrame(df["jc_use_det"].unique())
        jc_use_det["Description"] = ""
        gdx_dict["jc_use_det"] = {"type":"set",
                                  "elements":jc_use_det,
                                  "text":"Dynamically created set from parameter use_det_units, identifiers for use table columns"
        }

        #use_det_units(yr_det,ir_use,jc_use,*)
        gdx_dict['use_det_units'] = {'type':"parameter",
                                     "domain":["yr","ir_use_det","jc_use_det","*"],
                                    'elements':df,
                                        "text":"Mapped DETAILED use tables, with units as domain (2007 and 2012 only)"
                                        }
        
        
        
        return gdx_dict                 

if __name__ == "__main__":
    
    w1 = BeaUseDet(r"C:\Users\Mitch\Documents\WiNDC\windc_raw_data")
    
    
    
    































































        