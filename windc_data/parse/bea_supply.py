# -*- coding: utf-8 -*-
"""
Created on Thu Aug  4 10:57:53 2022

@author: Mitch
"""

import pandas as pd
import os

from .data_parser import Parser






# =============================================================================
#                 "bea_supply": {
#                     "IOCode": str,
#                     "Row_Name": str,
#                     "Commodities/Industries": str,
#                     "Column_Name": str,
#                     "value": float,
#                     "year": str,
#                     "units": str,
#                 },
# =============================================================================

class BeaSupply(Parser):
    
    
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
        
            t.iloc[0,0] = t.iloc[1,0]
            t.columns = t.iloc[0,:]

            to_drop = t.iloc[0,1]

            t.drop([0,1], inplace = True)
            t.drop(to_drop, inplace=True, axis = "columns")

            tt = pd.melt(t, id_vars = ["IOCode"], var_name = to_drop)


            tt.dropna(inplace=True)
            
            # add in year label
            tt["year"] = str(key)
            
            tt["units"] = "millions of us dollars (USD)"
            
            # typing
            tt["IOCode"] = tt["IOCode"].map(str)
            tt["Commodities/Industries"] = tt["Commodities/Industries"].map(str)
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

        df = df.rename(columns = {"year":"yr","windc_label":"ir_supply","windc_label_col":"jc_supply"})

        ir_supply = df["ir_supply"].unique()
        ir_supply = pd.DataFrame(ir_supply)
        ir_supply["Description"] = ""

        gdx_dict["ir_supply"] = {"type":"set",
                                 "elements":ir_supply,
                                 "text":"Dynamically created set domain of second dimension of supply_units"
        }


        jc_supply = df["jc_supply"].unique()
        jc_supply = pd.DataFrame(jc_supply)
        jc_supply["Description"] = ""

        gdx_dict["jc_supply"] = {"type":"set",
                                 "elements":jc_supply,
                                 "text":"Dynamically created set domain of third dimension of supply_units"
        }


        gdx_dict['supply_units'] = {'type':"parameter",
                                    "domain":["yr","ir_supply","jc_supply","*"],
                                    'elements':df,
                                    "text":"Mapped annual supply tables, with units as domain"
        }
        
        
        
        return gdx_dict
                 
            
if __name__ == "__main__":
    
    w1 = BeaSupply(r"C:\Users\Mitch\Documents\WiNDC\windc_raw_data")
            
            
            
            





































            