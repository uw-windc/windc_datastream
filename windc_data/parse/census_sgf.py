# -*- coding: utf-8 -*-
"""
Created on Thu Aug  4 11:18:54 2022

@author: Mitch
"""

import pandas as pd
import os

from .data_parser import Parser


from .sgf_table_sums import sums_new_methodology



# =============================================================================
#                 "census_sgf": {
#                     "Category": str,
#                     "State": str,
#                     "value": float,
#                     "year": str,
#                     "units": str,
#                 },
# =============================================================================






class CensusSGF(Parser):
    
    
    
    def _load_data(self):
        
        self.ids = pd.read_excel(
            os.path.join(self.data_dir,self.data_info['ids']),
            dtype={"ID Code": str, "State": str},
            )
        
        self.ids["State"] = self.ids["State"].str.strip()

        self.map_id = dict(zip(self.ids["ID Code"], self.ids["State"]))
        self.map_id["00000000000000"] = "United States"
        self.map_id["09000000000000"] = "District of Columbia"
        
        
        for key,data in self.data_info['data'].items():
       
            self.data[key] = pd.read_table(
                os.path.join(self.data_dir, data['path']), 
                header=None, index_col=None)
    

    def clean(self):
        
        for key,tt in self.data.items():
            
            if int(key) in [1999]:
                t = self._clean_1999(tt)
                    
            
            else:
                t = self._clean(tt)
            
            regions = list(set(t["Government Name"]))
            regions.sort()
        
            cols = ["Category"]
            cols.extend(regions)
            table = pd.DataFrame(columns=cols)
        
            for n, row in enumerate(sums_new_methodology.keys()):
                table.loc[n, "Category"] = row
        
                for region in regions:
                    table.loc[n, region] = t[
                        (t["Government Name"] == region)
                        & (
                            t["Item Code"].isin(sums_new_methodology[row])
                            == True
                        )
                    ]["Amount"].sum()
        
            table = pd.melt(table, id_vars="Category", var_name="State")
            table["year"] = str(key)
            table["units"] = "thousands of us dollars (USD)"
        
            # typing
            table["Category"] = table["Category"].map(str)
            table["State"] = table["State"].map(str)
            table["value"] = table["value"].map(float)
            table["year"] = table["year"].map(str)
            table["units"] = table["units"].map(str)           
            
            self.data[key] = table



    def _clean(self,t):
        t["Government Code"] = [t.loc[i, 0][0:14] for i in t.index]
        t["Item Code"] = [t.loc[i, 0][14:17] for i in t.index]
        t["Amount"] = [t.loc[i, 0][17:29] for i in t.index]
        t["Survery Year"] = [t.loc[i, 0][29:31] for i in t.index]
        t["Year of Data"] = [t.loc[i, 0][31:33] for i in t.index]
        t["Origin"] = [t.loc[i, 0][33:35] for i in t.index]
    
        t["Amount"] = t["Amount"].map(int)
        t["Government Name"] = t["Government Code"].map(self.map_id)
    

        
        return t
        
    def _clean_1999(self,t):
        t["Government Code"] = [t.loc[i, 0][0:14] for i in t.index]
        t["Origin"] = [t.loc[i, 0][17:19] for i in t.index]
        t["Item Code"] = [t.loc[i, 0][21:24] for i in t.index]
        t["Amount"] = [t.loc[i, 0][24:35] for i in t.index]
        t["Survery Year"] = 99
        t["Year of Data"] = 99
     
        t["Amount"] = t["Amount"].map(int)
        t["Government Name"] = t["Government Code"].map(self.map_id)
     
     
        return t



    def build_notations(self):
        

    
        # create a new notation from data
        self.windc_notation["sgf_category"] = set(
            self.df["Category"]
        )
        
        self.notation_link = [
            ("year", "year"),
            ("State", "region.fullname"),
            ("units", "units"),
            ("Category", "sgf_category"),
        ]


    def transform(self):
        
        # change some units
        self.df["value"] = self.df["value"] * 1e-3
        
        self.df["units"] = "millions of us dollars (USD)"
        
        self.df = self.df.rename(columns = self.data_info['columns'])
                
    def _build_gdx_dict(self):
        
        gdx_dict = {}
        
        df = self.df.merge(self.gams_maps['census_sgf'],left_on = "category",right_on="sgf_category")
        df = df[['year','state','windc_label','units','value']]

        df = df.rename(columns = {"year":"yr","state":"sr","windc_label":"ec"})
        
        ec = pd.DataFrame(df["ec"].unique())
        ec["Description"] = ""
        gdx_dict["ec"] = {"type":"set",
                          "elements":ec,
                          "text": "Dynamically created set from the sgf_raw parameter, government expenditure categories"}

        #sgf_raw_units(yr,sr,ec,*)
        gdx_dict['sgf_units'] = {'type':"parameter",
                                 "domain":["yr","sr","ec","*"],
                                'elements':df,
                                "text":"State government finances (SGF), with units as domain"
                                        }
        
        
        
        return gdx_dict

if __name__ == "__main__":
    
    w1 = CensusSGF(r"C:\Users\Mitch\Documents\WiNDC\windc_raw_data")
    
    
    
    































































        