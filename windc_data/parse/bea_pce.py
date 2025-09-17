# -*- coding: utf-8 -*-
"""
Created on Thu Aug  4 10:47:01 2022

@author: Mitch
"""

import pandas as pd
import os

from .data_parser import Parser





BEA_PCE_DTYPES = {
                     "GeoFIPS": str,
                     "GeoName": str,
                     "Region": str,
                     "TableName": str,
                     "ComponentName": str,
                     "Unit": str,
                     "Line": str,
                     "IndustryClassification": str,
                     "Description": str,
                     "year": str,
                     "value": float,
                 },






class BeaPce(Parser):
    
    
    def _load_data(self):
        
        for key,data in self.data_info['data'].items():
       
            self.data[key] = pd.read_csv(
                os.path.join(self.data_dir, data['path']),
                index_col=None,
                engine="c",
                nrows=data['nrows'],
                low_memory=False,
                encoding = "latin-1"
            )


    def clean(self):
        
        
        for key,t in self.data.items():
        
            t["GeoFIPS"] = t["GeoFIPS"].replace({'"': ""}, regex=True)
            t["GeoFIPS"] = t["GeoFIPS"].map(int)
            
            
            
            # melt data
            t = pd.melt(t, id_vars=t.keys()[0:8], var_name="year")
            
            # typing
            t["GeoFIPS"] = t["GeoFIPS"].map(str)
            t["GeoName"] = t["GeoName"].map(str)
            t["Region"] = t["Region"].map(str)
            t["TableName"] = t["TableName"].map(str)
            t["ComponentName"] =   "Total personal consumption expenditures (PCE) by state"#t["ComponentName"].map(str)
            t["Unit"] = t["Unit"].map(str)
            t["LineCode"] = t["LineCode"].map(str)
            t["IndustryClassification"] = t["IndustryClassification"].map(str)
            t["Description"] = t["Description"].map(str)
            t["year"] = t["year"].map(str)
            t["value"] = t["value"].map(float)
            
            self.data[key] = t

    def build_notations(self):
        
        self.windc_notation["pce_componentname"] = {
        "Total personal consumption expenditures (PCE) by state"
        }
        
        
        self.notation_link = [
            ("year", "year"),
            ("GeoName", "region.fullname"),
            ("Unit", "units"),
            ("ComponentName", "pce_componentname"),
        ]

            
        
        
    def transform(self):
                
        self.df = self.df.rename(columns = self.data_info['columns'])       
        
        
    def _build_gdx_dict(self):
        gdx_dict = {}
        
        df = self.df.merge(self.gams_maps['bea_pce'],left_on = "Description",right_on = "pce_description")

        df = df[['year','state','windc_label','units','value']]
        df = df.rename(columns = {"year":"yr","state":"sr","windc_label":"pg"})

        pg = pd.DataFrame(df["pg"].unique())
        pg["Description"] = ""
        gdx_dict["pg"] = {"type":"set",
                          "elements":pg,
                          "text":"Dynamically created set from parameter pce_units, PCE goods"}

        #pce_raw_units(yr,sr,pg,*)
        gdx_dict['pce_units'] = {"type":"parameter",
                                 "domain":["yr","sr","pg","*"],
                                 "elements":df,
                                 'text':"Personal consumer expenditure by commodity (including aggregate subtotals, with units as domain"
                                 }
        
        return gdx_dict
        
        
        
        
if __name__ == "__main__":
    
    w1 = BeaPce(r"C:\Users\Mitch\Documents\WiNDC\windc_raw_data")
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    