# -*- coding: utf-8 -*-
"""
Created on Thu Aug  4 07:58:01 2022

@author: Mitch
"""

import pandas as pd
import os

from .data_parser import Parser





BEA_GSP_DTYPES =  {
    "GeoFIPS": str,
    "GeoName": str,
    "Region": str,
    "TableName": str,
    "ComponentName": str,
    "Unit": str,
    "IndustryId": str,
    "IndustryClassification": str,
    "Description": str,
    "year": str,
    "value": float,
}


class BeaGsp(Parser):
    
        
    def _load_data(self):
        """Load the data into a dictionary
        """
      
        for key,data in self.data_info['data'].items():
        
            
        
            self.data[key] = pd.read_csv(
                os.path.join(self.data_dir, data['path']),
                index_col=None,
                engine="c",
                nrows=data['nrows'],
                low_memory=False,
            )
         
            

        #print(self.data)
        
        
        
    def clean(self):
        
        for key in self.data:
        
            t = self.data[key]    
        
            t["GeoFIPS"] = t["GeoFIPS"].replace({'"': ""}, regex=True)
            t["GeoFIPS"] = t["GeoFIPS"].map(int)
            
            t["GeoName"] = t["GeoName"].replace({"\*": ""}, regex=True)
            
            # melt data
            t = pd.melt(t, id_vars=t.keys()[0:8], var_name="year")
            
            t["value"] = pd.to_numeric(t["value"], errors="coerce")
            t.fillna(0, inplace=True)
            
            t["ComponentName"] = self.data_info['data'][key]["ComponenentName"]
            
            # typing
            t["GeoFIPS"] = t["GeoFIPS"].map(str)
            t["GeoName"] = t["GeoName"].map(str)
            t["Region"] = t["Region"].map(str)
            t["TableName"] = t["TableName"].map(str)
            t["LineCode"] = t["LineCode"].map(str)
            t["Unit"] = t["Unit"].map(str)
            t["IndustryClassification"] = t["IndustryClassification"].map(str)
            t["Description"] = t["Description"].map(str)
            t["year"] = t["year"].map(str)
            t["value"] = t["value"].map(float)
           
            t.rename(
                columns={"LineCode": "IndustryId"}, inplace=True
            )
           
            
            self.data[key] = t
        
        
        
    def transform(self):
        

        # change some units
        self.df.loc[self.df[self.df["Unit"] == "thousands of us dollars (USD)"].index, "value"] = (
            self.df[self.df["Unit"] == "thousands of us dollars (USD)"].value * 1e-3
        )
    
        self.df.loc[
            self.df[self.df["Unit"] == "thousands of us dollars (USD)"].index, "Unit"
        ] = "millions of us dollars (USD)"
        
        
        
        self.df = self.df.rename(columns = self.data_info['columns'])
        
        
    
    
    
    
    
    def _build_gdx_dict(self):
        gdx_dict = {}


        
        df = self.df.merge(self.gams_maps['bea_gsp'],left_on = "ComponentName",right_on = "bea_code")

        
        gdx_dict['gsp_units'] = {"type":"parameter",
                                "elements":df[['state','year','windc_label','IndustryId','units','value']],
                                "text":"Mapped state level annual GDP, with units as domain"}
    
        return gdx_dict
    
    
    
    def build_notations(self):
        gsp = set(self.df["ComponentName"])
        gsp = gsp - {"Contributions to percent change in real GDP"}
        self.windc_notation["gsp_componentname"] = gsp
        
        
        
        self.notation_link = [
             ("year", "year"),
             ("GeoName", "region.fullname"),
             ("Unit", "units"),
             ("IndustryId", "gsp_industry_id"),
             ("ComponentName", "gsp_componentname"),
         ]
        
        self.windc_notation["gsp_industry_id"] = {
            "4",
            "5",
            "7",
            "8",
            "9",
            "10",
            "11",
            "14",
            "15",
            "16",
            "17",
            "18",
            "19",
            "20",
            "21",
            "22",
            "23",
            "24",
            "26",
            "27",
            "28",
            "29",
            "30",
            "31",
            "32",
            "33",
            "34",
            "35",
            "37",
            "38",
            "39",
            "40",
            "41",
            "42",
            "43",
            "44",
            "46",
            "47",
            "48",
            "49",
            "52",
            "53",
            "54",
            "55",
            "57",
            "58",
            "61",
            "62",
            "63",
            "64",
            "66",
            "67",
            "69",
            "71",
            "72",
            "73",
            "76",
            "77",
            "79",
            "80",
            "81",
            "83",
            "84",
            "85",
        }
        
if __name__ == "__main__":
    
    w1 = BeaGsp(r"C:\Users\Mitch\Documents\WiNDC\windc_raw_data")
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    