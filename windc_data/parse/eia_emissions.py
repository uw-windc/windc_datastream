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







class EiaEmissions(Parser):
    
    def _load_data(self):
        
        self.data['coal'] = self._coal_emissions(os.path.join(self.data_dir,self.data_info['data']['coal']))
        self.data['natgas'] = self._natgas_emissions(os.path.join(self.data_dir,self.data_info['data']['natgas']))
        self.data['petrol'] = self._petrol_emissions(os.path.join(self.data_dir,self.data_info['data']['petrol']))
        self.data['industrial'] = self._industrial_emissions(os.path.join(self.data_dir,self.data_info['data']['industrial']))
        self.data['commercial'] = self._commercial_emissions(os.path.join(self.data_dir,self.data_info['data']['commercial']))
        self.data['residential'] = self._residential_emissions(os.path.join(self.data_dir,self.data_info['data']['residential']))
        self.data['electricity'] = self._electricity_emissions(os.path.join(self.data_dir,self.data_info['data']['electricity']))
        self.data['transport'] = self._transport_emissions(os.path.join(self.data_dir,self.data_info['data']['transportation']))

                                             




    def clean(self):
        
        for key,t in self.data.items():
            #print(t['State'].unique())
            self.data[key] = t.replace({"State total1":"United States"})
            

    
    
    def _coal_emissions(self,data_dir):

    
        t = pd.read_excel(
            data_dir, sheet_name="Sheet1", skiprows=2, nrows=52
        )
    
        t.drop(columns=["Percent", "Absolute"], inplace=True)
    
        # melt data
        t = pd.melt(t, id_vars=["State"], var_name="year", value_name="emissions")
    
        t["units"] = "million metric tons of carbon dioxide"
        t["sector"] = "coal"
    
        # typing
        t["State"] = t["State"].map(str)
        t["year"] = t["year"].map(str)
        t["emissions"] = t["emissions"].map(float)
        t["units"] = t["units"].map(str)
    
        return t
    
    
    def _natgas_emissions(self,data_dir):

        t = pd.read_excel(
            data_dir, sheet_name="Sheet1", skiprows=2, nrows=52
        )
    
        t.drop(columns=["Percent", "Absolute"], inplace=True)
    
        # melt data
        t = pd.melt(t, id_vars=["State"], var_name="year", value_name="emissions")
    
        t["units"] = "million metric tons of carbon dioxide"
        t["sector"] = "natural_gas"
    
        # typing
        t["State"] = t["State"].map(str)
        t["year"] = t["year"].map(str)
        t["emissions"] = t["emissions"].map(float)
        t["units"] = t["units"].map(str)
    
        return t
    
    
    def _petrol_emissions(self,data_dir):
        t = pd.read_excel(
            data_dir, sheet_name="Sheet1", skiprows=2, nrows=52
        )
    
        t.drop(columns=["Percent", "Absolute"], inplace=True)
    
        # melt data
        t = pd.melt(t, id_vars=["State"], var_name="year", value_name="emissions")
    
        t["units"] = "million metric tons of carbon dioxide"
        t["sector"] = "petroleum"
    
        # typing
        t["State"] = t["State"].map(str)
        t["year"] = t["year"].map(str)
        t["emissions"] = t["emissions"].map(float)
        t["units"] = t["units"].map(str)
    
        return t
    
    
    def _industrial_emissions(self,data_dir):

        t = pd.read_excel(
            data_dir, sheet_name="Sheet1", skiprows=2, nrows=52
        )
    
        t.drop(columns=["Percent", "Absolute"], inplace=True)
    
        # melt data
        t = pd.melt(t, id_vars=["State"], var_name="year", value_name="emissions")
    
        t["units"] = "million metric tons of carbon dioxide"
        t["sector"] = "industrial"
    
        # typing
        t["State"] = t["State"].map(str)
        t["year"] = t["year"].map(str)
        t["emissions"] = t["emissions"].map(float)
        t["units"] = t["units"].map(str)
    
        return t
    
    
    def _commercial_emissions(self,data_dir):

        t = pd.read_excel(
            data_dir, sheet_name="Sheet1", skiprows=2, nrows=52
        )
    
        t.drop(columns=["Percent", "Absolute"], inplace=True)
    
        # melt data
        t = pd.melt(t, id_vars=["State"], var_name="year", value_name="emissions")
    
        t["units"] = "million metric tons of carbon dioxide"
        t["sector"] = "commercial"
    
        # typing
        t["State"] = t["State"].map(str)
        t["year"] = t["year"].map(str)
        t["emissions"] = t["emissions"].map(float)
        t["units"] = t["units"].map(str)
    
        return t
    
    
    def _residential_emissions(self,data_dir):

        t = pd.read_excel(
            data_dir, sheet_name="Sheet1", skiprows=2, nrows=52
        )
    
        t.drop(columns=["Percent", "Absolute"], inplace=True)
    
        # melt data
        t = pd.melt(t, id_vars=["State"], var_name="year", value_name="emissions")
    
        t["units"] = "million metric tons of carbon dioxide"
        t["sector"] = "residential"
    
        # typing
        t["State"] = t["State"].map(str)
        t["year"] = t["year"].map(str)
        t["emissions"] = t["emissions"].map(float)
        t["units"] = t["units"].map(str)
    
        return t
    
    
    def _electricity_emissions(self,data_dir):
        t = pd.read_excel(
            data_dir, sheet_name="Sheet1", skiprows=2, nrows=52
        )
    
        t.drop(columns=["Percent", "Absolute"], inplace=True)
    
        # melt data
        t = pd.melt(t, id_vars=["State"], var_name="year", value_name="emissions")
    
        t["units"] = "million metric tons of carbon dioxide"
        t["sector"] = "electricity"
    
        # typing
        t["State"] = t["State"].map(str)
        t["year"] = t["year"].map(str)
        t["emissions"] = t["emissions"].map(float)
        t["units"] = t["units"].map(str)
    
        return t
    
    
    def _transport_emissions(self,data_dir):
        t = pd.read_excel(
            data_dir, sheet_name="Sheet1", skiprows=2, nrows=52
        )
    
        t.drop(columns=["Percent", "Absolute"], inplace=True)
    
        # melt data
        t = pd.melt(t, id_vars=["State"], var_name="year", value_name="emissions")
    
        t["units"] = "million metric tons of carbon dioxide"
        t["sector"] = "transport"
    
        # typing
        t["State"] = t["State"].map(str)
        t["year"] = t["year"].map(str)
        t["emissions"] = t["emissions"].map(float)
        t["units"] = t["units"].map(str)
    
        return t
    
    
 
    
    
    def _heatrate(self,data_dir):
    
        t = pd.read_csv(data_dir, index_col=None)
    
        # add in units
        t["units"] = "btu per kWh generated"
    
        # reshape
        t = pd.melt(t, id_vars=["year", "units"])
    
        return t
    
    
    
    def build_notations(self):
        
        self.notation_link = [
            ("year", "year"),
            ("State", "region.fullname"),
            ("units", "units"),
        ]


    def transform(self):
        self.df = self.df.rename(columns = self.data_info['columns'])



    def _build_gdx_dict(self):
        gdx_dict = {}


        
        df = self.df.merge(self.gams_maps['eia_emissions'],left_on = "sector",right_on="eia_sector")

        
        gdx_dict['emissions_units'] = {"type":"parameter",
                                "elements":df[['windc_label','state','year','units','value']],
                                "text":"CO2 emissions by fuel and sector, with units as domain"}
    
        return gdx_dict

            
if __name__ == "__main__":
    
    w1 = EiaEmissions(r"C:\Users\Mitch\Documents\WiNDC\windc_raw_data")
        
        
        


















