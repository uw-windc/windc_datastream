# -*- coding: utf-8 -*-
"""
Created on Thu Aug  4 12:46:27 2022

@author: Mitch
"""

import pandas as pd
import os

from .data_parser import Parser


# =============================================================================
#                 "cfs": {
#                     "SHIPMT_ID": str,
#                     "ORIG_STATE": str,
#                     "ORIG_MA": str,
#                     "ORIG_CFS_AREA": str,
#                     "DEST_STATE": str,
#                     "DEST_MA": str,
#                     "DEST_CFS_AREA": str,
#                     "NAICS": str,
#                     "QUARTER": str,
#                     "SCTG": str,
#                     "MODE": str,
#                     "SHIPMT_VALUE": float,
#                     "SHIPMT_WGHT": float,
#                     "SHIPMT_DIST_GC": float,
#                     "SHIPMT_DIST_ROUTED": float,
#                     "TEMP_CNTL_YN": str,
#                     "EXPORT_YN": str,
#                     "EXPORT_CNTRY": str,
#                     "HAZMAT": str,
#                     "WGT_FACTOR": float,
#                     "SHIPMT_VALUE_units": str,
#                     "SHIPMT_WGHT_units": str,
#                     "SHIPMT_DIST_GC_units": str,
#                     "SHIPMT_DIST_ROUTED_units": str,
#                     "year": str,
#                 },
# =============================================================================


class CFS(Parser):
    
    def _load_data(self):
        
        for key,path in self.data_info['data'].items():
       
            self.data[key] = pd.read_csv(
                os.path.join(self.data_dir, path), 
                index_col=None, 
                low_memory=False, 
                engine="c"
                )


    def clean(self):
        
        for key,a in self.data.items():
        
            a["SHIPMT_VALUE_units"] = "us dollars (USD)"
            a["SHIPMT_WGHT_units"] = "weight of shipment in pounds"
            a["SHIPMT_DIST_GC_units"] = "great circle distance in miles"
            a["SHIPMT_DIST_ROUTED_units"] = "routed distance in miles"
        
            a["year"] = key
        
            a["SHIPMT_ID"] = a["SHIPMT_ID"].map(str)
            a["ORIG_STATE"] = a["ORIG_STATE"].map(str)
            a["ORIG_MA"] = a["ORIG_MA"].map(str)
            a["ORIG_CFS_AREA"] = a["ORIG_CFS_AREA"].map(str)
            a["DEST_STATE"] = a["DEST_STATE"].map(str)
            a["DEST_MA"] = a["DEST_MA"].map(str)
            a["DEST_CFS_AREA"] = a["DEST_CFS_AREA"].map(str)
            a["NAICS"] = a["NAICS"].map(str)
            a["QUARTER"] = a["QUARTER"].map(str)
            a["SCTG"] = a["SCTG"].map(str)
            a["MODE"] = a["MODE"].map(str)
            a["SHIPMT_VALUE"] = a["SHIPMT_VALUE"].map(float)
            a["SHIPMT_WGHT"] = a["SHIPMT_WGHT"].map(float)
            a["SHIPMT_DIST_GC"] = a["SHIPMT_DIST_GC"].map(float)
            a["SHIPMT_DIST_ROUTED"] = a["SHIPMT_DIST_ROUTED"].map(float)
            a["TEMP_CNTL_YN"] = a["TEMP_CNTL_YN"].map(str)
            a["EXPORT_YN"] = a["EXPORT_YN"].map(str)
            a["EXPORT_CNTRY"] = a["EXPORT_CNTRY"].map(str)
            a["HAZMAT"] = a["HAZMAT"].map(str)
            a["WGT_FACTOR"] = a["WGT_FACTOR"].map(float)
            a["year"] = a["year"].map(str)
            a["SHIPMT_VALUE_units"] = a["SHIPMT_VALUE_units"].map(str)
            a["SHIPMT_WGHT_units"] = a["SHIPMT_WGHT_units"].map(str)
            a["SHIPMT_DIST_GC_units"] = a["SHIPMT_DIST_GC_units"].map(str)
            a["SHIPMT_DIST_ROUTED_units"] = a["SHIPMT_DIST_ROUTED_units"].map(str)
            
            self.data[key] = a







    def transform(self):
        # calculate total value
        self.df["TOTAL_VALUE"] = self.df.WGT_FACTOR * self.df.SHIPMT_VALUE * 1e-6
        
        # add in units
        self.df["units"] = "millions of us dollars (USD)"
        
        # pivot data
        self.cfs_st = self.df.pivot_table(
            index=["ORIG_STATE", "DEST_STATE", "NAICS", "SCTG"],
            values=["TOTAL_VALUE"],
            aggfunc=sum,
        )
        self.cfs_st["units"] = "millions of us dollars (USD)"
        self.cfs_st.reset_index(inplace=True)
        
        self.cfs_ma = self.df.pivot_table(
            index=["ORIG_MA", "DEST_MA", "NAICS", "SCTG"],
            values=["TOTAL_VALUE"],
            aggfunc=sum,
        )
        self.cfs_ma["units"] = "millions of us dollars (USD)"
        self.cfs_ma.reset_index(inplace=True)
        
        self.cfs_area = self.df.pivot_table(
            index=["ORIG_CFS_AREA", "DEST_CFS_AREA", "NAICS", "SCTG"],
            values=["TOTAL_VALUE"],
            aggfunc=sum,
        )
        self.cfs_area["units"] = "millions of us dollars (USD)"
        self.cfs_area.reset_index(inplace=True)

        self.df = self.df.rename(columns = self.data_info['cfs_columns'])
        self.cfs_ma = self.cfs_ma.rename(columns = self.data_info['cfs_ma_columns'])
        self.cfs_st = self.cfs_st.rename(columns = self.data_info['cfs_st_columns'])
        self.cfs_area = self.cfs_area.rename(columns = self.data_info['cfs_area_columns'])



    def to_csv(self,output_dir,output_name):
        
        super().to_csv(output_dir,output_name)
        
        self.cfs_st.to_csv(os.path.join(output_dir,"cfs_st.csv"),index=False)
        self.cfs_ma.to_csv(os.path.join(output_dir,"cfs_ma.csv"),index=False)
        self.cfs_area.to_csv(os.path.join(output_dir,"cfs_area.csv"),index=False)
 

    def _build_gdx_dict(self):
        gdx_dict = {}

        cfs_ma = self.cfs_ma[['ORIG_MA','DEST_MA','NAICS','SCTG','units','value']]

        

        

        cfs_st = self.cfs_st[['ORIG_STATE','DEST_STATE','NAICS','SCTG','units','value']]
        cfs_st = cfs_st.rename(columns = {"ORIG_STATE":"sr_orig","DEST_STATE":"sr_dest","NAICS":"n","SCTG":"sg"})

        n = pd.DataFrame(cfs_st["n"].unique())
        n["Description"] = ""
        gdx_dict["n"] = {"type":"set",
                         "elements":n,
                         "text": "Dynamically created set from cfs2012 parameter, NAICS codes"
                         }

        sg = pd.DataFrame(cfs_st["sg"].unique())
        sg["Description"] = ""
        gdx_dict["sg"] = {"type":"set",
                         "elements":sg,
                         "text": "Dynamically created set from cfs2012 parameter, SCTG codes"
        }


        #cfs2012_units(sr,sr,n,sg,*)        
        gdx_dict['cfsdata_st_units'] = {"type":"parameter",
                                        "domain":["sr","sr","n","sg","*"],
                                        "elements":cfs_st,
                                        "text":"CFS - State level shipments (value), with units as domain"}      
    
        #gdx_dict['cfsdata_ma_units'] = {"type":"parameter",
        #                                "domain":[],
        #                                "elements":cfs_ma,
        #                                "text":"CFS - Metro area level shipments (value), with units as domain"}

        return gdx_dict        
        

    def build_notations(self):
        
        # create a new notation from data
        cfs_ma = set(self.df["ORIG_MA"])
        cfs_ma.update(set(self.df["DEST_MA"]))
        cfs_ma = cfs_ma - {"0"}
        self.windc_notation["cfs_ma"] = cfs_ma



        # create a new notation from data
        # drop undisclosed SCTG data
        cfs_sctg = set(self.df["SCTG"])
        cfs_sctg = cfs_sctg - {
            "00",
            "25-30",
            "01-05",
            "15-19",
            "10-14",
            "06-09",
            "39-99",
            "20-24",
            "31-34",
            "35-38",
            "99",
        }
        self.windc_notation["cfs_sctg"] = cfs_sctg

        # create a new notation from data
        # drop undisclosed SCTG data
        self.windc_notation["cfs_export"] = {"N"}

        # create notation links
        self.notation_link = [
            ("ORIG_STATE", "fips.state"),
            ("DEST_STATE", "fips.state"),
            ("ORIG_MA", "cfs_ma"),
            ("DEST_MA", "cfs_ma"),
            ("SCTG", "cfs_sctg"),
            ("EXPORT_YN", "cfs_export"),
        ]
            
if __name__ == "__main__":
    
    w1 = CFS(r"C:\Users\Mitch\Documents\WiNDC\windc_raw_data")
        
        
        
        
        
        