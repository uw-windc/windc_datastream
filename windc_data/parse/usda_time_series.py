# -*- coding: utf-8 -*-
"""
Created on Thu Aug  4 10:57:53 2022

@author: Mitch Phillipson
"""

import pandas as pd
import os

from .data_parser import Parser


class USDATimeSeries(Parser):

    def _load_data(self):
        path = os.path.join(self.data_dir,self.data_info["data"]["path"])
        
        self.data["time"] = pd.read_excel(
            path,
            "Total exports",
            skiprows = 2,
            usecols = self.data_info["data"]["colrange"],
            nrows = 52
        )

    def clean(self):

        df = self.data["time"]

        df = df.rename(columns = {"Unnamed: 0":"state"})
        df = df.drop([0,1])
        df = df.melt(id_vars = df.columns[0], value_vars = df.columns[1:], var_name = "year")
        df["value"] = pd.to_numeric(df["value"])
        self.data["time"] = df


    def build_notations(self):
        self.notation_link = [
            ("year","year"),
            ("state","region.fullname")
        ]

    def _build_gdx_dict(self):

        gdx_dict = {}

        df = self.df

        ayr = df["year"].unique()
        gdx_dict["ayr"] = {"type":"set",
                           "elements":ayr,
                           "text":"Dynamically created set from parameter usda"
        }

        gdx_dict["usda"] = {"type":"parameter",
                            "domain":["r","ayr"],
                            "elements":df,
                            "text":"State level exports from usda of total agricultural output"
        }

        return gdx_dict