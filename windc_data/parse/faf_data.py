# -*- coding: utf-8 -*-
"""
Created on Thu Aug  4 10:57:53 2022

@author: Mitch Phillipson
"""

import pandas as pd
import os

from .data_parser import Parser

class FAF(Parser):

    def _load_data(self):

        self.data["state"] = pd.read_csv(os.path.join(self.data_dir,self.data_info["data"]["state"]))
        self.data["reprocessed"] = pd.read_csv(os.path.join(self.data_dir,self.data_info["data"]["reprocessed"]))


    def clean(self):

        df = self.data["state"]
        cols_to_drop = ["fr_orig","fr_dest","fr_inmode","fr_outmode","trade_type"]
        cols_to_drop.extend([a for a in df.columns if "current_value" in a or "tons" in a or "tmiles" in a])

        X = df.drop(labels = cols_to_drop, axis = 1)

        id_vars = ["dms_origst","dms_destst","dms_mode","sctg2","dist_band"]
        pivot_cols = [a for a in X.columns if "value" in a]

        X = X.melt(id_vars = id_vars, value_vars = pivot_cols, var_name = "year")

        X["year"] = X["year"].str.strip("value_")

        X = (X.groupby(["dms_origst","dms_destst","sctg2","year"])
        .agg({"value":sum})
        .reset_index()
        )

        X["dms_origst"] = X["dms_origst"].apply(str)
        X["dms_destst"] = X["dms_destst"].apply(str)

        self.data["state"] = X

        df2 = self.data["reprocessed"]
        cols_to_drop = ["fr_orig","fr_dest","fr_inmode","fr_outmode","trade_type"]
        cols_to_drop.extend([a for a in df2.columns if "current_value" in a or "tons" in a or "tmiles" in a])

        Y = df2.drop(labels = cols_to_drop, axis = 1)

        id_vars = ["dms_origst","dms_destst","dms_mode","sctg2"]
        pivot_cols = [a for a in Y.columns if "value" in a]

        Y = Y.melt(id_vars = id_vars, value_vars = pivot_cols, var_name = "year")

        Y["year"] = Y["year"].str.strip("value_")

        Y = (Y.groupby(["dms_origst","dms_destst","sctg2","year"])
        .agg({"value":sum})
        .reset_index()
        )

        Y["dms_origst"] = Y["dms_origst"].apply(str)
        Y["dms_destst"] = Y["dms_destst"].apply(str)

        self.data["reprocessed"] = Y

    def build_notations(self):
        self.notation_link = [
            ("year","year"),
            ("dms_origst", "fips.state"),
            ("dms_destst", "fips.state"),
        ]

    def _build_gdx_dict(self):
        gdx_dict = {}

        df = self.df
        
        df = df.rename(columns = {"dms_origst":"orig","dms_destst":"dest"})



        gdx_dict["faf_units"] = {"type":"parameter",
                                 "domain": ["sr","sr","sg","yr"],
                                 "elements":df,
                                 "text":"FAF Data"
        }

        return gdx_dict
