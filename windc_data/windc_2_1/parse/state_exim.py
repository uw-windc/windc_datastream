import pandas as pd
import re
import os


def _export(data_dir):
    file = "State Exports by NAICS Commodities.csv"

    t = pd.read_csv(os.path.join(data_dir, file), skiprows=3, engine="c")
    t.dropna(how="all", axis=1, inplace=True)

    # rename in order to aid in joining with import data later
    t.rename(columns={"Total Exports Value ($US)": "value"}, inplace=True)

    # convert values to numeric
    t["value"] = t["value"].replace({",": ""}, regex=True)
    t["value"] = t["value"].map(float)

    # pull NAICS code out to new column
    t["NAICS"] = t["Commodity"].str.split(" ").str[0]
    t["NAICS"] = t["NAICS"].map(str)

    # pull NAICS description out
    t["Commodity Description"] = [
        t.loc[i, "Commodity"].split(str(t.loc[i, "NAICS"]) + " ")[1] for i in t.index
    ]

    # add units label
    t["units"] = "us dollars (USD)"
    t["flow"] = "exports"

    # typing
    t["State"] = t["State"].map(str)
    t["Commodity"] = t["Commodity"].map(str)
    t["Country"] = t["Country"].map(str)
    t["Time"] = t["Time"].map(str)
    t["value"] = t["value"].map(float)
    t["NAICS"] = t["NAICS"].map(str)
    t["Commodity Description"] = t["Commodity Description"].map(str)
    t["units"] = t["units"].map(str)
    t["flow"] = t["flow"].map(str)

    return t


def _import(data_dir):
    file = "State Imports by NAICS Commodities.csv"

    t = pd.read_csv(os.path.join(data_dir, file), skiprows=3, engine="c")
    t.dropna(how="all", axis=1, inplace=True)

    # rename in order to aid in joining with export data later
    t.rename(columns={"Customs Value (Gen) ($US)": "value"}, inplace=True)

    # convert values to numeric
    t["value"] = t["value"].replace({",": ""}, regex=True)
    t["value"] = t["value"].map(float)

    # pull NAICS code out to new column
    t["NAICS"] = t["Commodity"].str.split(" ").str[0]
    t["NAICS"] = t["NAICS"].map(str)

    # pull NAICS description out
    t["Commodity Description"] = [
        t.loc[i, "Commodity"].split(str(t.loc[i, "NAICS"]) + " ")[1] for i in t.index
    ]

    # add units label
    t["units"] = "us dollars (USD)"
    t["flow"] = "imports"

    # typing
    t["State"] = t["State"].map(str)
    t["Commodity"] = t["Commodity"].map(str)
    t["Country"] = t["Country"].map(str)
    t["Time"] = t["Time"].map(str)
    t["value"] = t["value"].map(float)
    t["NAICS"] = t["NAICS"].map(str)
    t["Commodity Description"] = t["Commodity Description"].map(str)
    t["units"] = t["units"].map(str)
    t["flow"] = t["flow"].map(str)

    return t
