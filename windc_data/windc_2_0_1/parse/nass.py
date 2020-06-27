import pandas as pd
import os


def _2012(data_dir):
    file = "agcensus_2012_sales_redownload.csv"

    t = pd.read_csv(os.path.join(data_dir, file), index_col=None)

    t["Value"] = [t.loc[i, "Value"].replace(",", "") for i in t.index]
    t["Value"] = pd.to_numeric(t["Value"], errors="coerce")

    t["Value"].fillna(0, inplace=True)

    t["Domain Category"] = [
        t.loc[i, "Domain Category"].split(": (")[1] for i in t.index
    ]
    t["Domain Category"] = [t.loc[i, "Domain Category"].split(")")[0] for i in t.index]
    t["Domain Category"] = t["Domain Category"].map(str)

    t["CV (%)"] = pd.to_numeric(t["CV (%)"], errors="coerce")
    t["CV (%)"].fillna(0, inplace=True)

    # drop unused columns (all rows = nan)
    t.dropna(axis=1, how="all", inplace=True)

    t["units"] = "us dollars (USD)"

    # typing
    t["Program"] = t["Program"].map(str)
    t["Year"] = t["Year"].map(str)
    t["Period"] = t["Period"].map(str)
    t["Geo Level"] = t["Geo Level"].map(str)
    t["State"] = t["State"].map(str)
    t["State ANSI"] = t["State ANSI"].map(str)
    t["watershed_code"] = t["watershed_code"].map(str)
    t["Commodity"] = t["Commodity"].map(str)
    t["Data Item"] = t["Data Item"].map(str)
    t["Domain"] = t["Domain"].map(str)
    t["Domain Category"] = t["Domain Category"].map(str)
    t["Value"] = t["Value"].map(float)
    t["CV (%)"] = t["CV (%)"].map(float)
    t["units"] = t["units"].map(str)

    return t
