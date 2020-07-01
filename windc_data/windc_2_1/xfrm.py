import os
import pandas as pd


def cfs(df):

    # calculate total value
    df["TOTAL_VALUE"] = df.WGT_FACTOR * df.SHIPMT_VALUE * 1e-6

    # add in units
    df["units"] = "millions of us dollars (USD)"

    # pivot data
    cfs_st = df.pivot_table(
        index=["ORIG_STATE", "DEST_STATE", "NAICS", "SCTG"],
        values=["TOTAL_VALUE"],
        aggfunc=sum,
    )
    cfs_st["units"] = "millions of us dollars (USD)"
    cfs_st.reset_index(inplace=True)

    cfs_ma = df.pivot_table(
        index=["ORIG_MA", "DEST_MA", "NAICS", "SCTG"],
        values=["TOTAL_VALUE"],
        aggfunc=sum,
    )
    cfs_ma["units"] = "millions of us dollars (USD)"
    cfs_ma.reset_index(inplace=True)

    return cfs_st, cfs_ma


def gsp(df):

    # change some units
    df.loc[df[df["Unit"] == "thousands of us dollars (USD)"].index, "value"] = (
        df[df["Unit"] == "thousands of us dollars (USD)"].value * 1e-3
    )

    df.loc[
        df[df["Unit"] == "thousands of us dollars (USD)"].index, "Unit"
    ] = "millions of us dollars (USD)"

    return df


def usda_nass(df):

    # change some units
    df["Value"] = df["Value"] * 1e-6

    df["units"] = "millions of us dollars (USD)"

    return df


def census_sgf(df):

    # change some units
    df["value"] = df["value"] * 1e-3

    df["units"] = "millions of us dollars (USD)"

    return df


def state_exim(df):

    # change some units
    df["value"] = df["value"] * 1e-6

    df["units"] = "millions of us dollars (USD)"

    return df
