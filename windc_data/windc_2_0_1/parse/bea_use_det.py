import pandas as pd
import os


def _2007(data_dir):
    file = "Use_SUT_Framework_2007_2012_DET.xlsx"

    t = pd.read_excel(
        os.path.join(data_dir, file), sheet_name="2007", skiprows=3, nrows=416
    )

    coldex = [t.loc[0, "Unnamed: 2":].tolist(), t.loc[1, "Unnamed: 2":].tolist()]
    coldex = list(zip(*coldex))
    coldex = pd.MultiIndex.from_tuples(
        coldex, names=["Column_Name", "Commodities/Industries"]
    )

    t.drop([0, 1], inplace=True)
    index = [t["Unnamed: 0"].tolist(), t["Unnamed: 1"].tolist()]
    index = list(zip(*index))
    index = pd.MultiIndex.from_tuples(index, names=["IOCode", "Row_Name"])

    t.set_index(["Unnamed: 0", "Unnamed: 1"], inplace=True)

    # create MultiIndex dataframe for melting
    tt = pd.DataFrame(data=t.values, index=index, columns=coldex)

    tt = pd.melt(tt.reset_index(drop=False), id_vars=["IOCode", "Row_Name"])
    tt.fillna(0, inplace=True)

    # add in year label
    tt["year"] = "2007"

    tt["units"] = "millions of us dollars (USD)"

    # typing
    tt["IOCode"] = tt["IOCode"].map(str)
    tt["Row_Name"] = tt["Row_Name"].map(str)
    tt["Commodities/Industries"] = tt["Commodities/Industries"].map(str)
    tt["Column_Name"] = tt["Column_Name"].map(str)
    tt["value"] = tt["value"].map(float)

    return tt[
        [
            "IOCode",
            "Row_Name",
            "Commodities/Industries",
            "Column_Name",
            "value",
            "year",
            "units",
        ]
    ]


def _2012(data_dir):
    file = "Use_SUT_Framework_2007_2012_DET.xlsx"

    t = pd.read_excel(
        os.path.join(data_dir, file), sheet_name="2012", skiprows=3, nrows=416
    )

    coldex = [t.loc[0, "Unnamed: 2":].tolist(), t.loc[1, "Unnamed: 2":].tolist()]
    coldex = list(zip(*coldex))
    coldex = pd.MultiIndex.from_tuples(
        coldex, names=["Column_Name", "Commodities/Industries"]
    )

    t.drop([0, 1], inplace=True)
    index = [t["Unnamed: 0"].tolist(), t["Unnamed: 1"].tolist()]
    index = list(zip(*index))
    index = pd.MultiIndex.from_tuples(index, names=["IOCode", "Row_Name"])

    t.set_index(["Unnamed: 0", "Unnamed: 1"], inplace=True)

    # create MultiIndex dataframe for melting
    tt = pd.DataFrame(data=t.values, index=index, columns=coldex)

    tt = pd.melt(tt.reset_index(drop=False), id_vars=["IOCode", "Row_Name"])
    tt.fillna(0, inplace=True)

    # add in year label
    tt["year"] = "2012"

    tt["units"] = "millions of us dollars (USD)"

    # typing
    tt["IOCode"] = tt["IOCode"].map(str)
    tt["Row_Name"] = tt["Row_Name"].map(str)
    tt["Commodities/Industries"] = tt["Commodities/Industries"].map(str)
    tt["Column_Name"] = tt["Column_Name"].map(str)
    tt["value"] = tt["value"].map(float)

    return tt[
        [
            "IOCode",
            "Row_Name",
            "Commodities/Industries",
            "Column_Name",
            "value",
            "year",
            "units",
        ]
    ]
