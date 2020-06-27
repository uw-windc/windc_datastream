import pandas as pd
import os


def _1997(data_dir):
    file = "Use_SUT_Framework_1997-2017_SUM.xlsx"

    t = pd.read_excel(
        os.path.join(data_dir, file), sheet_name="1997", skiprows=4, na_values=["..."]
    )

    coldex = [t.loc[0, "Unnamed: 2":].tolist(), t.loc[1, "Unnamed: 2":].tolist()]
    coldex = list(zip(*coldex))
    coldex = pd.MultiIndex.from_tuples(
        coldex, names=["Commodities/Industries", "Column_Name"]
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
    tt["year"] = "1997"

    tt["units"] = "millions of us dollars (USD)"

    # typing
    tt["IOCode"] = tt["IOCode"].map(str)
    tt["Row_Name"] = tt["Row_Name"].map(str)
    tt["Commodities/Industries"] = tt["Commodities/Industries"].map(str)
    tt["Column_Name"] = tt["Column_Name"].map(str)
    tt["value"] = tt["value"].map(float)

    return tt


def _1998(data_dir):
    file = "Use_SUT_Framework_1997-2017_SUM.xlsx"

    t = pd.read_excel(
        os.path.join(data_dir, file), sheet_name="1998", skiprows=4, na_values=["..."]
    )

    coldex = [t.loc[0, "Unnamed: 2":].tolist(), t.loc[1, "Unnamed: 2":].tolist()]
    coldex = list(zip(*coldex))
    coldex = pd.MultiIndex.from_tuples(
        coldex, names=["Commodities/Industries", "Column_Name"]
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
    tt["year"] = "1998"

    tt["units"] = "millions of us dollars (USD)"

    # typing
    tt["IOCode"] = tt["IOCode"].map(str)
    tt["Row_Name"] = tt["Row_Name"].map(str)
    tt["Commodities/Industries"] = tt["Commodities/Industries"].map(str)
    tt["Column_Name"] = tt["Column_Name"].map(str)
    tt["value"] = tt["value"].map(float)

    return tt


def _1999(data_dir):
    file = "Use_SUT_Framework_1997-2017_SUM.xlsx"

    t = pd.read_excel(
        os.path.join(data_dir, file), sheet_name="1999", skiprows=4, na_values=["..."]
    )

    coldex = [t.loc[0, "Unnamed: 2":].tolist(), t.loc[1, "Unnamed: 2":].tolist()]
    coldex = list(zip(*coldex))
    coldex = pd.MultiIndex.from_tuples(
        coldex, names=["Commodities/Industries", "Column_Name"]
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
    tt["year"] = "1999"

    tt["units"] = "millions of us dollars (USD)"

    tt["units"] = "millions of us dollars (USD)"

    # typing
    tt["IOCode"] = tt["IOCode"].map(str)
    tt["Row_Name"] = tt["Row_Name"].map(str)
    tt["Commodities/Industries"] = tt["Commodities/Industries"].map(str)
    tt["Column_Name"] = tt["Column_Name"].map(str)
    tt["value"] = tt["value"].map(float)

    return tt


def _2000(data_dir):
    file = "Use_SUT_Framework_1997-2017_SUM.xlsx"

    t = pd.read_excel(
        os.path.join(data_dir, file), sheet_name="2000", skiprows=4, na_values=["..."]
    )

    coldex = [t.loc[0, "Unnamed: 2":].tolist(), t.loc[1, "Unnamed: 2":].tolist()]
    coldex = list(zip(*coldex))
    coldex = pd.MultiIndex.from_tuples(
        coldex, names=["Commodities/Industries", "Column_Name"]
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
    tt["year"] = "2000"

    tt["units"] = "millions of us dollars (USD)"

    # typing
    tt["IOCode"] = tt["IOCode"].map(str)
    tt["Row_Name"] = tt["Row_Name"].map(str)
    tt["Commodities/Industries"] = tt["Commodities/Industries"].map(str)
    tt["Column_Name"] = tt["Column_Name"].map(str)
    tt["value"] = tt["value"].map(float)

    return tt


def _2001(data_dir):
    file = "Use_SUT_Framework_1997-2017_SUM.xlsx"

    t = pd.read_excel(
        os.path.join(data_dir, file), sheet_name="2001", skiprows=4, na_values=["..."]
    )

    coldex = [t.loc[0, "Unnamed: 2":].tolist(), t.loc[1, "Unnamed: 2":].tolist()]
    coldex = list(zip(*coldex))
    coldex = pd.MultiIndex.from_tuples(
        coldex, names=["Commodities/Industries", "Column_Name"]
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
    tt["year"] = "2001"

    tt["units"] = "millions of us dollars (USD)"

    # typing
    tt["IOCode"] = tt["IOCode"].map(str)
    tt["Row_Name"] = tt["Row_Name"].map(str)
    tt["Commodities/Industries"] = tt["Commodities/Industries"].map(str)
    tt["Column_Name"] = tt["Column_Name"].map(str)
    tt["value"] = tt["value"].map(float)

    return tt


def _2002(data_dir):
    file = "Use_SUT_Framework_1997-2017_SUM.xlsx"

    t = pd.read_excel(
        os.path.join(data_dir, file), sheet_name="2002", skiprows=4, na_values=["..."]
    )

    coldex = [t.loc[0, "Unnamed: 2":].tolist(), t.loc[1, "Unnamed: 2":].tolist()]
    coldex = list(zip(*coldex))
    coldex = pd.MultiIndex.from_tuples(
        coldex, names=["Commodities/Industries", "Column_Name"]
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
    tt["year"] = "2002"

    tt["units"] = "millions of us dollars (USD)"

    # typing
    tt["IOCode"] = tt["IOCode"].map(str)
    tt["Row_Name"] = tt["Row_Name"].map(str)
    tt["Commodities/Industries"] = tt["Commodities/Industries"].map(str)
    tt["Column_Name"] = tt["Column_Name"].map(str)
    tt["value"] = tt["value"].map(float)

    return tt


def _2003(data_dir):
    file = "Use_SUT_Framework_1997-2017_SUM.xlsx"

    t = pd.read_excel(
        os.path.join(data_dir, file), sheet_name="2003", skiprows=4, na_values=["..."],
    )

    coldex = [t.loc[0, "Unnamed: 2":].tolist(), t.loc[1, "Unnamed: 2":].tolist()]
    coldex = list(zip(*coldex))
    coldex = pd.MultiIndex.from_tuples(
        coldex, names=["Commodities/Industries", "Column_Name"]
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
    tt["year"] = "2003"

    tt["units"] = "millions of us dollars (USD)"

    # typing
    tt["IOCode"] = tt["IOCode"].map(str)
    tt["Row_Name"] = tt["Row_Name"].map(str)
    tt["Commodities/Industries"] = tt["Commodities/Industries"].map(str)
    tt["Column_Name"] = tt["Column_Name"].map(str)
    tt["value"] = tt["value"].map(float)

    return tt


def _2004(data_dir):
    file = "Use_SUT_Framework_1997-2017_SUM.xlsx"

    t = pd.read_excel(
        os.path.join(data_dir, file), sheet_name="2004", skiprows=4, na_values=["..."]
    )

    coldex = [t.loc[0, "Unnamed: 2":].tolist(), t.loc[1, "Unnamed: 2":].tolist()]
    coldex = list(zip(*coldex))
    coldex = pd.MultiIndex.from_tuples(
        coldex, names=["Commodities/Industries", "Column_Name"]
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
    tt["year"] = "2004"

    tt["units"] = "millions of us dollars (USD)"

    # typing
    tt["IOCode"] = tt["IOCode"].map(str)
    tt["Row_Name"] = tt["Row_Name"].map(str)
    tt["Commodities/Industries"] = tt["Commodities/Industries"].map(str)
    tt["Column_Name"] = tt["Column_Name"].map(str)
    tt["value"] = tt["value"].map(float)

    return tt


def _2005(data_dir):
    file = "Use_SUT_Framework_1997-2017_SUM.xlsx"

    t = pd.read_excel(
        os.path.join(data_dir, file), sheet_name="2005", skiprows=4, na_values=["..."]
    )

    coldex = [t.loc[0, "Unnamed: 2":].tolist(), t.loc[1, "Unnamed: 2":].tolist()]
    coldex = list(zip(*coldex))
    coldex = pd.MultiIndex.from_tuples(
        coldex, names=["Commodities/Industries", "Column_Name"]
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
    tt["year"] = "2005"

    tt["units"] = "millions of us dollars (USD)"

    # typing
    tt["IOCode"] = tt["IOCode"].map(str)
    tt["Row_Name"] = tt["Row_Name"].map(str)
    tt["Commodities/Industries"] = tt["Commodities/Industries"].map(str)
    tt["Column_Name"] = tt["Column_Name"].map(str)
    tt["value"] = tt["value"].map(float)

    return tt


def _2006(data_dir):
    file = "Use_SUT_Framework_1997-2017_SUM.xlsx"

    t = pd.read_excel(
        os.path.join(data_dir, file), sheet_name="2006", skiprows=4, na_values=["..."]
    )

    coldex = [t.loc[0, "Unnamed: 2":].tolist(), t.loc[1, "Unnamed: 2":].tolist()]
    coldex = list(zip(*coldex))
    coldex = pd.MultiIndex.from_tuples(
        coldex, names=["Commodities/Industries", "Column_Name"]
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
    tt["year"] = "2006"

    tt["units"] = "millions of us dollars (USD)"

    # typing
    tt["IOCode"] = tt["IOCode"].map(str)
    tt["Row_Name"] = tt["Row_Name"].map(str)
    tt["Commodities/Industries"] = tt["Commodities/Industries"].map(str)
    tt["Column_Name"] = tt["Column_Name"].map(str)
    tt["value"] = tt["value"].map(float)

    return tt


def _2007(data_dir):
    file = "Use_SUT_Framework_1997-2017_SUM.xlsx"

    t = pd.read_excel(
        os.path.join(data_dir, file), sheet_name="2007", skiprows=4, na_values=["..."]
    )

    coldex = [t.loc[0, "Unnamed: 2":].tolist(), t.loc[1, "Unnamed: 2":].tolist()]
    coldex = list(zip(*coldex))
    coldex = pd.MultiIndex.from_tuples(
        coldex, names=["Commodities/Industries", "Column_Name"]
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

    return tt


def _2008(data_dir):
    file = "Use_SUT_Framework_1997-2017_SUM.xlsx"

    t = pd.read_excel(
        os.path.join(data_dir, file), sheet_name="2008", skiprows=4, na_values=["..."]
    )

    coldex = [t.loc[0, "Unnamed: 2":].tolist(), t.loc[1, "Unnamed: 2":].tolist()]
    coldex = list(zip(*coldex))
    coldex = pd.MultiIndex.from_tuples(
        coldex, names=["Commodities/Industries", "Column_Name"]
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
    tt["year"] = "2008"

    tt["units"] = "millions of us dollars (USD)"

    # typing
    tt["IOCode"] = tt["IOCode"].map(str)
    tt["Row_Name"] = tt["Row_Name"].map(str)
    tt["Commodities/Industries"] = tt["Commodities/Industries"].map(str)
    tt["Column_Name"] = tt["Column_Name"].map(str)
    tt["value"] = tt["value"].map(float)

    return tt


def _2009(data_dir):
    file = "Use_SUT_Framework_1997-2017_SUM.xlsx"

    t = pd.read_excel(
        os.path.join(data_dir, file), sheet_name="2009", skiprows=4, na_values=["..."]
    )

    coldex = [t.loc[0, "Unnamed: 2":].tolist(), t.loc[1, "Unnamed: 2":].tolist()]
    coldex = list(zip(*coldex))
    coldex = pd.MultiIndex.from_tuples(
        coldex, names=["Commodities/Industries", "Column_Name"]
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
    tt["year"] = "2009"

    tt["units"] = "millions of us dollars (USD)"

    # typing
    tt["IOCode"] = tt["IOCode"].map(str)
    tt["Row_Name"] = tt["Row_Name"].map(str)
    tt["Commodities/Industries"] = tt["Commodities/Industries"].map(str)
    tt["Column_Name"] = tt["Column_Name"].map(str)
    tt["value"] = tt["value"].map(float)

    return tt


def _2010(data_dir):
    file = "Use_SUT_Framework_1997-2017_SUM.xlsx"

    t = pd.read_excel(
        os.path.join(data_dir, file), sheet_name="2010", skiprows=4, na_values=["..."]
    )

    coldex = [t.loc[0, "Unnamed: 2":].tolist(), t.loc[1, "Unnamed: 2":].tolist()]
    coldex = list(zip(*coldex))
    coldex = pd.MultiIndex.from_tuples(
        coldex, names=["Commodities/Industries", "Column_Name"]
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
    tt["year"] = "2010"

    tt["units"] = "millions of us dollars (USD)"

    # typing
    tt["IOCode"] = tt["IOCode"].map(str)
    tt["Row_Name"] = tt["Row_Name"].map(str)
    tt["Commodities/Industries"] = tt["Commodities/Industries"].map(str)
    tt["Column_Name"] = tt["Column_Name"].map(str)
    tt["value"] = tt["value"].map(float)

    return tt


def _2011(data_dir):
    file = "Use_SUT_Framework_1997-2017_SUM.xlsx"

    t = pd.read_excel(
        os.path.join(data_dir, file), sheet_name="2011", skiprows=4, na_values=["..."]
    )

    coldex = [t.loc[0, "Unnamed: 2":].tolist(), t.loc[1, "Unnamed: 2":].tolist()]
    coldex = list(zip(*coldex))
    coldex = pd.MultiIndex.from_tuples(
        coldex, names=["Commodities/Industries", "Column_Name"]
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
    tt["year"] = "2011"

    tt["units"] = "millions of us dollars (USD)"

    # typing
    tt["IOCode"] = tt["IOCode"].map(str)
    tt["Row_Name"] = tt["Row_Name"].map(str)
    tt["Commodities/Industries"] = tt["Commodities/Industries"].map(str)
    tt["Column_Name"] = tt["Column_Name"].map(str)
    tt["value"] = tt["value"].map(float)

    return tt


def _2012(data_dir):
    file = "Use_SUT_Framework_1997-2017_SUM.xlsx"

    t = pd.read_excel(
        os.path.join(data_dir, file), sheet_name="2012", skiprows=4, na_values=["..."]
    )

    coldex = [t.loc[0, "Unnamed: 2":].tolist(), t.loc[1, "Unnamed: 2":].tolist()]
    coldex = list(zip(*coldex))
    coldex = pd.MultiIndex.from_tuples(
        coldex, names=["Commodities/Industries", "Column_Name"]
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

    return tt


def _2013(data_dir):
    file = "Use_SUT_Framework_1997-2017_SUM.xlsx"

    t = pd.read_excel(
        os.path.join(data_dir, file), sheet_name="2013", skiprows=4, na_values=["..."]
    )

    coldex = [t.loc[0, "Unnamed: 2":].tolist(), t.loc[1, "Unnamed: 2":].tolist()]
    coldex = list(zip(*coldex))
    coldex = pd.MultiIndex.from_tuples(
        coldex, names=["Commodities/Industries", "Column_Name"]
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
    tt["year"] = "2013"

    tt["units"] = "millions of us dollars (USD)"

    # typing
    tt["IOCode"] = tt["IOCode"].map(str)
    tt["Row_Name"] = tt["Row_Name"].map(str)
    tt["Commodities/Industries"] = tt["Commodities/Industries"].map(str)
    tt["Column_Name"] = tt["Column_Name"].map(str)
    tt["value"] = tt["value"].map(float)

    return tt


def _2014(data_dir):
    file = "Use_SUT_Framework_1997-2017_SUM.xlsx"

    t = pd.read_excel(
        os.path.join(data_dir, file), sheet_name="2014", skiprows=4, na_values=["..."]
    )

    coldex = [t.loc[0, "Unnamed: 2":].tolist(), t.loc[1, "Unnamed: 2":].tolist()]
    coldex = list(zip(*coldex))
    coldex = pd.MultiIndex.from_tuples(
        coldex, names=["Commodities/Industries", "Column_Name"]
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
    tt["year"] = "2014"

    tt["units"] = "millions of us dollars (USD)"

    # typing
    tt["IOCode"] = tt["IOCode"].map(str)
    tt["Row_Name"] = tt["Row_Name"].map(str)
    tt["Commodities/Industries"] = tt["Commodities/Industries"].map(str)
    tt["Column_Name"] = tt["Column_Name"].map(str)
    tt["value"] = tt["value"].map(float)

    return tt


def _2015(data_dir):
    file = "Use_SUT_Framework_1997-2017_SUM.xlsx"

    t = pd.read_excel(
        os.path.join(data_dir, file), sheet_name="2015", skiprows=4, na_values=["..."]
    )

    coldex = [t.loc[0, "Unnamed: 2":].tolist(), t.loc[1, "Unnamed: 2":].tolist()]
    coldex = list(zip(*coldex))
    coldex = pd.MultiIndex.from_tuples(
        coldex, names=["Commodities/Industries", "Column_Name"]
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
    tt["year"] = "2015"

    tt["units"] = "millions of us dollars (USD)"

    # typing
    tt["IOCode"] = tt["IOCode"].map(str)
    tt["Row_Name"] = tt["Row_Name"].map(str)
    tt["Commodities/Industries"] = tt["Commodities/Industries"].map(str)
    tt["Column_Name"] = tt["Column_Name"].map(str)
    tt["value"] = tt["value"].map(float)

    return tt


def _2016(data_dir):
    file = "Use_SUT_Framework_1997-2017_SUM.xlsx"

    t = pd.read_excel(
        os.path.join(data_dir, file), sheet_name="2016", skiprows=4, na_values=["..."]
    )

    coldex = [t.loc[0, "Unnamed: 2":].tolist(), t.loc[1, "Unnamed: 2":].tolist()]
    coldex = list(zip(*coldex))
    coldex = pd.MultiIndex.from_tuples(
        coldex, names=["Commodities/Industries", "Column_Name"]
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
    tt["year"] = "2016"

    tt["units"] = "millions of us dollars (USD)"

    # typing
    tt["IOCode"] = tt["IOCode"].map(str)
    tt["Row_Name"] = tt["Row_Name"].map(str)
    tt["Commodities/Industries"] = tt["Commodities/Industries"].map(str)
    tt["Column_Name"] = tt["Column_Name"].map(str)
    tt["value"] = tt["value"].map(float)

    return tt


def _2017(data_dir):
    file = "Use_SUT_Framework_1997-2017_SUM.xlsx"

    t = pd.read_excel(
        os.path.join(data_dir, file), sheet_name="2017", skiprows=4, na_values=["..."]
    )

    coldex = [t.loc[0, "Unnamed: 2":].tolist(), t.loc[1, "Unnamed: 2":].tolist()]
    coldex = list(zip(*coldex))
    coldex = pd.MultiIndex.from_tuples(
        coldex, names=["Commodities/Industries", "Column_Name"]
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
    tt["year"] = "2017"

    tt["units"] = "millions of us dollars (USD)"

    # typing
    tt["IOCode"] = tt["IOCode"].map(str)
    tt["Row_Name"] = tt["Row_Name"].map(str)
    tt["Commodities/Industries"] = tt["Commodities/Industries"].map(str)
    tt["Column_Name"] = tt["Column_Name"].map(str)
    tt["value"] = tt["value"].map(float)

    return tt
