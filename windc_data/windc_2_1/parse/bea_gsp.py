import pandas as pd
import os


def _sagdp2n(data_dir):
    file = "SAGDP2N__ALL_AREAS_1997_2019.csv"
    t = pd.read_csv(
        os.path.join(data_dir, file),
        index_col=None,
        engine="c",
        nrows=5464,
        low_memory=False,
    )

    t["GeoFIPS"] = t["GeoFIPS"].replace({'"': ""}, regex=True)
    t["GeoFIPS"] = t["GeoFIPS"].map(int)

    t["GeoName"] = t["GeoName"].replace({"\*": ""}, regex=True)

    # melt data
    t = pd.melt(t, id_vars=t.keys()[0:8], var_name="year")

    t["value"] = pd.to_numeric(t["value"], errors="coerce")
    t.fillna(0, inplace=True)

    t["ComponentName"] = "Gross domestic product (GDP) by state"

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

    return t


def _sagdp3n(data_dir):
    file = "SAGDP3N__ALL_AREAS_1997_2017.csv"
    t = pd.read_csv(
        os.path.join(data_dir, file),
        index_col=None,
        engine="c",
        nrows=5460,
        low_memory=False,
    )

    t["GeoFIPS"] = t["GeoFIPS"].replace({'"': ""}, regex=True)
    t["GeoFIPS"] = t["GeoFIPS"].map(int)

    t["GeoName"] = t["GeoName"].replace({"\*": ""}, regex=True)

    # melt data
    t = pd.melt(t, id_vars=t.keys()[0:8], var_name="year")

    t["value"] = pd.to_numeric(t["value"], errors="coerce")
    t.fillna(0, inplace=True)

    t["ComponentName"] = "Taxes on production and imports less subsidies"

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

    return t


def _sagdp4n(data_dir):
    file = "SAGDP4N__ALL_AREAS_1997_2017.csv"
    t = pd.read_csv(
        os.path.join(data_dir, file),
        index_col=None,
        engine="c",
        nrows=5464,
        low_memory=False,
    )

    t["GeoFIPS"] = t["GeoFIPS"].replace({'"': ""}, regex=True)
    t["GeoFIPS"] = t["GeoFIPS"].map(int)

    t["GeoName"] = t["GeoName"].replace({"\*": ""}, regex=True)

    # melt data
    t = pd.melt(t, id_vars=t.keys()[0:8], var_name="year")

    t["value"] = pd.to_numeric(t["value"], errors="coerce")
    t.fillna(0, inplace=True)

    t["ComponentName"] = "Compensation of employees"

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

    return t


def _sagdp5n(data_dir):
    file = "SAGDP5N__ALL_AREAS_1997_2017.csv"
    t = pd.read_csv(
        os.path.join(data_dir, file),
        index_col=None,
        engine="c",
        nrows=5460,
        low_memory=False,
    )

    t["GeoFIPS"] = t["GeoFIPS"].replace({'"': ""}, regex=True)
    t["GeoFIPS"] = t["GeoFIPS"].map(int)

    t["GeoName"] = t["GeoName"].replace({"\*": ""}, regex=True)

    # melt data
    t = pd.melt(t, id_vars=t.keys()[0:8], var_name="year")

    t["value"] = pd.to_numeric(t["value"], errors="coerce")
    t.fillna(0, inplace=True)

    t["ComponentName"] = "Subsidies"

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

    return t


def _sagdp6n(data_dir):
    file = "SAGDP6N__ALL_AREAS_1997_2017.csv"
    t = pd.read_csv(
        os.path.join(data_dir, file),
        index_col=None,
        engine="c",
        nrows=5460,
        low_memory=False,
    )

    t["GeoFIPS"] = t["GeoFIPS"].replace({'"': ""}, regex=True)
    t["GeoFIPS"] = t["GeoFIPS"].map(int)

    t["GeoName"] = t["GeoName"].replace({"\*": ""}, regex=True)

    # melt data
    t = pd.melt(t, id_vars=t.keys()[0:8], var_name="year")

    t["value"] = pd.to_numeric(t["value"], errors="coerce")
    t.fillna(0, inplace=True)

    t["ComponentName"] = "Taxes on production and imports"

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

    return t


def _sagdp7n(data_dir):
    file = "SAGDP7N__ALL_AREAS_1997_2017.csv"
    t = pd.read_csv(
        os.path.join(data_dir, file),
        index_col=None,
        engine="c",
        nrows=5464,
        low_memory=False,
    )

    t["GeoFIPS"] = t["GeoFIPS"].replace({'"': ""}, regex=True)
    t["GeoFIPS"] = t["GeoFIPS"].map(int)

    t["GeoName"] = t["GeoName"].replace({"\*": ""}, regex=True)

    # melt data
    t = pd.melt(t, id_vars=t.keys()[0:8], var_name="year")

    t["value"] = pd.to_numeric(t["value"], errors="coerce")
    t.fillna(0, inplace=True)

    t["ComponentName"] = "Gross operating surplus"

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

    return t


def _sagdp8n(data_dir):
    file = "SAGDP8N__ALL_AREAS_1997_2019.csv"
    t = pd.read_csv(
        os.path.join(data_dir, file),
        index_col=None,
        engine="c",
        nrows=5460,
        low_memory=False,
    )

    t["GeoFIPS"] = t["GeoFIPS"].replace({'"': ""}, regex=True)
    t["GeoFIPS"] = t["GeoFIPS"].map(int)

    t["GeoName"] = t["GeoName"].replace({"\*": ""}, regex=True)

    # melt data
    t = pd.melt(t, id_vars=t.keys()[0:8], var_name="year")

    t["value"] = pd.to_numeric(t["value"], errors="coerce")
    t.fillna(0, inplace=True)

    t["ComponentName"] = "Quantity indexes for real GDP by state (2012=100.0)"

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

    return t


def _sagdp9n(data_dir):
    file = "SAGDP9N__ALL_AREAS_1997_2019.csv"
    t = pd.read_csv(
        os.path.join(data_dir, file),
        index_col=None,
        engine="c",
        nrows=5460,
        low_memory=False,
    )

    t["GeoFIPS"] = t["GeoFIPS"].replace({'"': ""}, regex=True)
    t["GeoFIPS"] = t["GeoFIPS"].map(int)

    t["GeoName"] = t["GeoName"].replace({"\*": ""}, regex=True)

    # melt data
    t = pd.melt(t, id_vars=t.keys()[0:8], var_name="year")

    t["value"] = pd.to_numeric(t["value"], errors="coerce")
    t.fillna(0, inplace=True)

    t["ComponentName"] = "Real GDP by state"

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

    return t


def _sagdp11n(data_dir):
    file = "SAGDP11N__ALL_AREAS_1998_2019.csv"
    t = pd.read_csv(
        os.path.join(data_dir, file),
        index_col=None,
        engine="c",
        nrows=5460,
        low_memory=False,
    )

    t["GeoFIPS"] = t["GeoFIPS"].replace({'"': ""}, regex=True)
    t["GeoFIPS"] = t["GeoFIPS"].map(int)

    t["GeoName"] = t["GeoName"].replace({"\*": ""}, regex=True)

    # melt data
    t = pd.melt(t, id_vars=t.keys()[0:8], var_name="year")

    t["value"] = pd.to_numeric(t["value"], errors="coerce")
    t.fillna(0, inplace=True)

    t["ComponentName"] = "Contributions to percent change in real GDP"

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

    return t
