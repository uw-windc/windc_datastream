import pandas as pd
import os


def _crude_oil(data_dir):
    file = "R0000____3a.xlsx"
    t = pd.read_excel(os.path.join(data_dir, file), sheet_name="Data 1", skiprows=2)

    t.rename(
        {
            "Date": "year",
            "U.S. Crude Oil Composite Acquisition Cost by Refiners (Dollars per Barrel)": "price",
        },
        axis="columns",
        inplace=True,
    )

    t["units"] = "us dollars (USD) per barrel"
    t["notes"] = "crude oil composite acquisition cost by refiners"

    # extract year
    t["year"] = t["year"].dt.year

    # typing
    t["year"] = t["year"].map(str)
    t["price"] = t["price"].map(float)
    t["units"] = t["units"].map(str)
    t["notes"] = t["notes"].map(str)

    return t


def _coal_emissions(data_dir):
    file = "coal_CO2_by_state_2013.xlsx"

    t = pd.read_excel(
        os.path.join(data_dir, file), sheet_name="Sheet1", skiprows=2, nrows=52
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


def _natgas_emissions(data_dir):
    file = "natural_gas_CO2_by_state_2013.xlsx"
    t = pd.read_excel(
        os.path.join(data_dir, file), sheet_name="Sheet1", skiprows=2, nrows=52
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


def _petrol_emissions(data_dir):
    file = "petroleum_CO2_by_state_2013.xlsx"
    t = pd.read_excel(
        os.path.join(data_dir, file), sheet_name="Sheet1", skiprows=2, nrows=52
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


def _industrial_emissions(data_dir):
    file = "industrial_CO2_by_state_2013.xlsx"
    t = pd.read_excel(
        os.path.join(data_dir, file), sheet_name="Sheet1", skiprows=2, nrows=52
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


def _commercial_emissions(data_dir):
    file = "commercial_CO2_by_state_2013.xlsx"
    t = pd.read_excel(
        os.path.join(data_dir, file), sheet_name="Sheet1", skiprows=2, nrows=52
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


def _residential_emissions(data_dir):
    file = "residential_CO2_by_state_2013.xlsx"
    t = pd.read_excel(
        os.path.join(data_dir, file), sheet_name="Sheet1", skiprows=2, nrows=52
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


def _electricity_emissions(data_dir):
    file = "electric_CO2_by_state_2013.xlsx"
    t = pd.read_excel(
        os.path.join(data_dir, file), sheet_name="Sheet1", skiprows=2, nrows=52
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


def _transport_emissions(data_dir):
    file = "transportation_CO2_by_state_2013.xlsx"
    t = pd.read_excel(
        os.path.join(data_dir, file), sheet_name="Sheet1", skiprows=2, nrows=52
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


def _seds(data_dir):
    file = "Complete_SEDS_update.csv"

    t = pd.read_csv(
        os.path.join(data_dir, file), index_col=None, engine="c", low_memory=False
    )

    # add in descriptions
    desc = pd.read_excel(
        os.path.join(data_dir, "Codes_and_Descriptions.xlsx"),
        usecols="B:D",
        skiprows=10,
        sheet_name="MSN Descriptions",
    )

    t["full_description"] = t["MSN"].map(dict(zip(desc["MSN"], desc["Description"])))
    t["units"] = t["MSN"].map(dict(zip(desc["MSN"], desc["Unit"])))

    # split out sources, sector and units from MSN code
    t["source"] = t["MSN"].str[0:2]
    t["sector"] = t["MSN"].str[2:4]

    # typing
    t["Data_Status"] = t["Data_Status"].map(str)
    t["MSN"] = t["MSN"].map(str)
    t["StateCode"] = t["StateCode"].map(str)
    t["Year"] = t["Year"].map(str)
    t["Data"] = t["Data"].map(float)
    t["full_description"] = t["full_description"].map(str)
    t["units"] = t["units"].map(str)
    t["source"] = t["source"].map(str)

    return t


def _heatrate(data_dir):
    file = "generator_heat_rates.csv"

    t = pd.read_csv(os.path.join(data_dir, file), index_col=None)

    # add in units
    t["units"] = "btu per kWh generated"

    # reshape
    t = pd.melt(t, id_vars=["year", "units"])

    return t
