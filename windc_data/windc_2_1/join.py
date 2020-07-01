import os
import pandas as pd


def bea_use(data_dir):
    from .parse import bea_use

    data_dir = os.path.join(data_dir, "windc_2_1", "BEA", "IO")

    df = []
    for i in dir(bea_use):
        if callable(getattr(bea_use, i)):
            df.append(getattr(bea_use, i)(data_dir))

    df = pd.concat(df, ignore_index=True)

    return df


def bea_supply(data_dir):
    from .parse import bea_supply

    data_dir = os.path.join(data_dir, "windc_2_1", "BEA", "IO")

    df = []
    for i in dir(bea_supply):
        if callable(getattr(bea_supply, i)):
            df.append(getattr(bea_supply, i)(data_dir))

    df = pd.concat(df, ignore_index=True)

    return df


def cfs(data_dir):
    # pass through only, no join necessary
    from .parse import cfs

    data_dir = os.path.join(data_dir, "windc_2_1", "CFS")

    t, a1, a2, a3, a4 = cfs._2012(data_dir)

    return t


def bea_use_det(data_dir):
    from .parse import bea_use_det

    data_dir = os.path.join(data_dir, "windc_2_1", "BEA_2007_2012")

    df = []
    for i in dir(bea_use_det):
        if callable(getattr(bea_use_det, i)):
            df.append(getattr(bea_use_det, i)(data_dir))

    df = pd.concat(df, ignore_index=True)

    return df


def bea_supply_det(data_dir):
    from .parse import bea_supply_det

    data_dir = os.path.join(data_dir, "windc_2_1", "BEA_2007_2012")

    df = []
    for i in dir(bea_supply_det):
        if callable(getattr(bea_supply_det, i)):
            df.append(getattr(bea_supply_det, i)(data_dir))

    df = pd.concat(df, ignore_index=True)

    return df


def eia_emissions(data_dir):
    from .parse import eia

    data_dir = os.path.join(data_dir, "windc_2_1", "SEDS", "Emissions")

    df = []
    for i in [
        "_coal_emissions",
        "_natgas_emissions",
        "_petrol_emissions",
        "_industrial_emissions",
        "_commercial_emissions",
        "_residential_emissions",
        "_electricity_emissions",
        "_transport_emissions",
    ]:
        df.append(getattr(eia, i)(data_dir))

    df = pd.concat(df, ignore_index=True)

    return df


def eia_crude_price(data_dir):
    from .parse import eia

    data_dir = os.path.join(data_dir, "windc_2_1", "SEDS", "CrudeOil")

    df = []
    for i in [
        "_crude_oil",
    ]:
        df.append(getattr(eia, i)(data_dir))

    df = pd.concat(df, ignore_index=True)

    return df


def eia_seds(data_dir):
    from .parse import eia

    data_dir = os.path.join(data_dir, "windc_2_1", "SEDS")

    return eia._seds(data_dir)


def eia_heatrate(data_dir):
    from .parse import eia

    data_dir = os.path.join(data_dir, "windc_2_1", "SEDS")

    return eia._heatrate(data_dir)


def usda_nass(data_dir):
    from .parse import nass

    data_dir = os.path.join(data_dir, "windc_2_1", "NASS")

    return nass._all(data_dir)


def bea_pce(data_dir):
    from .parse import bea_pce

    data_dir = os.path.join(data_dir, "windc_2_1", "PCE")

    df = []
    for i in [
        "_saexp1",
        "_saexp2",
    ]:
        df.append(getattr(bea_pce, i)(data_dir))

    df = pd.concat(df, ignore_index=True)

    return df


def bea_gsp(data_dir):
    from .parse import bea_gsp

    data_dir = os.path.join(data_dir, "windc_2_1", "BEA", "GDP")

    df = []
    for i in dir(bea_gsp):
        if callable(getattr(bea_gsp, i)):
            df.append(getattr(bea_gsp, i)(data_dir))

    df = pd.concat(df, ignore_index=True)

    return df


def state_exim(data_dir):
    from .parse import state_exim

    data_dir = os.path.join(data_dir, "windc_2_1", "USATradeOnline")

    df = []
    for i in dir(state_exim):
        if callable(getattr(state_exim, i)):
            df.append(getattr(state_exim, i)(data_dir))

    df = pd.concat(df, ignore_index=True)

    return df


def census_sgf(data_dir):
    from .parse import census_sgf

    data_dir = os.path.join(data_dir, "windc_2_1", "SGF")

    df = []
    for i in dir(census_sgf):
        if callable(getattr(census_sgf, i)):
            df.append(getattr(census_sgf, i)(data_dir))

    df = pd.concat(df, ignore_index=True)

    return df


def emission_rate(data_dir):

    emiss = {
        ("oil", "kilograms CO2 per million btu"): 70,
        ("col", "kilograms CO2 per million btu"): 95,
        ("gas", "kilograms CO2 per million btu"): 53,
        ("cru", "kilograms CO2 per million btu"): 70,
    }
    df = pd.DataFrame(data=emiss.keys(), columns=["fuel", "units"])
    df["value"] = emiss.values()

    return df
