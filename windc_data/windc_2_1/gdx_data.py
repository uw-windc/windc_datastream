import copy
from windc_data import gams_windc


def build_data(version, windc_notation, windc_data):
    # build data structure that is compatible with gmsxfr
    data = {}

    # windc version numbers
    data["version"] = {
        "type": "set",
        "elements": "windc_2_1",
        "text": "WiNDC data version number",
    }

    # regions
    regions = copy.deepcopy(windc_notation["region.abbv"])
    regions = regions - {"US"}
    data["r"] = {
        "type": "set",
        "elements": regions,
        "text": "States in WiNDC Database",
    }

    # super regions (states + "US")
    super_regions = copy.deepcopy(regions)
    super_regions.update(["US"])
    data["sr"] = {
        "type": "set",
        "elements": super_regions,
        "text": "States + US in WiNDC Database",
    }

    data["yr"] = {
        "type": "set",
        "elements": windc_notation["year"],
        "text": "Years in WiNDC Database",
    }

    # i -- BEA Goods and sectors categories
    build_i = set(
        gams_windc.gams_maps["bea_all"][
            gams_windc.gams_maps["bea_all"]["category"] == "goods"
        ]["windc_label"]
    )
    data["i"] = {
        "type": "set",
        "elements": build_i,
        "text": "BEA Goods and sectors categories",
    }

    # va -- BEA Value added categories
    build_va = set(
        gams_windc.gams_maps["bea_all"][
            gams_windc.gams_maps["bea_all"]["category"] == "valueadded"
        ]["windc_label"]
    )
    data["va"] = {
        "type": "set",
        "elements": build_va,
        "text": "BEA Value added categories",
    }

    # fd -- BEA Final demand categories
    build_fd = set(
        gams_windc.gams_maps["bea_all"][
            gams_windc.gams_maps["bea_all"]["category"] == "finaldemand"
        ]["windc_label"]
    )
    data["fd"] = {
        "type": "set",
        "elements": build_fd,
        "text": "BEA Final demand categories",
    }

    # ts -- BEA Taxes and subsidies categories
    build_ts = set(
        gams_windc.gams_maps["bea_all"][
            gams_windc.gams_maps["bea_all"]["category"] == "taxessubsidies"
        ]["windc_label"]
    )
    data["ts"] = {
        "type": "set",
        "elements": build_ts,
        "text": "BEA Taxes and subsidies categories",
    }

    # i_det -- Detailed BEA Goods and sectors categories (2007 and 2012 only)
    build_i_det = set(
        gams_windc.gams_maps["bea_all_det"][
            gams_windc.gams_maps["bea_all_det"]["category"] == "goods"
        ]["windc_label"]
    )
    data["i_det"] = {
        "type": "set",
        "elements": build_i_det,
        "text": "Detailed BEA Goods and sectors categories (2007 and 2012 only)",
    }

    # sector_map -- Mapping between detailed and aggregated BEA sectors
    build_sm = list(
        zip(
            gams_windc.gams_maps["bea_all_det"][
                gams_windc.gams_maps["bea_all_det"]["category"] == "goods"
            ]["windc_label"],
            gams_windc.gams_maps["bea_all_det"][
                gams_windc.gams_maps["bea_all_det"]["category"] == "goods"
            ]["windc_aggr_label"],
        )
    )
    data["sector_map"] = {
        "type": "set",
        "elements": build_sm,
        "text": "Mapping between detailed and aggregated BEA sectors",
    }

    # seds_src -- Energy Technologies in EIA SEDS Data
    build_src = set(gams_windc.gams_maps["eia_gen"]["windc_label"])
    data["seds_src"] = {
        "type": "set",
        "elements": build_src,
        "text": "Energy Technologies in EIA SEDS Data",
    }

    # cfsdata_ma_units
    build_cfs_ma = dict(
        zip(
            zip(
                windc_data["cfs_ma"]["ORIG_MA"],
                windc_data["cfs_ma"]["DEST_MA"],
                windc_data["cfs_ma"]["NAICS"],
                windc_data["cfs_ma"]["SCTG"],
                windc_data["cfs_ma"]["units"],
            ),
            windc_data["cfs_ma"]["TOTAL_VALUE"],
        )
    )
    data["cfsdata_ma_units"] = {
        "type": "parameter",
        "elements": build_cfs_ma,
        "text": "CFS - Metro area level shipments (value), with units as domain",
    }

    # cfsdata_st_units
    build_cfs_st = dict(
        zip(
            zip(
                windc_data["cfs_st"]["ORIG_STATE"],
                windc_data["cfs_st"]["DEST_STATE"],
                windc_data["cfs_st"]["NAICS"],
                windc_data["cfs_st"]["SCTG"],
                windc_data["cfs_st"]["units"],
            ),
            windc_data["cfs_st"]["TOTAL_VALUE"],
        )
    )
    data["cfsdata_st_units"] = {
        "type": "parameter",
        "elements": build_cfs_st,
        "text": "CFS - State level shipments (value), with units as domain",
    }

    # co2perbtu_units
    build_co2 = dict(
        zip(
            zip(
                windc_data["emission_rate"]["fuel"],
                windc_data["emission_rate"]["units"],
            ),
            windc_data["emission_rate"]["value"],
        )
    )
    data["co2perbtu_units"] = {
        "type": "parameter",
        "elements": build_co2,
        "text": "Carbon dioxide -- not CO2e -- content, with units as domain",
    }

    # crude_oil_price_units
    build_oilprice = dict(
        zip(
            zip(
                windc_data["eia_crude_price"]["year"],
                windc_data["eia_crude_price"]["units"],
            ),
            windc_data["eia_crude_price"]["price"],
        )
    )
    data["crude_oil_price_units"] = {
        "type": "parameter",
        "elements": build_oilprice,
        "text": "Crude oil composite acquisition cost by refiners, with units as domain",
    }

    # emissions_units
    build_emiss = dict(
        zip(
            zip(
                windc_data["eia_emissions"]["gams.sector"],
                windc_data["eia_emissions"]["State"],
                windc_data["eia_emissions"]["year"],
                windc_data["eia_emissions"]["units"],
            ),
            windc_data["eia_emissions"]["emissions"],
        )
    )
    data["emissions_units"] = {
        "type": "parameter",
        "elements": build_emiss,
        "text": "CO2 emissions by fuel and sector, with units as domain",
    }

    # gsp_units
    build_gsp = windc_data["bea_gsp"].dropna(axis="index", how="any")
    build_gsp = dict(
        zip(
            zip(
                build_gsp["GeoName"],
                build_gsp["year"],
                build_gsp["gams.ComponentName"],
                build_gsp["IndustryId"],
                build_gsp["Unit"],
            ),
            build_gsp["value"],
        )
    )
    data["gsp_units"] = {
        "type": "parameter",
        "elements": build_gsp,
        "text": "Mapped state level annual GDP, with units as domain",
    }

    # heatrate_units
    build_hr = dict(
        zip(
            zip(
                windc_data["eia_heatrate"]["year"],
                windc_data["eia_heatrate"]["gams.variable"],
                windc_data["eia_heatrate"]["units"],
            ),
            windc_data["eia_heatrate"]["value"],
        )
    )
    data["heatrate_units"] = {
        "type": "parameter",
        "elements": build_hr,
        "text": "Electricity generator (avg across tech) heat rate by fuel, with units as domain",
    }

    # nass_units
    build_nass = dict(
        zip(
            zip(
                windc_data["usda_nass"]["State"],
                windc_data["usda_nass"]["Year"],
                windc_data["usda_nass"]["Domain Category"],
                windc_data["usda_nass"]["units"],
            ),
            windc_data["usda_nass"]["Value"],
        )
    )
    data["nass_units"] = {
        "type": "parameter",
        "elements": build_nass,
        "text": "USDA NASS Ag Census 2012 Sales, with units as domain",
    }

    # pce_units
    build_pce = windc_data["bea_pce"].dropna(axis="index", how="any")
    build_pce = dict(
        zip(
            zip(
                build_pce["year"],
                build_pce["GeoName"],
                build_pce["gams.Description"],
                build_pce["Unit"],
            ),
            build_pce["value"],
        )
    )
    data["pce_units"] = {
        "type": "parameter",
        "elements": build_pce,
        "text": "Personal consumer expenditure by commodity (including aggregate subtotals, with units as domain",
    }

    # seds_units
    build_seds = dict(
        zip(
            zip(
                windc_data["eia_seds"]["source"],
                windc_data["eia_seds"]["sector"],
                windc_data["eia_seds"]["StateCode"],
                windc_data["eia_seds"]["Year"],
                windc_data["eia_seds"]["units"],
            ),
            windc_data["eia_seds"]["Data"],
        )
    )
    data["seds_units"] = {
        "type": "parameter",
        "elements": build_seds,
        "text": "Complete EIA SEDS data, with units as domain",
    }

    # sgf_units
    build_sgf = dict(
        zip(
            zip(
                windc_data["census_sgf"]["year"],
                windc_data["census_sgf"]["State"],
                windc_data["census_sgf"]["gams.Category"],
                windc_data["census_sgf"]["units"],
            ),
            windc_data["census_sgf"]["value"],
        )
    )
    data["sgf_units"] = {
        "type": "parameter",
        "elements": build_sgf,
        "text": "State government finances (SGF), with units as domain",
    }

    # usatrd_units
    build_exim = dict(
        zip(
            zip(
                windc_data["state_exim"]["State"],
                windc_data["state_exim"]["NAICS"],
                windc_data["state_exim"]["Time"],
                windc_data["state_exim"]["flow"],
                windc_data["state_exim"]["units"],
            ),
            windc_data["state_exim"]["value"],
        )
    )
    data["usatrd_units"] = {
        "type": "parameter",
        "elements": build_exim,
        "text": "USA trade data, with units as domain",
    }

    # supply_units
    build_supply = dict(
        zip(
            zip(
                windc_data["bea_supply"]["year"],
                windc_data["bea_supply"]["gams.IOCode"],
                windc_data["bea_supply"]["gams.Commodities/Industries"],
                windc_data["bea_supply"]["units"],
            ),
            windc_data["bea_supply"]["value"],
        )
    )
    data["supply_units"] = {
        "type": "parameter",
        "elements": build_supply,
        "text": "Mapped annual supply tables, with units as domain",
    }

    # supply_det_units
    build_supply_det = dict(
        zip(
            zip(
                windc_data["bea_supply_det"]["year"],
                windc_data["bea_supply_det"]["gams.IOCode"],
                windc_data["bea_supply_det"]["gams.Commodities/Industries"],
                windc_data["bea_supply_det"]["units"],
            ),
            windc_data["bea_supply_det"]["value"],
        )
    )
    data["supply_det_units"] = {
        "type": "parameter",
        "elements": build_supply_det,
        "text": "Mapped DETAILED supply tables, with units as domain (2007 and 2012 only)",
    }

    # use_units
    build_use = dict(
        zip(
            zip(
                windc_data["bea_use"]["year"],
                windc_data["bea_use"]["gams.IOCode"],
                windc_data["bea_use"]["gams.Commodities/Industries"],
                windc_data["bea_use"]["units"],
            ),
            windc_data["bea_use"]["value"],
        )
    )
    data["use_units"] = {
        "type": "parameter",
        "elements": build_use,
        "text": "Mapped annual use tables, with units as domain",
    }

    # use_det_units
    build_use_det = dict(
        zip(
            zip(
                windc_data["bea_use_det"]["year"],
                windc_data["bea_use_det"]["gams.IOCode"],
                windc_data["bea_use_det"]["gams.Commodities/Industries"],
                windc_data["bea_use_det"]["units"],
            ),
            windc_data["bea_use_det"]["value"],
        )
    )
    data["use_det_units"] = {
        "type": "parameter",
        "elements": build_use_det,
        "text": "Mapped DETAILED use tables, with units as domain (2007 and 2012 only)",
    }

    return data
