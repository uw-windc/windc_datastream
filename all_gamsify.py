import pandas as pd
import gdxtools as gt
import glob
import argparse
import os
import scrub_bea_supply
import scrub_bea_use
import scrub_bea_supply_detailed
import scrub_bea_use_detailed
import scrub_cfs
import scrub_sgf
import scrub_gsp
import scrub_pce
import scrub_emissions
import scrub_nass
import scrub_eia_crude_oil
import scrub_usatrade
import scrub_seds
import scrub_heatrate


def check_gams_set(filename):
    s = pd.read_csv(filename, index_col=None)
    if len(s.keys().unique()) != len(s.keys()):
        raise Exception('ERROR: GAMS set definition in ' +
                        filename + ' has duplicate entities, must fix')


def check_gams_map(filename):
    m = pd.read_csv(filename, index_col=None)

    if len(m['from'].unique()) != len(m['to'].unique()):
        raise Exception('ERROR: Mapping specifified in file ' + filename + ' does not map 1 to 1')


def find_gams_maps(path):
    f = glob.glob(path+'map*.csv')
    if len(f) == 0:
        raise Exception('ERROR: no gams mapping files found in ' + path)
    return f


def find_gams_sets(path):
    f = glob.glob(path+'set*.csv')
    if len(f) == 0:
        raise Exception('ERROR: no gams set files found in ' + path)
    return f


def check_gams_maps():
    f = find_gams_maps(path='.{s}core_maps{s}gams{s}'.format(s=os.sep))
    for i in f:
        check_gams_map(filename=i)


def check_gams_sets():
    f = find_gams_sets(path='.{s}core_maps{s}gams{s}'.format(s=os.sep))
    for i in f:
        check_gams_set(filename=i)


def gams_bea_use(from_sql):
    use = scrub_bea_use.scrub_use(from_sql=from_sql)

    # drop all zero values
    use.drop(use[use['value'] == 0].index, inplace=True)

    # category mapping
    bea_map = pd.read_csv('.{s}core_maps{s}bea_all.csv'.format(s=os.sep), index_col=None)
    bea_map = dict(zip(bea_map['from'], bea_map['to']))

    use['input_code'] = use['input_code'].map(bea_map)
    use['output_code'] = use['output_code'].map(bea_map)

    # organize columns a bit
    use = use[['year', 'input_code', 'output_code', 'value', 'units']]
    return use


def gams_bea_use_detailed(from_sql):
    use = scrub_bea_use_detailed.scrub_use(from_sql=from_sql)

    # drop all zero values
    use.drop(use[use['value'] == 0].index, inplace=True)

    # category mapping
    bea_map = pd.read_csv('.{s}core_maps{s}bea_all_detailed.csv'.format(s=os.sep), index_col=None)
    bea_map = dict(zip(bea_map['from'], bea_map['to']))

    use['from_industry'] = use['from_industry'].map(bea_map)
    use['to_industry'] = use['to_industry'].map(bea_map)

    # organize columns a bit
    use = use[['year', 'from_industry', 'to_industry', 'value', 'units']]
    return use


def gams_bea_supply_detailed(from_sql):
    supply = scrub_bea_supply_detailed.scrub_supply(from_sql=from_sql)

    # drop all zero values
    supply.drop(supply[supply['value'] == 0].index, inplace=True)

    # category mapping
    bea_map = pd.read_csv('.{s}core_maps{s}bea_all_detailed.csv'.format(s=os.sep), index_col=None)
    bea_map = dict(zip(bea_map['from'], bea_map['to']))

    supply['from_industry'] = supply['from_industry'].map(bea_map)
    supply['to_industry'] = supply['to_industry'].map(bea_map)

    # organize columns a bit
    supply = supply[['year', 'from_industry', 'to_industry', 'value', 'units']]
    return supply


def gams_bea_supply(from_sql):
    supply = scrub_bea_supply.scrub_supply(from_sql=from_sql)

    # drop all zero values
    supply.drop(supply[supply['value'] == 0].index, inplace=True)

    # category mapping
    bea_map = pd.read_csv('.{s}core_maps{s}bea_all.csv'.format(s=os.sep), index_col=None)
    bea_map = dict(zip(bea_map['from'], bea_map['to']))

    supply['input_code'] = supply['input_code'].map(bea_map)
    supply['output_code'] = supply['output_code'].map(bea_map)

    # organize columns a bit
    supply = supply[['year', 'input_code', 'output_code', 'value', 'units']]

    return supply


def add_bea_categories_to_gdx():
    # category mapping
    bea_map = pd.read_csv('.{s}core_maps{s}bea_all.csv'.format(s=os.sep), index_col=None)

    # add goods category
    i = list(bea_map[bea_map['category'] == 'goods'].to)
    gdxout.add_set(gamssetname='i', toset=i, desc='BEA Goods and sectors categories')

    # add value added category
    va = list(bea_map[bea_map['category'] == 'valueadded'].to)
    gdxout.add_set(gamssetname='va', toset=va, desc='BEA Value added categories')

    # add final demand category
    fd = list(bea_map[bea_map['category'] == 'finaldemand'].to)
    gdxout.add_set(gamssetname='fd', toset=fd, desc='BEA Final demand categories')

    # add taxes and subsidies category
    ts = list(bea_map[bea_map['category'] == 'taxessubsidies'].to)
    gdxout.add_set(gamssetname='ts', toset=ts, desc='BEA Taxes and subsidies categories')


def add_bea_detailed_categories_to_gdx():
    # category mapping
    bea_map = pd.read_csv('.{s}core_maps{s}bea_all_detailed.csv'.format(s=os.sep), index_col=None)

    # add goods category
    i = list(bea_map[bea_map['category'] == 'goods'].to)
    gdxout.add_set(gamssetname='i_det', toset=i,
                   desc='Detailed BEA Goods and sectors categories (2007 and 2012 only)')

    # # add value added category
    # va = list(bea_map[bea_map['category'] == 'valueadded'].to)
    # gdxout.add_set(gamssetname='va_det', toset=va, desc='Detailed BEA Value added categories (2007 and 2012 only)')
    #
    # # add final demand category
    # fd = list(bea_map[bea_map['category'] == 'finaldemand'].to)
    # gdxout.add_set(gamssetname='fd_det', toset=fd, desc='Detailed BEA Final demand categories (2007 and 2012 only)')

    # add final demand category
    sector_map = list(zip(bea_map[bea_map['category'] == 'goods'].to,
                          bea_map[bea_map['category'] == 'goods'].aggr_sector))
    gdxout.add_set(gamssetname='sector_map', toset=sector_map,
                   desc='Mapping between detailed and aggregated BEA sectors')


def gams_gsp(from_sql):
    gsp = scrub_gsp.scrub_gsp(from_sql=from_sql)

    # drop all zero values
    gsp.drop(gsp[gsp['value'] == 0].index, inplace=True)

    # category mapping
    gsp_map = pd.read_csv('.{s}core_maps{s}gams{s}map_gsp.csv'.format(s=os.sep), index_col=None)
    gsp_map = dict(zip(gsp_map['from'], gsp_map['to']))

    gsp['ComponentName'] = gsp['ComponentName'].map(gsp_map)

    # change some units
    gsp.loc[gsp[gsp['Unit'] == 'thousands of us dollars (USD)'].index,
            'value'] = gsp[gsp['Unit'] == 'thousands of us dollars (USD)'].value * 1e-3

    gsp.loc[gsp[gsp['Unit'] ==
                'thousands of us dollars (USD)'].index, 'Unit'] = 'millions of us dollars (USD)'

    # organize columns a bit
    gsp = gsp[['GeoName', 'year', 'ComponentName', 'IndustryId', 'value', 'Unit']]

    return gsp


def gams_pce(from_sql):
    pce = scrub_pce.scrub_pce(from_sql=from_sql)

    # drop all zero values
    pce.drop(pce[pce['value'] == 0].index, inplace=True)

    # drop rows that have 'ComponentName'] == 'Per capita personal consumption expenditures (PCE) by state'
    pce.drop(pce[pce['ComponentName'] ==
                 'Per capita personal consumption expenditures (PCE) by state'].index, inplace=True)

    # category mapping
    pce_map = pd.read_csv('.{s}core_maps{s}gams{s}map_pce.csv'.format(s=os.sep), index_col=None)
    pce_map = dict(zip(pce_map['from'], pce_map['to']))

    pce['Description'] = pce['Description'].map(pce_map)

    # organize columns a bit
    pce = pce[['GeoName', 'Description', 'year', 'value', 'units']]

    return pce


def gams_sgf(from_sql):
    sgf = scrub_sgf.scrub_sgf(from_sql=from_sql)

    # drop all zero values
    sgf.drop(sgf[sgf['value'] == 0].index, inplace=True)

    # category mapping
    sgf_map = pd.read_csv('.{s}core_maps{s}gams{s}map_sgf.csv'.format(s=os.sep), index_col=None)
    sgf_map = dict(zip(sgf_map['from'], sgf_map['to']))

    sgf['label'] = sgf['label'].map(sgf_map)

    # change some units
    sgf.loc[sgf[sgf['units'] == 'thousands of us dollars (USD)'].index,
            'value'] = sgf[sgf['units'] == 'thousands of us dollars (USD)'].value * 1e-3

    sgf.loc[sgf[sgf['units'] ==
                'thousands of us dollars (USD)'].index, 'units'] = 'millions of us dollars (USD)'

    # organize columns a bit
    sgf = sgf[['year', 'geographic_region', 'label', 'value', 'units']]

    return sgf


def gams_cfs(from_sql):
    cfs, apdx1, apdx2, apdx3, apdx4 = scrub_cfs.scrub_cfs(from_sql=from_sql)

    # drop unnecessary columns
    cfs = cfs[['ORIG_STATE', 'ORIG_MA', 'DEST_STATE', 'DEST_MA', 'NAICS',
               'SCTG', 'SHIPMT_VALUE', 'SHIPMT_VALUE_units', 'WGT_FACTOR']]

    # calculate total value
    cfs['TOTAL_VALUE'] = cfs.WGT_FACTOR * cfs.SHIPMT_VALUE * 1e-6

    # add in units
    cfs['units'] = 'millions of us dollars (USD)'

    # pivot data
    cfs_st = cfs.pivot_table(index=['ORIG_STATE', 'DEST_STATE', 'NAICS', 'SCTG'], values=[
        'TOTAL_VALUE'], aggfunc=sum)
    cfs_st['units'] = 'millions of us dollars (USD)'
    cfs_st.reset_index(inplace=True)

    cfs_ma = cfs.pivot_table(index=['ORIG_MA', 'DEST_MA', 'NAICS', 'SCTG'], values=[
        'TOTAL_VALUE'], aggfunc=sum)
    cfs_ma['units'] = 'millions of us dollars (USD)'
    cfs_ma.reset_index(inplace=True)

    return cfs, cfs_st, cfs_ma


def gams_usatrade(from_sql):
    usatrade = scrub_usatrade.scrub_usatrade(from_sql=from_sql)

    # drop all zero values
    usatrade.drop(usatrade[usatrade['value'] == 0].index, inplace=True)

    # change units
    usatrade['value'] = usatrade['value'] * 1e-6
    usatrade['units'] = 'millions of us dollars (USD)'

    # organize columns a bit
    usatrade = usatrade[['region', 'NAICS', 'year', 'flow', 'value', 'units']]

    return usatrade


def gams_nass(from_sql):
    nass = scrub_nass.scrub_nass(from_sql=from_sql)

    # drop all zero values
    nass.drop(nass[nass['value'] == 0].index, inplace=True)

    # change units
    nass['value'] = nass['value'] * 1e-6
    nass['units'] = 'millions of us dollars (USD)'

    # organize columns a bit
    nass = nass[['region', 'naics', 'value', 'units']]

    return nass


def gams_emissions(from_sql):
    emissions = scrub_emissions.scrub_emissions(from_sql=from_sql)

    # drop all zero values
    emissions.drop(emissions[emissions['emissions'] == 0].index, inplace=True)

    # category mapping
    emissions_map = pd.read_csv(
        '.{s}core_maps{s}gams{s}map_emissions.csv'.format(s=os.sep), index_col=None)
    emissions_map = dict(zip(emissions_map['from'], emissions_map['to']))

    emissions['source'] = emissions['source'].map(emissions_map)

    # organize columns a bit
    emissions = emissions[['source', 'region', 'year', 'emissions', 'units']]

    return emissions


def gams_crude_oil(from_sql):
    crude_oil = scrub_eia_crude_oil.scrub_eia_crude_oil(from_sql=from_sql)

    # drop all zero values
    crude_oil.drop(crude_oil[crude_oil['price'] == 0].index, inplace=True)

    # organize columns a bit
    crude_oil = crude_oil[['year', 'price', 'units']]

    return crude_oil


def gams_seds(from_sql):
    seds = scrub_seds.scrub_seds(from_sql=from_sql)

    # drop all zero values
    seds.drop(seds[seds['value'] == 0].index, inplace=True)

    # organize columns a bit
    seds = seds[['source', 'sector', 'region', 'year', 'value', 'units']]

    return seds


def gams_gen_heatrate(from_sql):
    heatrate = scrub_heatrate.scrub_eia_gen_heatrate(from_sql=from_sql)

    heatrate = pd.melt(heatrate, id_vars=['year'], value_vars=[
        'coal', 'petroleum', 'nat_gas', 'nuclear'], var_name='source', value_name='value')
    heatrate['units'] = 'btu per kWh generated'

    # category mapping
    seds_energy_tech_map = pd.read_csv(
        '.{s}core_maps{s}gams{s}map_seds_energy_tech.csv'.format(s=os.sep), index_col=None)
    seds_energy_tech_map = dict(zip(seds_energy_tech_map['from'], seds_energy_tech_map['to']))

    heatrate['source'] = heatrate['source'].map(seds_energy_tech_map)

    # drop all zero values
    heatrate.drop(heatrate[heatrate['value'] == 0].index, inplace=True)

    # organize columns a bit
    heatrate = heatrate[['year', 'source', 'value', 'units']]

    return heatrate


def add_regions_to_gdx():
    regions = pd.read_csv('.{s}core_maps{s}gams{s}set_regions.csv'.format(s=os.sep), index_col=None)
    regions = list(regions.keys())

    gdxout.add_set(gamssetname='r', toset=regions, desc='Regions (States) in WiNDC Database')

    regions.append('us')
    super_regions = regions

    gdxout.add_set(gamssetname='sr', toset=super_regions,
                   desc='Super Set of Regions (States + US) in WiNDC Database')


def add_years_to_gdx():
    years = pd.read_csv('.{s}core_maps{s}gams{s}set_years.csv'.format(s=os.sep), index_col=None)
    years = list(years.keys())
    gdxout.add_set(gamssetname='yr', toset=years, desc='Years in WiNDC Database')


def add_seds_energy_tech_to_gdx():
    et = pd.read_csv('.{s}core_maps{s}gams{s}map_seds_energy_tech.csv'.format(
        s=os.sep), index_col=None)
    et = list(et['to'].unique())
    gdxout.add_set(gamssetname='seds_src', toset=et, desc='Energy Technologies in EIA SEDS Data')


def add_bea_use_to_gdx(from_sql):
    use = gams_bea_use(from_sql=from_sql)

    # parameters
    tups = list(tuple(zip(use['year'], use['input_code'], use['output_code'], use['units'])))
    d = dict(zip(tups, use['value']))

    gdxout.add_parameter(gamsparametername='use_units', toparameter=d,
                         desc='Mapped annual use tables, with units as domain')


def add_bea_use_detailed_to_gdx(from_sql):
    use = gams_bea_use_detailed(from_sql=from_sql)

    # parameters
    tups = list(tuple(zip(use['year'], use['from_industry'], use['to_industry'], use['units'])))
    d = dict(zip(tups, use['value']))

    gdxout.add_parameter(gamsparametername='use_det_units', toparameter=d,
                         desc='Mapped DETAILED use tables, with units as domain (2007 and 2012 only)')


def add_bea_supply_detailed_to_gdx(from_sql):
    supply = gams_bea_supply_detailed(from_sql=from_sql)

    # parameters
    tups = list(tuple(zip(supply['year'], supply['from_industry'],
                          supply['to_industry'], supply['units'])))
    d = dict(zip(tups, supply['value']))

    gdxout.add_parameter(gamsparametername='supply_det_units', toparameter=d,
                         desc='Mapped DETAILED supply tables, with units as domain (2007 and 2012 only)')


def add_bea_supply_to_gdx(from_sql):
    supply = gams_bea_supply(from_sql=from_sql)

    # parameters
    tups = list(tuple(zip(supply['year'], supply['input_code'],
                          supply['output_code'], supply['units'])))
    d = dict(zip(tups, supply['value']))

    gdxout.add_parameter(gamsparametername='supply_units', toparameter=d,
                         desc='Mapped annual supply tables, with units as domain')


def add_pce_to_gdx(from_sql):
    pce = gams_pce(from_sql=from_sql)

    # parameters
    tups = list(tuple(zip(pce['year'], pce['GeoName'], pce['Description'], pce['units'])))
    d = dict(zip(tups, pce['value']))

    gdxout.add_parameter(gamsparametername='pce_units', toparameter=d,
                         desc='Personal consumer expenditure by commodity (including aggregate subtotals, with units as domain')


def add_sgf_to_gdx(from_sql):
    sgf = gams_sgf(from_sql=from_sql)

    # parameters
    tups = list(tuple(zip(sgf['year'], sgf['geographic_region'], sgf['label'], sgf['units'])))
    d = dict(zip(tups, sgf['value']))

    gdxout.add_parameter(gamsparametername='sgf_units', toparameter=d,
                         desc='State government finances (SGF), with units as domain')


def add_cfs_to_gdx(from_sql):
    cfs, cfs_st, cfs_ma = gams_cfs(from_sql=from_sql)

    # parameters
    tups = list(tuple(zip(cfs_st['ORIG_STATE'], cfs_st['DEST_STATE'],
                          cfs_st['NAICS'], cfs_st['SCTG'], cfs_st['units'])))
    d = dict(zip(tups, cfs_st['TOTAL_VALUE']))

    gdxout.add_parameter(gamsparametername='cfsdata_st_units', toparameter=d,
                         desc='CFS - State level shipments (value), with units as domain')

    tups = list(tuple(zip(cfs_ma['ORIG_MA'], cfs_ma['DEST_MA'],
                          cfs_ma['NAICS'], cfs_ma['SCTG'], cfs_ma['units'])))
    d = dict(zip(tups, cfs_ma['TOTAL_VALUE']))

    gdxout.add_parameter(gamsparametername='cfsdata_ma_units', toparameter=d,
                         desc='CFS - Metro area level shipments (value), with units as domain')


def add_usatrade_to_gdx(from_sql):
    usatrade = gams_usatrade(from_sql=from_sql)

    # parameters
    tups = list(tuple(zip(usatrade['region'], usatrade['NAICS'],
                          usatrade['year'], usatrade['flow'], usatrade['units'])))
    d = dict(zip(tups, usatrade['value']))

    gdxout.add_parameter(gamsparametername='usatrd_units', toparameter=d,
                         desc='USA trade data, with units as domain')


def add_gsp_to_gdx(from_sql):
    gsp = gams_gsp(from_sql=from_sql)

    # parameters
    tups = list(tuple(zip(gsp['GeoName'], gsp['year'],
                          gsp['ComponentName'], gsp['IndustryId'], gsp['Unit'])))
    d = dict(zip(tups, gsp['value']))

    gdxout.add_parameter(gamsparametername='gsp_units', toparameter=d,
                         desc='Mapped state level annual GDP, with units as domain')


def add_emissions_to_gdx(from_sql):
    emissions = gams_emissions(from_sql=from_sql)

    # parameters
    tups = list(tuple(zip(emissions['source'], emissions['region'],
                          emissions['year'], emissions['units'])))

    d = dict(zip(tups, emissions['emissions']))

    gdxout.add_parameter(gamsparametername='emissions_units', toparameter=d,
                         desc='CO2 emissions by fuel and sector, with units as domain')


def add_gen_heatrate_to_gdx(from_sql):
    heatrate = gams_gen_heatrate(from_sql=from_sql)

    # parameters
    tups = list(tuple(zip(heatrate['year'], heatrate['source'], heatrate['units'])))

    d = dict(zip(tups, heatrate['value']))

    gdxout.add_parameter(gamsparametername='heatrate_units', toparameter=d,
                         desc='Electricity generator (avg across tech) heat rate by fuel, with units as domain')

    return heatrate


def add_seds_to_gdx(from_sql):
    seds = gams_seds(from_sql=from_sql)

    # parameters
    tups = list(tuple(zip(seds['source'], seds['sector'],
                          seds['region'], seds['year'], seds['units'])))

    d = dict(zip(tups, seds['value']))

    gdxout.add_parameter(gamsparametername='seds_units', toparameter=d,
                         desc='Complete EIA SEDS data, with units as domain')


def add_crude_oil_to_gdx(from_sql):
    crude_oil = gams_crude_oil(from_sql=from_sql)

    # parameters
    tups = list(tuple(zip(crude_oil['year'], crude_oil['units'])))

    d = dict(zip(tups, crude_oil['price']))

    gdxout.add_parameter(gamsparametername='crude_oil_price_units', toparameter=d,
                         desc='Crude oil composite acquisition cost by refiners, with units as domain')


def add_fuel_carbon_content_to_gdx():
    # parameters
    d = {('col', 'kilograms CO2 per million btu'): 95, ('gas', 'kilograms CO2 per million btu'): 53,
         ('oil', 'kilograms CO2 per million btu'): 70, ('cru', 'kilograms CO2 per million btu'): 70}

    gdxout.add_parameter(gamsparametername='co2perbtu_units', toparameter=d,
                         desc='Carbon dioxide -- not CO2e -- content, with units as domain')


def add_nass_to_gdx(from_sql):
    nass = gams_nass(from_sql=from_sql)

    # parameters
    tups = list(tuple(zip(nass['region'], nass['naics'], nass['units'])))

    d = dict(zip(tups, nass['value']))

    gdxout.add_parameter(gamsparametername='nass_units', toparameter=d,
                         desc='USDA NASS Ag Census 2012 Sales, with units as domain')

    return nass


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--no-sql', dest='sql', action='store_false')
    parser.set_defaults(sql=True)

    args = parser.parse_args()

    # check gams set/map definitions
    check_gams_maps()
    check_gams_sets()

    # instantiate gdx container
    gdxout = gt.gdxrw.gdx_writer('windc_base.gdx')

    # add data to gdx container
    add_regions_to_gdx()
    add_years_to_gdx()

    # BEA Data to GDX
    add_bea_use_to_gdx(from_sql=args.sql)
    add_bea_supply_to_gdx(from_sql=args.sql)
    add_bea_categories_to_gdx()

    add_bea_use_detailed_to_gdx(from_sql=args.sql)
    add_bea_supply_detailed_to_gdx(from_sql=args.sql)
    add_bea_detailed_categories_to_gdx()

    # GSP data to GDX
    add_gsp_to_gdx(from_sql=args.sql)

    # PCE data to GDX
    add_pce_to_gdx(from_sql=args.sql)

    # SGF data to GDX
    add_sgf_to_gdx(from_sql=args.sql)

    # CFS data to GDX
    add_cfs_to_gdx(from_sql=args.sql)

    # USA trade data to GDX
    add_usatrade_to_gdx(from_sql=args.sql)

    # EIA Data (for blueNOTE)
    add_emissions_to_gdx(from_sql=args.sql)
    add_gen_heatrate_to_gdx(from_sql=args.sql)
    add_seds_energy_tech_to_gdx()
    add_seds_to_gdx(from_sql=args.sql)
    add_crude_oil_to_gdx(from_sql=args.sql)
    add_fuel_carbon_content_to_gdx()

    # USDA NASS
    add_nass_to_gdx(from_sql=args.sql)

    # export gdx
    gdxout.export_gdx()
