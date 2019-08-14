import pandas as pd
from sql_engines import windc_engine as engine
from sqlalchemy import INTEGER, FLOAT, TEXT, text
import argparse
import os


def parse_file():
    path = '.{s}datasources{s}SEDS{s}'.format(s=os.sep)
    filename = 'Complete_SEDS_update.csv'

    a = pd.read_csv(path+filename, index_col=None)

    # add in descriptions
    seds_map = pd.read_csv('.{s}core_maps{s}seds.csv'.format(s=os.sep), index_col=None)
    seds_desc_map = dict(zip(seds_map['from'], seds_map['full_desc']))
    a['full_desc'] = a['MSN'].map(seds_desc_map)

    # add in units
    seds_units_map = dict(zip(seds_map['from'], seds_map['units']))
    seds_units_abbv_map = dict(zip(seds_map['from'], seds_map['units_abbv']))
    a['units'] = a['MSN'].map(seds_units_map)
    a['units_abbv'] = a['MSN'].map(seds_units_abbv_map)

    # split out sources, sector and units from MSN code
    seds_source_map = dict(zip(seds_map['from'], seds_map['source']))
    seds_source_desc_map = dict(zip(seds_map['from'], seds_map['source_desc']))

    seds_sector_map = dict(zip(seds_map['from'], seds_map['sector']))
    seds_sector_desc_map = dict(zip(seds_map['from'], seds_map['sector_desc']))

    a['source'] = a['MSN'].map(seds_source_map)
    a['source_desc'] = a['MSN'].map(seds_source_desc_map)

    a['sector'] = a['MSN'].map(seds_sector_map)
    a['sector_desc'] = a['MSN'].map(seds_sector_desc_map)

    # standardize regions
    region_map = pd.read_csv('.{s}core_maps{s}regions.csv'.format(s=os.sep), index_col=None)
    region_map = dict(zip(region_map['from'], region_map['to']))
    a['StateCode'] = a['StateCode'].map(region_map)

    # rename some columns
    a.rename({'StateCode': 'region', 'Data': 'value', 'Year': 'year'}, axis='columns', inplace=True)

    return a


def parse_file_to_csv():
    s = parse_file()
    s.to_csv('seds_parsed.csv')


def parse_file_to_sql():
    df = parse_file()

    types = {'Data_Status': TEXT,
             'MSN': TEXT,
             'region': TEXT,
             'year': INTEGER,
             'value': FLOAT,
             'desc': TEXT,
             'units': TEXT}
    df.to_sql('seds', con=engine, if_exists='replace', dtype=types)

    tbl_desc = 'EIA State Energy Data System -- https://www.eia.gov/state/seds/seds-data-fuel.php?sid=US#DataFiles'
    comment = text("COMMENT ON TABLE seds IS '{}';".format(
        tbl_desc)).execution_options(autocommit=True)
    engine.execute(comment)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--csv-out', dest='csv', action='store_true')
    parser.set_defaults(csv=False)

    args = parser.parse_args()

    if args.csv == False:
        parse_file_to_sql()
    elif args.csv == True:
        parse_file_to_csv()
