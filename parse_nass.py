import pandas as pd
from sql_engines import windc_engine as engine
from sqlalchemy import INTEGER, TEXT, FLOAT, text
import argparse
import os


def parse_file():
    path = '.{s}datasources{s}NASS{s}'.format(s=os.sep)
    file = 'agcensus_2012_sales_redownload.csv'

    a = pd.read_csv(path+file, index_col=None)

    region_map = pd.read_csv('.{s}core_maps{s}regions.csv'.format(s=os.sep), index_col=None)
    region_map = dict(zip(region_map['from'], region_map['to']))

    # rename columns
    a.rename({'State': 'region', 'Value': 'value', 'Year': 'year',
              'Domain Category': 'naics'}, axis='columns', inplace=True)

    a['value'] = [a.loc[i, 'value'].replace(',', '') for i in a.index]
    a['value'] = pd.to_numeric(a['value'], errors='coerce')

    a['value'].fillna(0, inplace=True)

    a['naics'] = [a.loc[i, 'naics'].split(': (')[1] for i in a.index]
    a['naics'] = [a.loc[i, 'naics'].split(')')[0] for i in a.index]
    a['naics'] = a['naics'].map(int)

    convert_to_zero = ['(D)', '(H)', '(L)']
    a['test'] = a['CV (%)'].isin(convert_to_zero)
    a.loc[a[a['test'] == True].index, 'CV (%)'] = 0
    a.drop(columns='test', inplace=True)

    # drop unused columns (all rows = nan)
    a.dropna(axis=1, how='all', inplace=True)

    a['units'] = 'us dollars (USD)'

    a['region'] = a['region'].map(region_map)

    return a


def parse_file_to_csv():
    s = parse_file()
    s.to_csv('nass_2012.csv')


def parse_file_to_sql():
    # parse NASS Data
    df = parse_file()
    df.rename({'CV (%)': 'CV_pct'}, axis='columns', inplace=True)

    # to sql
    types = {'Program': TEXT,
             'year': INTEGER,
             'Period': TEXT,
             'Geo Level': TEXT,
             'region': TEXT,
             'State ANSI': INTEGER,
             'watershed_code': INTEGER,
             'Commodity': TEXT,
             'Data Item': TEXT,
             'Domain': TEXT,
             'naics': INTEGER,
             'value': FLOAT,
             'CV_pct': TEXT,
             'units': TEXT}

    df.to_sql('nass', con=engine, if_exists='replace', dtype=types)

    tbl_desc = 'USDA NASS -- https://quickstats.nass.usda.gov/#777837D3-E71B-323A-8137-D10646E77104'
    comment = text("COMMENT ON TABLE nass IS '{}';".format(
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
