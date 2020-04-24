from pandas import read_csv, read_sql
from sql_engines import windc_engine
from sqlalchemy import text
import parse_crude_oil
import argparse
import os


def scrub_eia_crude_oil(from_sql):
    if from_sql == True:
        print('Sourcing EIA Crude Oil data from SQL database...')
        # query sql database
        sql = 'select * from eia_crude_oil;'
        eia_crude_oil = read_sql(sql, con=windc_engine, index_col='index')
    else:
        print('Sourcing EIA Crude Oil data from raw files...')
        eia_crude_oil = parse_crude_oil.parse_file()

    # drop data that is not in set yr
    years = read_csv('.{s}core_maps{s}gams{s}set_years.csv'.format(s=os.sep), index_col=None)
    years = list(years.keys())
    eia_crude_oil['test'] = eia_crude_oil['year'].isin(years)
    eia_crude_oil.drop(eia_crude_oil[eia_crude_oil['test'] == False].index, inplace=True)

    # reset index and drop test column
    eia_crude_oil.reset_index(drop=True, inplace=True)
    eia_crude_oil.drop(columns='test', inplace=True)

    return eia_crude_oil


def scrub_eia_crude_oil_to_csv(from_sql):
    s = scrub_eia_crude_oil(from_sql=from_sql)
    s.to_csv('eia_crude_oil_all_scrubed.csv')


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--csv-out', dest='csv', action='store_true')
    parser.add_argument('--no-sql', dest='sql', action='store_false')
    parser.set_defaults(csv=False)
    parser.set_defaults(sql=True)

    args = parser.parse_args()

    if args.csv == True:
        scrub_eia_crude_oil_to_csv(from_sql=args.sql)
