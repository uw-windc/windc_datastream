from pandas import read_csv, read_sql
from sql_engines import windc_engine
from sqlalchemy import text
import parse_bea_supply_io_detailed
import argparse
import os


def scrub_supply(from_sql):
    if from_sql == True:
        print('Sourcing Detailed BEA supply data from SQL database...')
        # query sql database
        sql = 'select * from bea_supply_detailed;'
        supply = read_sql(sql, con=windc_engine, index_col='index')
    else:
        print('Sourcing Detailed BEA supply data from raw files...')
        supply = parse_bea_supply_io_detailed.file_parser()

    # drop years if necessary
    years = read_csv('.{s}core_maps{s}gams{s}set_years.csv'.format(s=os.sep), index_col=None)
    years = list(years.keys())
    supply['test'] = supply['year'].isin(years)
    supply.drop(supply[supply['test'] == False].index, inplace=True)
    supply.drop(columns='test', inplace=True)

    return supply


def scrub_supply_to_csv(from_sql):
    s = scrub_supply(from_sql=from_sql)
    s.to_csv('bea_supply_io_detailed_scrubed.csv')


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--csv-out', dest='csv', action='store_true')
    parser.add_argument('--no-sql', dest='sql', action='store_false')
    parser.set_defaults(csv=False)
    parser.set_defaults(sql=True)

    args = parser.parse_args()

    if args.csv == True:
        scrub_supply_to_csv(from_sql=args.sql)
