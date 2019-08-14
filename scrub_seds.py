from pandas import read_csv, read_sql
from sql_engines import windc_engine
from sqlalchemy import text
import parse_seds
import argparse
import os


def scrub_seds(from_sql):

    if from_sql == True:
        print('Sourcing EIA State Energy Data System (SEDS) data from SQL database...')
        # query sql database
        sql = 'select * from seds;'
        seds = read_sql(sql, con=windc_engine, index_col='index')
    else:
        print('Sourcing EIA State Energy Data System (SEDS) data from raw files...')
        seds = parse_seds.parse_file()

    # drop data that is not in set yr
    years = read_csv('.{s}core_maps{s}gams{s}set_years.csv'.format(s=os.sep), index_col=None)
    years = list(years.keys())
    seds['test'] = seds['year'].isin(years)
    seds.drop(seds[seds['test'] == False].index, inplace=True)

    # drop data that is not in set r (states)
    states = read_csv('.{s}core_maps{s}gams{s}set_regions.csv'.format(s=os.sep), index_col=None)
    states = list(states.keys())
    # add in 'us' region to account for ROW imports/exports of certain energy products
    states.append('us')
    seds['test'] = seds['region'].isin(states)
    seds.drop(seds[seds['test'] == False].index, inplace=True)
    seds.drop(columns='test', inplace=True)

    return seds


def scrub_seds_to_csv(from_sql):
    s = scrub_seds(from_sql=from_sql)
    s.to_csv('seds_all_scrubed.csv')


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--csv-out', dest='csv', action='store_true')
    parser.add_argument('--no-sql', dest='sql', action='store_false')
    parser.set_defaults(csv=False)
    parser.set_defaults(sql=True)

    args = parser.parse_args()

    if args.csv == True:
        scrub_seds_to_csv(from_sql=args.sql)
