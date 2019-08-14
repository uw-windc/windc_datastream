from pandas import read_csv, read_sql
from sql_engines import windc_engine
from sqlalchemy import text
import parse_nass
import argparse
import os


def scrub_nass(from_sql):

    if from_sql == True:
        print('Sourcing NASS data from SQL database...')
        # query sql database
        sql = 'select * from nass;'
        nass = read_sql(sql, con=windc_engine, index_col='index')
    else:
        print('Sourcing NASS data from raw files...')
        nass = parse_nass.parse_file()

    # drop data that is not in set yr
    years = read_csv('.{s}core_maps{s}gams{s}set_years.csv'.format(s=os.sep), index_col=None)
    years = list(years.keys())
    nass['test'] = nass['year'].isin(years)
    nass.drop(nass[nass['test'] == False].index, inplace=True)

    # drop data that is not in set r (states)
    states = read_csv('.{s}core_maps{s}gams{s}set_regions.csv'.format(s=os.sep), index_col=None)
    states = list(states.keys())
    nass['test'] = nass['region'].isin(states)
    nass.drop(nass[nass['test'] == False].index, inplace=True)

    # drop data that is not in the set nass (specific naics categories)
    naics = read_csv('.{s}core_maps{s}gams{s}set_nass.csv'.format(s=os.sep), index_col=None)
    naics = list(naics.keys())
    nass['test'] = nass['naics'].isin(naics)
    nass.drop(nass[nass['test'] == False].index, inplace=True)

    # reset index and drop test column
    nass.reset_index(drop=True, inplace=True)
    nass.drop(columns='test', inplace=True)

    return nass


def scrub_nass_to_csv(from_sql):
    s = scrub_nass(from_sql=from_sql)
    s.to_csv('nass_all_scrubed.csv')


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--csv-out', dest='csv', action='store_true')
    parser.add_argument('--no-sql', dest='sql', action='store_false')
    parser.set_defaults(csv=False)
    parser.set_defaults(sql=True)

    args = parser.parse_args()

    if args.csv == True:
        scrub_nass_to_csv(from_sql=args.sql)
