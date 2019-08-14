from pandas import read_csv, read_sql
from sql_engines import windc_engine
from sqlalchemy import text
import join_pce
import argparse
import os


def scrub_pce(from_sql):

    if from_sql == True:
        print('Sourcing Personal Consumption Expenditures (PCE) data from SQL database...')
        # query sql database
        sql = 'select * from pce;'
        pce = read_sql(sql, con=windc_engine, index_col='index')
    else:
        print('Sourcing Personal Consumption Expenditures (PCE) data from raw files...')
        pce = join_pce.join_all_pce()

    # drop data that is not in set yr
    years = read_csv('.{s}core_maps{s}gams{s}set_years.csv'.format(s=os.sep), index_col=None)
    years = list(years.keys())
    pce['test'] = pce['year'].isin(years)
    pce.drop(pce[pce['test'] == False].index, inplace=True)

    # drop data that is not in set r (states)
    states = read_csv('.{s}core_maps{s}gams{s}set_regions.csv'.format(s=os.sep), index_col=None)
    states = list(states.keys())
    pce['test'] = pce['GeoName'].isin(states)
    pce.drop(pce[pce['test'] == False].index, inplace=True)

    # drop test column
    pce.drop(columns='test', inplace=True)

    return pce


def scrub_pce_to_csv(from_sql):
    s = scrub_pce(from_sql=from_sql)
    s.to_csv('pce_all_scrubed.csv')


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--csv-out', dest='csv', action='store_true')
    parser.add_argument('--no-sql', dest='sql', action='store_false')
    parser.set_defaults(csv=False)
    parser.set_defaults(sql=True)

    args = parser.parse_args()

    if args.csv == True:
        scrub_pce_to_csv(from_sql=args.sql)
