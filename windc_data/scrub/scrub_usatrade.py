from pandas import read_csv, read_sql
from sql_engines import windc_engine
from sqlalchemy import text
import join_usatrade
import argparse
import os


def scrub_usatrade(from_sql):

    if from_sql == True:
        print('Sourcing USA Trade data from SQL database...')
        # query sql database
        sql = 'select * from usatrade;'
        usatrade = read_sql(sql, con=windc_engine, index_col='index')
    else:
        print('Sourcing USA Trade data from raw files...')
        usatrade = join_usatrade.join_all_usatrade()

    # drop data that is not in set yr
    years = read_csv('.{s}core_maps{s}gams{s}set_years.csv'.format(s=os.sep), index_col=None)
    years = list(years.keys())
    usatrade['test'] = usatrade['year'].isin(years)
    usatrade.drop(usatrade[usatrade['test'] == False].index, inplace=True)

    # drop data that is not in set r (states)
    states = read_csv('.{s}core_maps{s}gams{s}set_regions.csv'.format(s=os.sep), index_col=None)
    states = list(states.keys())
    usatrade['test'] = usatrade['region'].isin(states)
    usatrade.drop(usatrade[usatrade['test'] == False].index, inplace=True)

    # keep only 'World Totals'
    usatrade['test'] = usatrade['Country'].isin(['world total'])
    usatrade.drop(usatrade[usatrade['test'] == False].index, inplace=True)

    # drop test column
    usatrade.drop(columns='test', inplace=True)

    return usatrade


def scrub_usatrade_to_csv(from_sql):
    s = scrub_usatrade(from_sql=from_sql)
    s.to_csv('usatrade_all_scrubed.csv')


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--csv-out', dest='csv', action='store_true')
    parser.add_argument('--no-sql', dest='sql', action='store_false')
    parser.set_defaults(csv=False)
    parser.set_defaults(sql=True)

    args = parser.parse_args()

    if args.csv == True:
        scrub_usatrade_to_csv(from_sql=args.sql)
