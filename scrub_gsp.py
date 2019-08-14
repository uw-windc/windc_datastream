from pandas import read_csv, read_sql
from sql_engines import windc_engine
from sqlalchemy import text
import parse_state_gsp
import argparse
import os


def scrub_gsp(from_sql):

    if from_sql == True:
        print('Sourcing Gross State Product (GSP) data from SQL database...')
        # query sql database
        sql = 'select * from gsp;'
        gsp = read_sql(sql, con=windc_engine, index_col='index')
    else:
        print('Sourcing Gross State Product (GSP) data from raw files...')
        gsp = parse_state_gsp.file_parser()

    # drop data that is not in set yr
    years = read_csv('.{s}core_maps{s}gams{s}set_years.csv'.format(s=os.sep), index_col=None)
    years = list(years.keys())
    gsp['test'] = gsp['year'].isin(years)
    gsp.drop(gsp[gsp['test'] == False].index, inplace=True)

    # drop data that is not in set r (states)
    states = read_csv('.{s}core_maps{s}gams{s}set_regions.csv'.format(s=os.sep), index_col=None)
    states = list(states.keys())
    gsp['test'] = gsp['GeoName'].isin(states)
    gsp.drop(gsp[gsp['test'] == False].index, inplace=True)

    # drop data that has ComponentName = 'Contributions to percent change in real GDP'
    gsp.drop(gsp[gsp['ComponentName'] ==
                 'Contributions to percent change in real GDP'].index, inplace=True)
    gsp.reset_index(drop=True, inplace=True)
    gsp.drop(columns='test', inplace=True)

    return gsp


def scrub_gsp_to_csv(from_sql):
    s = scrub_gsp(from_sql=from_sql)
    s.to_csv('gsp_all_scrubed.csv')


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--csv-out', dest='csv', action='store_true')
    parser.add_argument('--no-sql', dest='sql', action='store_false')
    parser.set_defaults(csv=False)
    parser.set_defaults(sql=True)

    args = parser.parse_args()

    if args.csv == True:
        scrub_gsp_to_csv(from_sql=args.sql)
