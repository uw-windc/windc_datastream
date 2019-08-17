from pandas import read_csv, read_sql
from sql_engines import windc_engine
from sqlalchemy import text
import join_sgf
import argparse
import os


def scrub_sgf(from_sql):

    if from_sql == True:
        print('Sourcing State Govt Finance (SGF) data from SQL database...')
        # query sql database
        sql = 'select * from sgf;'
        sgf = read_sql(sql, con=windc_engine, index_col='index')
    else:
        print('Sourcing State Govt Finance (SGF) data from raw files...')
        sgf = join_sgf.join_all_sgf()

    # drop some rows
    to_drop = ['Population',
               'General Expenditure, by Function:',
               'Personal income',
               'Total Expenditure - General Expenditure - Intergovernmental General Expenditure',
               'Insurance Trust Expenditure - Unemployment Compensation Systems',
               "Insurance Trust Expenditure - Workers' Compensation Systems",
               'Insurance Trust Expenditure - State-Administered Pension Systems',
               'Insurance Trust Expenditure - Other Insurance Trust Systems']

    sgf['test'] = sgf['label'].isin(to_drop)
    sgf.drop(sgf[sgf['test'] == True].index, inplace=True)

    # BEA sgf mapping
    sgf_map = read_csv('.{s}core_maps{s}gams{s}map_sgf.csv'.format(s=os.sep), index_col=None)
    sgf_map = dict(zip(sgf_map['from'], sgf_map['to']))

    # test for complete mapping
    if sum(sgf['label'].isin(sgf_map.keys())) != len(sgf):
        raise Exception('... incomplete mapping between SGF categories')

    # drop data that is not in set r (states)
    states = read_csv('.{s}core_maps{s}gams{s}set_regions.csv'.format(s=os.sep), index_col=None)
    states = list(states.keys())
    sgf['test'] = sgf['geographic_region'].isin(states)
    sgf.drop(sgf[sgf['test'] == False].index, inplace=True)

    # drop years if necessary
    years = read_csv('.{s}core_maps{s}gams{s}set_years.csv'.format(s=os.sep), index_col=None)
    years = years.keys().astype(int).to_list()
    sgf['test'] = sgf['year'].isin(years)
    sgf.drop(sgf[sgf['test'] == False].index, inplace=True)
    sgf.drop(columns='test', inplace=True)

    return sgf


def scrub_sgf_to_csv(from_sql):
    s = scrub_sgf(from_sql=from_sql)
    s.to_csv('sgf_raw_scrubed.csv')


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--csv-out', dest='csv', action='store_true')
    parser.add_argument('--no-sql', dest='sql', action='store_false')
    parser.set_defaults(csv=False)
    parser.set_defaults(sql=True)

    args = parser.parse_args()

    if args.csv == True:
        scrub_sgf_to_csv(from_sql=args.sql)
