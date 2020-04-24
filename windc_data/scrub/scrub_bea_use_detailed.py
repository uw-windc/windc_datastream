from pandas import read_csv, read_sql
from sql_engines import windc_engine
from sqlalchemy import text
import parse_bea_use_io_detailed
import argparse
import os


def scrub_use(from_sql):
    if from_sql == True:
        print('Sourcing Detailed BEA use data from SQL database...')
        # query sql database
        sql = 'select * from bea_use_detailed;'
        use = read_sql(sql, con=windc_engine, index_col='index')
    else:
        print('Sourcing Detailed BEA use data from raw files...')
        use = parse_bea_use_io_detailed.file_parser()

    # drop years if necessary
    years = read_csv('.{s}core_maps{s}gams{s}set_years.csv'.format(s=os.sep), index_col=None)
    years = list(years.keys())
    use['test'] = use['year'].isin(years)
    use.drop(use[use['test'] == False].index, inplace=True)
    use.drop(columns='test', inplace=True)

    return use


def scrub_use_to_csv(from_sql):
    s = scrub_use(from_sql=from_sql)
    s.to_csv('bea_use_io_detailed_scrubed.csv')


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--csv-out', dest='csv', action='store_true')
    parser.add_argument('--no-sql', dest='sql', action='store_false')
    parser.set_defaults(csv=False)
    parser.set_defaults(sql=True)

    args = parser.parse_args()

    if args.csv == True:
        scrub_use_to_csv(from_sql=args.sql)
