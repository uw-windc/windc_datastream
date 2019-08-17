from pandas import read_csv, read_sql
from sql_engines import windc_engine
from sqlalchemy import text
import join_emissions
import argparse
import os


def scrub_emissions(from_sql):

    if from_sql == True:
        print('Sourcing Emissions data from SQL database...')
        # query sql database
        sql = 'select * from emissions;'
        emissions = read_sql(sql, con=windc_engine, index_col='index')
    else:
        print('Sourcing Emissions data from raw files...')
        emissions = join_emissions.join_all_emissions()

    # drop data that is not in set yr
    years = read_csv('.{s}core_maps{s}gams{s}set_years.csv'.format(s=os.sep), index_col=None)
    years = years.keys().astype(int).to_list()
    emissions['test'] = emissions['year'].isin(years)
    emissions.drop(emissions[emissions['test'] == False].index, inplace=True)

    # drop data that is not in set r (states)
    states = read_csv('.{s}core_maps{s}gams{s}set_regions.csv'.format(s=os.sep), index_col=None)
    states = list(states.keys())
    emissions['test'] = emissions['region'].isin(states)
    emissions.drop(emissions[emissions['test'] == False].index, inplace=True)

    emissions.drop(columns='test', inplace=True)

    return emissions


def scrub_emissions_to_csv(from_sql):
    s = scrub_emissions(from_sql=from_sql)
    s.to_csv('emissions_all_scrubed.csv')


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--csv-out', dest='csv', action='store_true')
    parser.add_argument('--no-sql', dest='sql', action='store_false')
    parser.set_defaults(csv=False)
    parser.set_defaults(sql=True)

    args = parser.parse_args()

    if args.csv == True:
        scrub_emissions_to_csv(from_sql=args.sql)
