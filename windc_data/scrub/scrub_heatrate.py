import pandas as pd
from sql_engines import windc_engine
from sqlalchemy import text
import parse_heatrate
import argparse
import os


def scrub_eia_gen_heatrate(from_sql):

    if from_sql == True:
        print('Sourcing EIA Generator Heatrate data from SQL database...')
        # query sql database
        sql = 'select * from eia_gen_heatrate;'
        eia_gen_heatrate = pd.read_sql(sql, con=windc_engine, index_col='index')
    else:
        print('Sourcing EIA Generator Heatrate data from raw files...')
        eia_gen_heatrate = parse_heatrate.parse_file()

    # need to fill in data for other years that do not have EIA heatrates
    years = pd.read_csv('.{s}core_maps{s}gams{s}set_years.csv'.format(s=os.sep), index_col=None)
    years = list(years.keys())

    eia2 = pd.DataFrame(data={'year': [1997, 1998, 1999, 2000, 2001, 2002, 2003, 2004]}, columns=[
                        'year', 'coal', 'petroleum', 'nat_gas', 'nuclear', 'units'])

    # fill in years before 2005 with heat rates at 2005 values
    eia2['coal'] = int(
        eia_gen_heatrate.loc[eia_gen_heatrate[eia_gen_heatrate['year'] == 2005].index, 'coal'])
    eia2['petroleum'] = int(
        eia_gen_heatrate.loc[eia_gen_heatrate[eia_gen_heatrate['year'] == 2005].index, 'petroleum'])
    eia2['nat_gas'] = int(
        eia_gen_heatrate.loc[eia_gen_heatrate[eia_gen_heatrate['year'] == 2005].index, 'nat_gas'])
    eia2['nuclear'] = int(
        eia_gen_heatrate.loc[eia_gen_heatrate[eia_gen_heatrate['year'] == 2005].index, 'nuclear'])
    eia2['units'] = list(eia_gen_heatrate['units'].unique())[0]

    e = eia2.append(eia_gen_heatrate, ignore_index=True)

    return e


def scrub_eia_gen_heatrate_to_csv(from_sql):
    s = scrub_eia_gen_heatrate(from_sql=from_sql)
    s.to_csv('eia_gen_heatrate_all_scrubed.csv')


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--csv-out', dest='csv', action='store_true')
    parser.add_argument('--no-sql', dest='sql', action='store_false')
    parser.set_defaults(csv=False)
    parser.set_defaults(sql=True)

    args = parser.parse_args()

    if args.csv == True:
        scrub_eia_gen_heatrate_to_csv(from_sql=args.sql)
