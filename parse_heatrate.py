import pandas as pd
from sql_engines import windc_engine as engine
from sqlalchemy import INTEGER, TEXT, text
import argparse
import os


def parse_file():
    path = '.{s}datasources{s}SEDS{s}'.format(s=os.sep)
    filename = 'generator_heat_rates.csv'

    a = pd.read_csv(path+filename, index_col=None)

    # add in units
    a['units'] = 'btu per kWh generated'

    return a


def parse_file_to_csv():
    s = parse_file()
    s.to_csv('eia_heatrate_parsed.csv')


def parse_file_to_sql():
    df = parse_file()

    types = {'year': INTEGER,
             'coal': INTEGER,
             'petroleum': INTEGER,
             'nat_gas': INTEGER,
             'nuclear': INTEGER,
             'units': TEXT}
    df.to_sql('eia_gen_heatrate', con=engine, if_exists='replace', dtype=types)

    tbl_desc = 'EIA Average Tested Heat Rates -- https://www.eia.gov/electricity/annual/html/epa_08_02.html'
    comment = text("COMMENT ON TABLE eia_gen_heatrate IS '{}';".format(
        tbl_desc)).execution_options(autocommit=True)
    engine.execute(comment)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--csv-out', dest='csv', action='store_true')
    parser.set_defaults(csv=False)

    args = parser.parse_args()

    if args.csv == False:
        parse_file_to_sql()
    elif args.csv == True:
        parse_file_to_csv()
