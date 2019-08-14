from pandas import DataFrame
import parse_emissions
from sql_engines import windc_engine as engine
from sqlalchemy import INTEGER, TEXT, FLOAT, text
import argparse


def join_all_emissions():
    # create empty dataframe container
    df = DataFrame(columns=['source', 'region', 'year', 'emissions', 'units'])

    t = {'coal_CO2_by_state_2013.xlsx': {'data_rng': 'A3:AI55', 'nickname': 'coal'},
         'natural_gas_CO2_by_state_2013.xlsx': {'data_rng': 'A3:AI55', 'nickname': 'natgas'},
         'petroleum_CO2_by_state_2013.xlsx': {'data_rng': 'A3:AI55', 'nickname': 'petrol'},
         'industrial_CO2_by_state_2013.xlsx': {'data_rng': 'A3:AI55', 'nickname': 'ind'},
         'commercial_CO2_by_state_2013.xlsx': {'data_rng': 'A3:AI55', 'nickname': 'com'},
         'residential_CO2_by_state_2013.xlsx': {'data_rng': 'A3:AI55', 'nickname': 'res'},
         'electric_CO2_by_state_2013.xlsx': {'data_rng': 'A3:AI55', 'nickname': 'elec'},
         'transportation_CO2_by_state_2013.xlsx': {'data_rng': 'A3:AI55', 'nickname': 'trans'}}

    for i in t.keys():
        df_p = parse_emissions.parse_file(
            filename=i, data_rng=t[i]['data_rng'], nickname=t[i]['nickname'])
        df_p['source'] = t[i]['nickname']
        df = df.append(df_p, sort=False, ignore_index=True)

    return df


def join_all_emissions_to_csv():
    s = join_all_emissions()
    s.to_csv('emissions_all.csv')


def join_all_emissions_to_sql():
    # parse EIA SEDS emissions
    df = join_all_emissions()

    types = {'source': TEXT,
             'region': TEXT,
             'year': INTEGER,
             'emissions': FLOAT,
             'units': TEXT}
    df.to_sql('emissions', con=engine, if_exists='replace', dtype=types)

    tbl_desc = 'EIA State Energy Data System -- https://www.eia.gov/environment/emissions/state/'
    comment = text("COMMENT ON TABLE emissions IS '{}';".format(
        tbl_desc)).execution_options(autocommit=True)
    engine.execute(comment)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--csv-out', dest='csv', action='store_true')
    parser.set_defaults(csv=False)

    args = parser.parse_args()

    if args.csv == False:
        join_all_emissions_to_sql()
    elif args.csv == True:
        join_all_emissions_to_csv()
