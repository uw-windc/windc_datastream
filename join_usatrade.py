from pandas import DataFrame
import parse_usatrade
from sql_engines import windc_engine as engine
from sqlalchemy import INTEGER, TEXT, FLOAT, text
import argparse


def join_all_usatrade():
    # create empty dataframe container
    df = DataFrame(columns=['flow', 'region', 'Country', 'year',
                            'value', 'NAICS', 'Commodity_desc', 'units'])

    t = {'State Exports by NAICS Commodities.csv': {'nickname': 'exports'},
         'State Imports by NAICS Commodities.csv': {'nickname': 'imports'}}

    for i in t:
        df_p = parse_usatrade.parse_file(filename=i, nickname=t[i]['nickname'])
        df_p['flow'] = t[i]['nickname']
        df = df.append(df_p, sort=False, ignore_index=True)

    return df


def join_all_usatrade_to_csv():
    s = join_all_usatrade()
    s.to_csv('usatrade_all.csv')


def join_all_usatrade_to_sql():
    # parse USA Trade Data
    df = join_all_usatrade()

    # drop annoying partial year data
    df.drop(df[df['year'] == '2017 through November'].index, inplace=True)
    df.reset_index(drop=True, inplace=True)

    # to sql
    types = {'flow': TEXT,
             'region': TEXT,
             'Country': TEXT,
             'year': INTEGER,
             'value': FLOAT,
             'NAICS': INTEGER,
             'Commodity_desc': TEXT,
             'units': TEXT,
             'Commodity': TEXT}

    df.to_sql('usatrade', con=engine, if_exists='replace', dtype=types)

    tbl_desc = 'USA Trade -- https://usatrade.census.gov/'
    comment = text("COMMENT ON TABLE usatrade IS '{}';".format(
        tbl_desc)).execution_options(autocommit=True)
    engine.execute(comment)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--csv-out', dest='csv', action='store_true')
    parser.set_defaults(csv=False)

    args = parser.parse_args()

    if args.csv == False:
        join_all_usatrade_to_sql()
    elif args.csv == True:
        join_all_usatrade_to_csv()
