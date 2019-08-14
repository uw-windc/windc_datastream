from pandas import DataFrame, to_numeric
import parse_pce
from sql_engines import windc_engine as engine
from sqlalchemy import INTEGER, TEXT, FLOAT, text
import argparse


def join_all_pce():
    # create empty dataframe container
    df = DataFrame(columns=['GeoFIPS', 'GeoName', 'Region', 'TableName',
                            'ComponentName', 'units', 'Line', 'Description', 'year', 'value'])

    t = ['SAEXP1_1997_2017_ALL_AREAS_.csv',
         'SAEXP2_1997_2017_ALL_AREAS_.csv']

    for i in t:
        df_p = parse_pce.parse_file(filename=i)
        df = df.append(df_p, sort=False, ignore_index=True)

    return df


def join_all_pce_to_csv():
    s = join_all_pce()
    s.to_csv('pce_all.csv')


def join_all_pce_to_sql():
    # personal consumption expenditures
    df = join_all_pce()
    df['Line'] = to_numeric(df['Line'], errors='coerce')
    df['Line'] = df['Line'].map(int)
    df['year'] = df['year'].map(int)

    types = {'GeoFIPS': TEXT,
             'GeoName': TEXT,
             'Region': TEXT,
             'TableName': TEXT,
             'ComponentName': TEXT,
             'units': TEXT,
             'Line': INTEGER,
             'Description': TEXT,
             'year': INTEGER,
             'value': FLOAT}
    df.to_sql('pce', con=engine, if_exists='replace', dtype=types)

    tbl_desc = 'BEA Personal Consumer Expenditures -- https://apps.bea.gov/itable/iTable.cfm?ReqID=70&step=1'
    comment = text("COMMENT ON TABLE pce IS '{}';".format(
        tbl_desc)).execution_options(autocommit=True)
    engine.execute(comment)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--csv-out', dest='csv', action='store_true')
    parser.set_defaults(csv=False)

    args = parser.parse_args()

    if args.csv == False:
        join_all_pce_to_sql()
    elif args.csv == True:
        join_all_pce_to_csv()
