import pandas as pd
from sql_engines import windc_engine as engine
from sqlalchemy import TEXT, FLOAT, text, INTEGER
import argparse
import os


def file_parser():
    path = '.{s}datasources{s}BEA{s}GDP{s}State{s}'.format(s=os.sep)
    file = ['SAGDP2N__ALL_AREAS_1997_2017.csv',
            'SAGDP3N__ALL_AREAS_1997_2017.csv',
            'SAGDP4N__ALL_AREAS_1997_2017.csv',
            'SAGDP5N__ALL_AREAS_1997_2017.csv',
            'SAGDP6N__ALL_AREAS_1997_2017.csv',
            'SAGDP7N__ALL_AREAS_1997_2017.csv',
            'SAGDP8N__ALL_AREAS_1997_2017.csv',
            'SAGDP9N__ALL_AREAS_1997_2017.csv',
            'SAGDP11N__ALL_AREAS_1998_2017.csv']

    yrs = {'SAGDP2N__ALL_AREAS_1997_2017.csv': [str(i) for i in range(1997, 2017+1)],
           'SAGDP3N__ALL_AREAS_1997_2017.csv': [str(i) for i in range(1997, 2017+1)],
           'SAGDP4N__ALL_AREAS_1997_2017.csv': [str(i) for i in range(1997, 2017+1)],
           'SAGDP5N__ALL_AREAS_1997_2017.csv': [str(i) for i in range(1997, 2017+1)],
           'SAGDP6N__ALL_AREAS_1997_2017.csv': [str(i) for i in range(1997, 2017+1)],
           'SAGDP7N__ALL_AREAS_1997_2017.csv': [str(i) for i in range(1997, 2017+1)],
           'SAGDP8N__ALL_AREAS_1997_2017.csv': [str(i) for i in range(1997, 2017+1)],
           'SAGDP9N__ALL_AREAS_1997_2017.csv': [str(i) for i in range(1997, 2017+1)],
           'SAGDP11N__ALL_AREAS_1998_2017.csv': [str(i) for i in range(1998, 2017+1)]}

    df = pd.DataFrame(columns=['GeoFIPS', 'GeoName', 'Region', 'TableName', 'ComponentName',
                               'Unit', 'IndustryId', 'IndustryClassification', 'Description', 'year', 'value'])

    for i in file:

        a = pd.read_csv(path + i, index_col=None)

        # drop nonsense rows
        a.drop(index=a[a['GeoName'].isnull()].index, inplace=True)

        a['GeoFIPS'] = [a.loc[i, 'GeoFIPS'].replace('"', '') for i in range(len(a))]

        a['GeoName'] = [a.loc[i, 'GeoName'].replace('*', '') for i in range(len(a))]

        for j in a.keys():
            # test if the entire column is an object dtype
            if a[j].dtype == 'O':
                a[j] = [a.loc[i, j].strip() for i in range(len(a)) if type(a.loc[i, j]) == str]

        # melt data
        a = pd.melt(a, id_vars=a.keys()[0:9], var_name='year')

        # data typing
        a['IndustryId'] = a['IndustryId'].map(str)

        econ_map = pd.read_csv('.{s}core_maps{s}units.csv'.format(s=os.sep), index_col=None)
        a['Unit'] = a['Unit'].map(dict(zip(econ_map['from'], econ_map['to'])))

        region_map = pd.read_csv('.{s}core_maps{s}regions.csv'.format(s=os.sep), index_col=None)
        a['GeoName'] = a['GeoName'].map(dict(zip(region_map['from'], region_map['to'])))

        a['value'] = pd.to_numeric(a['value'], errors='coerce')
        a['value'].fillna(0, inplace=True)

        # append to dataframe
        df = df.append(a, sort=False, ignore_index=True)

    return df


def file_parser_to_csv():
    a = file_parser()
    a.to_csv('gsp_all.csv')


def file_parser_to_sql():
    # parse Gross State Product
    df = file_parser()
    df['IndustryId'] = pd.to_numeric(df['IndustryId'], errors='coerce')
    df['IndustryId'] = df['IndustryId'].map(int)

    types = {'GeoFIPS': TEXT,
             'GeoName': TEXT,
             'Region': TEXT,
             'TableName': TEXT,
             'ComponentName': TEXT,
             'Unit': TEXT,
             'IndustryId': TEXT,
             'IndustryClassification': TEXT,
             'Description': TEXT,
             'year': INTEGER,
             'value': FLOAT}
    df.to_sql('gsp', con=engine, if_exists='replace', dtype=types)

    tbl_desc = 'BEA Gross State Product (Annual) -- https://www.bea.gov/data/gdp/gdp-state'
    comment = text("COMMENT ON TABLE gsp IS '{}';".format(
        tbl_desc)).execution_options(autocommit=True)
    engine.execute(comment)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--csv-out', dest='csv', action='store_true')
    parser.set_defaults(csv=False)

    args = parser.parse_args()

    if args.csv == False:
        file_parser_to_sql()
    elif args.csv == True:
        file_parser_to_csv()
