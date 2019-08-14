from pandas import DataFrame
import parse_97states
import parse_98states
import parse_99states
import parse_00_11states
import parse_12_13states
import parse_14_16states
from sql_engines import windc_engine as engine
from sqlalchemy import INTEGER, TEXT, FLOAT, text
import argparse


def join_all_sgf():
    # create empty dataframe container
    df = DataFrame(columns=['mapped_label', 'region', 'year', 'value', 'units'])

    # 1997
    df_p = parse_97states.file_parser()
    df_p['year'] = 1997
    df = df.append(df_p, sort=False, ignore_index=True)

    # 1998
    df_p = parse_98states.file_parser()
    df_p['year'] = 1998
    df = df.append(df_p, sort=False, ignore_index=True)

    # 1999
    df_p = parse_99states.file_parser()
    df_p['year'] = 1999
    df = df.append(df_p, sort=False, ignore_index=True)

    # 2000 - 2011
    t = {'00statess.xlsx': {'year': '2000', 'data_rng': 'A8:EX64', 'col_rng': 'A3:EX3'},
         '01statess.xlsx': {'year': '2001', 'data_rng': 'A8:EX66', 'col_rng': 'A3:EX3'},
         '02statess.xlsx': {'year': '2002', 'data_rng': 'A8:CY66', 'col_rng': 'A3:CY3'},
         '03statess.xlsx': {'year': '2003', 'data_rng': 'A8:CY65', 'col_rng': 'A3:CY3'},
         '04statess.xlsx': {'year': '2004', 'data_rng': 'A8:AZ64', 'col_rng': 'A3:AZ3'},
         '05statess.xlsx': {'year': '2005', 'data_rng': 'A8:AZ64', 'col_rng': 'A3:AZ3'},
         '06statess.xlsx': {'year': '2006', 'data_rng': 'A8:AZ64', 'col_rng': 'A3:AZ3'},
         '07statess.xlsx': {'year': '2007', 'data_rng': 'A8:AZ64', 'col_rng': 'A3:AZ3'},
         '08statess.xlsx': {'year': '2008', 'data_rng': 'A8:AZ64', 'col_rng': 'A3:AZ3'},
         '09statess.xlsx': {'year': '2009', 'data_rng': 'A8:AZ64', 'col_rng': 'A3:AZ3'},
         '10statess.xlsx': {'year': '2010', 'data_rng': 'A8:AZ64', 'col_rng': 'A3:AZ3'},
         '11statess.xlsx': {'year': '2011', 'data_rng': 'A8:AZ64', 'col_rng': 'A3:AZ3'}}

    for i in t.keys():
        df_p = parse_00_11states.parse_file(
            filename=i, year=t[i]['year'], data_rng=t[i]['data_rng'], col_rng=t[i]['col_rng'])
        df_p['year'] = int(t[i]['year'])
        df = df.append(df_p, sort=False, ignore_index=True)

    # 2012 - 2013
    t = {'SGF_2012_SGF001.csv': {'year': '2012'},
         'SGF_2013_SGF003.csv': {'year': '2013'}}

    for i in t.keys():
        df_p = parse_12_13states.parse_file(filename=i, year=t[i]['year'])
        df_p['year'] = int(t[i]['year'])
        df = df.append(df_p, sort=False, ignore_index=True)

    # 2014 - 2016
    t = {'SGF_2014_00A1.csv': {'year': '2014'},
         'SGF_2015_00A1.csv': {'year': '2015'},
         'SGF_2016_00A1.csv': {'year': '2016'}}

    for i in t.keys():
        df_p = parse_14_16states.parse_file(filename=i, year=t[i]['year'])
        df_p['year'] = int(t[i]['year'])
        df = df.append(df_p, sort=False, ignore_index=True)

    # column rename
    df.rename({'mapped_label': 'label', 'region': 'geographic_region'},
              axis='columns', inplace=True)

    return df


def join_all_sgf_to_csv():
    s = join_all_sgf()
    s.to_csv('sgf_all.csv')


def join_all_sgf_to_sql():
    # parse SGF
    df = join_all_sgf()

    # to sql
    types = {'label': TEXT,
             'geographic_region': TEXT,
             'year': INTEGER,
             'value': FLOAT,
             'units': TEXT}
    df.to_sql('sgf', con=engine, if_exists='replace', dtype=types)

    tbl_desc = 'Annual Survey of State Government Finances -- https://www.census.gov/programs-surveys/state/data/tables.html'
    comment = text("COMMENT ON TABLE sgf IS '{}';".format(
        tbl_desc)).execution_options(autocommit=True)
    engine.execute(comment)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--csv-out', dest='csv', action='store_true')
    parser.set_defaults(csv=False)

    args = parser.parse_args()

    if args.csv == False:
        join_all_sgf_to_sql()
    elif args.csv == True:
        join_all_sgf_to_csv()
