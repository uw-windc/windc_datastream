import pandas as pd
import re
import os


def parse_file(filename, nickname):
    path = '.{s}datasources{s}USATradeOnline{s}'.format(s=os.sep)
    file = filename

    v = {'State Exports by NAICS Commodities.csv': 'Total Exports Value ($US)',
         'State Imports by NAICS Commodities.csv': 'Customs Value (Gen) ($US)'}

    skip = {'State Exports by NAICS Commodities.csv': 3,
            'State Imports by NAICS Commodities.csv': 2}

    a = pd.read_csv(path+file, skiprows=skip[filename])
    a.rename({'State': 'region', 'Time': 'year',
              v[filename]: 'value'}, axis='columns', inplace=True)

    # drop all columns that have only null values
    a.dropna(how='all', axis=1, inplace=True)

    # strip whitespace from 'object' type columns
    for j in a.keys():
        # test if the entire column is an object dtype
        if a[j].dtype == 'O':
            a[j] = [a.loc[i, j].strip() for i in range(len(a)) if type(a.loc[i, j]) == str]

    # convert values to numeric
    a['value'] = [a.loc[i, 'value'].replace(',', '') for i in a.index]
    a['value'] = a['value'].map(int)

    # pull NAICS code out to new column
    a['NAICS'] = [a.loc[i, 'Commodity'].split(' ')[0] for i in a.index]
    a['NAICS'] = a['NAICS'].map(int)

    # pull NAICS description out
    a['Commodity_desc'] = [a.loc[i, 'Commodity'].split(
        str(a.loc[i, 'NAICS'])+' ')[1] for i in a.index]

    # region map
    region_map = pd.read_csv('.{s}core_maps{s}regions.csv'.format(s=os.sep), index_col=None)
    region_map = dict(zip(region_map['from'], region_map['to']))
    a['region'] = a['region'].map(region_map)
    a['Country'] = a['Country'].map(region_map)

    # add units label
    a['units'] = 'us dollars (USD)'

    return a


def file_parser_to_csv():
    t = {'State Exports by NAICS Commodities.csv': {'nickname': 'exports'},
         'State Imports by NAICS Commodities.csv': {'nickname': 'imports'}}

    for i in t:
        df = parse_file(filename=i, nickname=t[i]['nickname'])
        df.to_csv('usatrade_'+t[i]['nickname']+'.csv')


if __name__ == '__main__':

    file_parser_to_csv()
