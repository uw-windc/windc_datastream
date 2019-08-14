import pandas as pd
import os


def parse_file(filename):
    path = '.{s}datasources{s}PCE{s}'.format(s=os.sep)

    a = pd.read_csv(path+filename, index_col=None)

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
    a['value'] = pd.to_numeric(a['value'], errors='coerce')
    a['value'].fillna(0, inplace=True)

    a.rename({'Unit': 'units'}, axis='columns', inplace=True)
    econ_map = pd.read_csv('.{s}core_maps{s}units.csv'.format(s=os.sep), index_col=None)
    a['units'] = a['units'].map(dict(zip(econ_map['from'], econ_map['to'])))

    # drop unnecessary columns
    a.drop(columns='IndustryClassification', inplace=True)

    # standardize regions
    region_map = pd.read_csv('.{s}core_maps{s}regions.csv'.format(s=os.sep), index_col=None)
    region_map = dict(zip(region_map['from'], region_map['to']))

    a['GeoName'] = a['GeoName'].map(region_map)

    return a


def parse_file_to_csv(filename):
    s = parse_file(filename=filename)
    s.to_csv(filename.split('_')[0].lower() + '_parsed.csv')


def file_parser_to_csv():
    files = ['SAEXP1_1997_2017_ALL_AREAS_.csv',
             'SAEXP2_1997_2017_ALL_AREAS_.csv']

    for i in files:
        parse_file_to_csv(filename=i)


if __name__ == '__main__':

    file_parser_to_csv()
