import pandas as pd
import re
import os


def parse_file(filename, year):
    path = '.{s}datasources{s}SGF{s}'.format(s=os.sep)
    file = filename

    region_map = pd.read_csv('.{s}core_maps{s}regions.csv'.format(s=os.sep), index_col=None)
    region_map = dict(zip(region_map['from'], region_map['to']))

    sgf_map = pd.read_csv('.{s}core_maps{s}sgf.csv'.format(s=os.sep), index_col=None)
    sgf_map.drop(index=sgf_map[sgf_map['finsrc.id_2014'].isnull() == True].index, inplace=True)
    sgf_labels = dict(zip(sgf_map['finsrc.id_2014'], sgf_map['finsrc.id_2014_relabel']))
    sgf_units = dict(zip(sgf_map['finsrc.id_2014'], sgf_map['finsrc.id_2014_units']))

    a = pd.read_csv(path+file, index_col=None)
    a.drop(columns=['GEO.id', 'GEO.id2', 'GEO.annotation.id', 'YEAR.id',
                    'FINTYPE.id', 'FINTYPE.display-label', 'FINSRC.display-label'], inplace=True)
    a.rename({'GEO.display-label': 'region', 'AMOUNT': 'value'}, axis='columns', inplace=True)

    # get rid of non-float types
    a['value'] = pd.to_numeric(a['value'], errors='coerce')

    # zero out any null values
    a.fillna(0, inplace=True)

    # harmonize labels
    a['mapped_label'] = a['FINSRC.id'].map(sgf_labels)
    a['units'] = a['FINSRC.id'].map(sgf_units)
    a['region'] = a['region'].map(region_map)

    # sort a bit
    a.sort_values(by=['region'], inplace=True)
    a.reset_index(drop=True, inplace=True)

    # write out a harmonized csv file with data
    df2 = a[['mapped_label', 'region', 'value', 'units']]

    return df2


def parse_file_to_csv(filename, year):
    s = parse_file(filename=filename, year=year)
    s.to_csv('sgf_'+str(year)+'.csv')


def file_parser_to_csv():
    t = {'SGF_2014_00A1.csv': {'year': '2014'},
         'SGF_2015_00A1.csv': {'year': '2015'},
         'SGF_2016_00A1.csv': {'year': '2016'}}

    for i in t.keys():
        parse_file_to_csv(filename=i, year=t[i]['year'])


if __name__ == '__main__':

    file_parser_to_csv()
