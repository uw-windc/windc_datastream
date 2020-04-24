from pandas import read_csv, read_sql
from sql_engines import windc_engine
from sqlalchemy import TEXT, INTEGER, FLOAT, text
import parse_2012cfs
import argparse
import os


def scrub_cfs(from_sql):
    if from_sql == True:
        print('Sourcing Commodity Flow Survey data from SQL database...')
        # query sql database
        sql = 'select * from cfs_2012;'
        cfs = read_sql(sql, con=windc_engine, index_col='index')

        sql = 'select * from cfs_2012_orig_dest_cfs_area_map;'
        apdx1 = read_sql(sql, con=windc_engine, index_col='index')

        sql = 'select * from cfs_2012_naics_map;'
        apdx2 = read_sql(sql, con=windc_engine, index_col='index')

        sql = 'select * from cfs_2012_sctg_map;'
        apdx3 = read_sql(sql, con=windc_engine, index_col='index')

        sql = 'select * from cfs_2012_mode_map;'
        apdx4 = read_sql(sql, con=windc_engine, index_col='index')

    else:
        print('Sourcing Commodity Flow Survey data from raw files...')
        cfs, apdx1, apdx2, apdx3, apdx4 = parse_2012cfs.file_parser()

    # drop data that is not in set r (states)
    states = read_csv('.{s}core_maps{s}gams{s}set_regions.csv'.format(s=os.sep), index_col=None)
    states = list(states.keys())
    cfs['test'] = cfs['ORIG_STATE'].isin(states)
    cfs.drop(cfs[cfs['test'] == False].index, inplace=True)

    cfs['test'] = cfs['DEST_STATE'].isin(states)
    cfs.drop(cfs[cfs['test'] == False].index, inplace=True)

    # drop all data that is associated with foregin exports
    cfs.drop(cfs[cfs['EXPORT_YN'] == 'Y'].index, inplace=True)
    cfs.drop(columns='EXPORT_YN', inplace=True)

    # drop all missing or supressed data
    cfs.drop(cfs[cfs['ORIG_MA'] == 0].index, inplace=True)
    cfs.drop(cfs[cfs['DEST_MA'] == 0].index, inplace=True)

    # drop undisclosed SCTG data
    und = ['00', '25-30', '01-05', '15-19', '10-14',
           '06-09', '39-99', '20-24', '31-34', '35-38', '99']
    cfs['test'] = cfs['SCTG'].isin(und)
    cfs.drop(cfs[cfs['test'] == True].index, inplace=True)

    cfs.drop(columns='test', inplace=True)

    return cfs, apdx1, apdx2, apdx3, apdx4


def scrub_cfs_to_csv(from_sql):
    cfs, apdx1, apdx2, apdx3, apdx4 = scrub_cfs(from_sql=from_sql)
    cfs.to_csv('cfs_2012_pumf_scrubed.csv')
    apdx1.to_csv('cfs_2012_appendix_1.csv')
    apdx2.to_csv('cfs_2012_appendix_2.csv')
    apdx3.to_csv('cfs_2012_appendix_3.csv')
    apdx4.to_csv('cfs_2012_appendix_4.csv')


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--csv-out', dest='csv', action='store_true')
    parser.add_argument('--no-sql', dest='sql', action='store_false')
    parser.set_defaults(csv=False)
    parser.set_defaults(sql=True)

    args = parser.parse_args()

    if args.csv == True:
        scrub_cfs_to_csv(from_sql=args.sql)
