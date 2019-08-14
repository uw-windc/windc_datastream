import join_sgf
import parse_bea_use_io
import parse_bea_supply_io
import parse_bea_use_io_detailed
import parse_bea_supply_io_detailed
import parse_state_gsp
import parse_2012cfs
import parse_nass
import parse_crude_oil
import join_usatrade
import join_pce
import join_emissions
import parse_seds
import parse_heatrate
import argparse


def all_parse_to_csv():

    join_sgf.join_all_sgf_to_csv()
    parse_bea_use_io.file_parser_to_csv()
    parse_bea_supply_io.file_parser_to_csv()
    parse_bea_use_io_detailed.file_parser_to_csv()
    parse_bea_supply_io_detailed.file_parser_to_csv()
    parse_state_gsp.file_parser_to_csv()
    parse_2012cfs.file_parser_to_csv()
    join_pce.join_all_pce_to_csv()
    join_emissions.join_all_emissions_to_csv()
    parse_nass.parse_file_to_csv()
    parse_crude_oil.parse_file_to_csv()
    join_usatrade.join_all_usatrade_to_csv()
    parse_seds.parse_file_to_csv()
    parse_heatrate.parse_file_to_csv()


def all_parse_to_sql():

    join_sgf.join_all_sgf_to_sql()
    parse_bea_use_io.file_parser_to_sql()
    parse_bea_supply_io.file_parser_to_sql()
    parse_bea_use_io_detailed.file_parser_to_sql()
    parse_bea_supply_io_detailed.file_parser_to_sql()
    parse_state_gsp.file_parser_to_sql()
    parse_2012cfs.file_parser_to_sql()
    join_pce.join_all_pce_to_sql()
    join_emissions.join_all_emissions_to_sql()
    parse_nass.parse_file_to_sql()
    parse_crude_oil.parse_file_to_sql()
    join_usatrade.join_all_usatrade_to_sql()
    parse_seds.parse_file_to_sql()
    parse_heatrate.parse_file_to_sql()


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--csv-out', dest='csv', action='store_true')
    parser.set_defaults(csv=False)

    args = parser.parse_args()

    if args.csv == False:
        all_parse_to_sql()
    elif args.csv == True:
        all_parse_to_csv()
