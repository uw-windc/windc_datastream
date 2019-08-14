import scrub_bea_supply
import scrub_bea_use
import scrub_bea_supply_detailed
import scrub_bea_use_detailed
import scrub_cfs
import scrub_sgf
import scrub_gsp
import scrub_pce
import scrub_emissions
import scrub_nass
import scrub_eia_crude_oil
import scrub_usatrade
import scrub_seds
import scrub_heatrate
import argparse


def all_scrub_to_csv(from_sql):

    scrub_bea_supply.scrub_supply_to_csv(from_sql=from_sql)
    scrub_bea_use.scrub_use_to_csv(from_sql=from_sql)
    scrub_bea_supply_detailed.scrub_supply_to_csv(from_sql=from_sql)
    scrub_bea_use_detailed.scrub_use_to_csv(from_sql=from_sql)
    scrub_cfs.scrub_cfs_to_csv(from_sql=from_sql)
    scrub_sgf.scrub_sgf_to_csv(from_sql=from_sql)
    scrub_gsp.scrub_gsp_to_csv(from_sql=from_sql)
    scrub_pce.scrub_pce_to_csv(from_sql=from_sql)
    scrub_emissions.scrub_emissions_to_csv(from_sql=from_sql)
    scrub_nass.scrub_nass_to_csv(from_sql=from_sql)
    scrub_eia_crude_oil.scrub_eia_crude_oil_to_csv(from_sql=from_sql)
    scrub_usatrade.scrub_usatrade_to_csv(from_sql=from_sql)
    scrub_seds.scrub_seds_to_csv(from_sql=from_sql)
    scrub_heatrate.scrub_eia_gen_heatrate_to_csv(from_sql=from_sql)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--csv-out', dest='csv', action='store_true')
    parser.add_argument('--no-sql', dest='sql', action='store_false')
    parser.set_defaults(csv=False)
    parser.set_defaults(sql=True)

    args = parser.parse_args()

    if args.csv == True:
        all_scrub_to_csv(from_sql=args.sql)
