import windc_data as wd


if __name__ == "__main__":

    w1 = wd.WindcEnvironment(
        gams_sysdir="/Applications/GAMS30.3/Resources/sysdir",
        data_dir="~/Desktop/windc_raw_data",
        version="windc_2_0_1",
    )

    w1.rebuild(gdxout=True)
