import windc_data as wd


if __name__ == "__main__":

    w2 = wd.WindcEnvironment(
        gams_sysdir="/Applications/GAMS30.3/Resources/sysdir",
        data_dir="~/Desktop/windc_raw_data",
        version="windc_2_1",
    )

    w2.rebuild()
