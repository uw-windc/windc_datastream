import os


class WindcLoader:
    """
    A container object that can be used to load economic data for the WINDC system.

    """

    def __init__(self, data_path, sql_engine=None):
        """ __init__ constructor """

        # specify datasources directory
        if os.path.isabs(data_path) == False:
            if data_path[0] == '~':
                self.data_path = os.path.expanduser(data_path)
            else:
                self.data_path = os.path.abspath(data_path)

        # locate different verisions of the data source files
        self.data_version_paths = {}
        self.data_version_paths['1.0'] = os.path.join(self.data_path, 'data_version_1.0')
        self.data_version_paths['2.0'] = os.path.join(self.data_path, 'data_version_2.0')

        # sql engine connection
        self.sql_engine = sql_engine
