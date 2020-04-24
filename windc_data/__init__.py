__all__ = ['get']


# create database connection for use in package
from sqlalchemy import create_engine
import getpass
import os

user = getpass.getuser()
db = 'windc'

windc_engine = create_engine('postgresql://'+user+':@localhost:5432/'+db)


# data file path structures
root_path = os.path.join('.')
build_dir = os.path.join(root_path, 'data_build')
data_dir = os.path.join(root_path, 'datasources')

# Build directories
build_ver = {}
build_ver['1.0'] = os.path.join(build_dir, 'data_build_v1.0')
build_ver['2.0'] = os.path.join(build_dir, 'data_build_v2.0')

# Data directories
data_paths = {}
data_paths['1.0'] = os.path.join(data_dir, 'datasources_v1.0')
data_paths['2.0'] = os.path.join(data_dir, 'datasources_v2.0')
