from sqlalchemy import create_engine
import getpass


user = getpass.getuser()
db = 'windc'

windc_engine = create_engine('postgresql://'+user+':@localhost:5432/'+db)
