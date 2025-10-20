import pickle
import modules.db_connection as db
import os

# load env variables
db.load_env_variables(path="./.env")
# connection string
db.get_connection_string("defaultdb")
# engine
engine = db.get_engine("defaultdb")
db.check_database_available(engine=engine)


