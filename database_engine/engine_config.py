# mysql -h sports.cja6lzmqlwco.us-east-1.rds.amazonaws.com -P 3306 -u admin -p
# sharpsports
"""
Module for configuring the server and database to connect to
"""
import sqlalchemy

DATABASE = "snooker"
PORT = 3306

def set_server(name = "AWS"):
	"""
	Set the server for the MySQL database

	Parameters
	----------
	name : str
		The name of the server to use
	"""
	if name == "AWS":
		user = "admin"
		password = "sharpsports"
		host = "sports.cja6lzmqlwco.us-east-1.rds.amazonaws.com"
	else:
		user = "root"
		password = "billP+0-2"
		host = "localhost"
	return user, password, host

USER, PASSWORD, HOST = set_server("AWS")
CONNECT_STRING = f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
SNOOKER_ENGINE = sqlalchemy.create_engine(CONNECT_STRING)