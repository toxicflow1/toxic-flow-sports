"""
Module for writing to the database
"""
import pandas as pd
from .engine_config import SNOOKER_ENGINE
from .read import read, get_tables
from. delete import delete_duplicate_primary_keys
__all__ = ["to_database"]

# The number of rows to include in each chunk when uploading to the database
CHUNKSIZE = 5000

def append_to_database(df_to_append, table_name):
	"""
	Add a DataFrame to an existing table in the database

	Parameters
	----------
	df_to_append : pandas.DataFrame
		The DataFrame to append to the database
	table_name : str
		The name of the table to append to
	"""
	if df_to_append.empty == True:
		print(f"There were no new entries to add to the {table_name} table!\n")
	else:
		delete_duplicate_primary_keys(df_to_append, table_name)
		print(f"Adding the following entries to the {table_name} table in the snooker database...")
		print(df_to_append)
		df_to_append.to_sql(table_name, SNOOKER_ENGINE, if_exists = "append", chunksize = CHUNKSIZE)

def replace_in_database(replacing_df, table_name):
	"""
	Replace an existing table in the database with a DataFrame

	Parameters
	----------
	replacing_df : pandas.DataFrame
		The DataFrame to replace the table in the Database
	table_name : str
		The name of the table to repalce
	"""
	print(f"Replacing the current {table_name} table with the following table...")
	print(replacing_df)
	SNOOKER_ENGINE.execute("SET FOREIGN_KEY_CHECKS = 0")
	replacing_df.to_sql(table_name, SNOOKER_ENGINE, if_exists = "replace", chunksize = CHUNKSIZE)
	SNOOKER_ENGINE.execute("SET FOREIGN_KEY_CHECKS = 1")

def to_database(df_to_write, table_name, if_table_exists = "append"):
	"""
	Write a DataFrame to the snooker database

	The function uploads the DataFrame passed as an argument to the MySQL DataBase.
	If the DataFrame is being inserted into an existing table, first the existing table
	will be checked for entries where primary keys match. If theere at any entries where primary keys
	match, then these will be deleted so that the new DataFrame can be uploaded with no issue.

	Parameters
	----------
	df_to_write : pandas.DataFrame
		The DataFrame to upload to the snooker database
	table_name : str
		Name of SQL table
	if_exists : {‘fail’, ‘replace’, ‘append’}, default "append"
		How to behave if the table already exists.
		- fail: Raise a ValueError.
		- replace: Drop the table before inserting new values.
		- append: Insert new values to the existing table.

	Returns
	-------
	None
	"""
	# If we are inserting new values to the existing table
	if if_table_exists == "append":
		if table_name in get_tables():
			append_to_database(df_to_write, table_name)
			return None
		else:
			print(f"Creating a new table called {table_name} in the database...")
	elif if_table_exists == "replace":
		if table_name in get_tables():
			replace_in_database(df_to_write, table_name)
			return None
		else:
			print(f"Creating a new table called {table_name} in the database...")
	print(df_to_write)
	df_to_write.to_sql(table_name, SNOOKER_ENGINE, if_exists = if_table_exists, chunksize = CHUNKSIZE)
	print(f"The upload to the database was succesful!\n")
	return None



