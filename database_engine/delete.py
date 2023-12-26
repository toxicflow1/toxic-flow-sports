"""
Module for deleting rows from tables in the database
"""
from .engine_config import SNOOKER_ENGINE
from .read import read

def delete_duplicate_primary_keys(df_to_upload, table_name):
	"""
	Delete rows from a table in the database which have the same primary key as the DataFrame which is about to
	be uploaded to the database

	Parameters
	----------
	df_to_upload : pandas.DataFrame
		DataFrame which is about the be uploaded to the database. It may contain the same primary keys as items already
		in the database, therefore these rows need to be deleted from the database
	table_name : pandas.DataFrame
		The name of the table which is about to be uploaded to the database
	"""
	current_df = read(table_name)
	if current_df is not None:
		index_column_name = current_df.index.name
		replace_mask = df_to_upload.index.isin(current_df.index)
		ids_to_replace = tuple(df_to_upload.index[replace_mask])
		number_of_ids_to_replace = len(ids_to_replace)
		if number_of_ids_to_replace > 0:
			print(f"Deleting the following entries from the {table_name} table in the snooker database...")
			delete_mask = current_df.index.isin(ids_to_replace)
			print(current_df.loc[delete_mask,:])
			SNOOKER_ENGINE.execute("SET FOREIGN_KEY_CHECKS = 0")
			if number_of_ids_to_replace == 1:
				SNOOKER_ENGINE.execute(f"DELETE FROM {table_name} WHERE {index_column_name} = {ids_to_replace[0]}")
			else:
				SNOOKER_ENGINE.execute(f"DELETE FROM {table_name} WHERE {index_column_name} IN {ids_to_replace}")
			SNOOKER_ENGINE.execute("SET FOREIGN_KEY_CHECKS = 1")

def delete_from_raw_game(game_ids):
	"""
	Delete rows from the raw_game table

	Parameters
	---------
	game_ids : int or sequence-like
		The game IDs corresponding to the rows to delete from the raw_game table
	"""
	if isinstance(game_ids,int):
		SNOOKER_ENGINE.execute(f"DELETE FROM raw_game WHERE game_id = {game_ids}")
	else:
		if len(game_ids) == 1:
			SNOOKER_ENGINE.execute(f"DELETE FROM raw_game WHERE game_id = {game_ids[0]}")
		else:
			SNOOKER_ENGINE.execute(f"DELETE FROM raw_game WHERE game_id IN {tuple(game_ids)}")
