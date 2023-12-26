"""
Module for updating the snooker.org tables in the database
"""
from ..scrapers import snookerorg

from ..transformers.snookerorg import game_transform
from ..transformers.snookerorg import player_transform

from ..create import create_snookerorg_player_table, create_upcoming_game_table
from ..write import to_database

def get_snookerorg_player():
	"""
	Get DataFrame representing snookerorg_player table

	Returns
	-------
	player_df : pandas.DataFrame
		DataFrame representing the snookerorg_player table 
	"""
	raw_player_df = snookerorg.get_raw_player()
	player_df = player_transform.raw_player_transform(raw_player_df)
	return player_df

def get_new_snooker_org_player():
	"""
	Get DataFrame with new players not currently in snookerorg_player table

	Returns
	------
	player_df : pandas.DataFrame or None
		DataFrame with new players not currently in the snookerorg_player table
	"""
	raw_player_df = snookerorg.get_new_raw_player()
	if raw_player_df is None:
		return None
	else:
		player_df = player_transform.raw_player_transform(raw_player_df)
		return player_df

def get_upcoming_game():
	"""
	Get DataFrame representing upcoming_game table

	Returns
	-------
	upcoming_game_df : pandas.DataFrame
		DataFrame representing the upcoming_game table
	"""
	raw_upcoming_game_df = snookerorg.get_raw_upcoming_game()
	upcoming_game_df = game_transform.raw_game_transform(raw_upcoming_game_df)
	return upcoming_game_df

def update():
	"""
	Update the snookerorg_player and upcoming_game tables in the database
	"""
	print("Updating the snooker.org tables in the database (snookerorg_player and upcoming_game)\n")
	upcoming_game_df = get_upcoming_game()
	create_upcoming_game_table(upcoming_game_df)
	snookerorg_player_df = get_new_snooker_org_player()
	if snookerorg_player_df is not None:
		to_database(snookerorg_player_df,"snookerorg_player","append")
	print("The snooker.org tables have been updated\n")

