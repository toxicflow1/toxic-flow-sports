"""
Module for refreshing tables in the database
"""
from .scrapers import cuetracker
from .transformers.cuetracker import tournament_transform
from .transformers.cuetracker import game_transform
from .transformers.cuetracker import player_transform

from .create import *
from .read import *
from .drop import *
from. write import to_database

__all__ = ["raw_refresh",
"tournament_refresh",
"player_refresh",
"game_refresh",
"full_refresh",
"half_refresh"]

def raw_refresh():
	"""
	Refresh the raw_game and raw_tournament 

	This function redownloads data from CueTracker and adds it to the raw_game and raw_tournament tables in
	the database
	"""
	raw_game, raw_tournament = cuetracker.get_all_raw()
	# Upload the raw data to the database
	to_database(raw_game, "raw_game", if_table_exists = "replace")
	to_database(raw_tournament, "raw_tournament", if_table_exists = "replace")

def tournament_refresh():
	"""
	Refresh the tournament table in the database

	This function refreshes the tournament table in the database. It reads the raw_tournament table from the database
	and converts it into the nicely formatted tournament table. 
	If there is a tournament table present, this function will delete that table and replace it with a new one and if there is
	no tournamnet table present, this function will simply create a new tournament table.
	In order for this function to work properly, the raw_tournament table in the database should be updated to contain the
	latest raw data on the tournaments
	"""
	print("Refreshing the tournament table in the snooker database...")
	raw_tournament_df = read_raw_tournament()
	tournament_df = tournament_transform.raw_tournament_transform(raw_tournament_df)
	create_tournament_table(tournament_df)
	print("Tournament table refresh complete!\n")

def player_refresh():
	"""
	Refresh the player table in the database

	This function refreshes the player table in the database. It reads the raw_game table from the database and converts
	it into the nicely formatted player table.
	If there is a player table present, this function will delete that table and replace it with a new one and if there is
	no player table present, this function will simply create a new player table.
	In order for this function to work propertly, the raw_game table in the database should be updated to contain the latest raw
	data on the games
	"""
	print("Refreshing the player table in the snooker database...")
	raw_game_df = read_raw_game()
	player_df = player_transform.raw_game_to_player_transform(raw_game_df)
	create_player_table(player_df)
	print("Player table refresh complete!\n")

def game_refresh():
	"""
	Refresh the game table in the database

	This function refreshes the game table in the database. It reads the raw_game table from the database
	and converts it into the nicely formatted game table.
	If there is a game table present, this function will delete that table and replace it with a new one and if there is
	no game table present, this function will simply create a new game table.
	In order for this function to work properly, the raw_game and raw_tournament tables in the database should be updated to 
	contain the latest raw data on the games. The tournament and player tables should also be refreshed to reflect
	the lastest information in the raw game and tournament tables.
	"""
	print("Refreshing the game table in the snooker database...")
	raw_game_df = read_raw_game()
	tournament_df = read_tournament()
	player_df = read_player()
	game_df = game_transform.raw_game_transform(raw_game_df, tournament_df, player_df)
	create_game_table(game_df)
	print("Game table refresh complete!\n")

def full_refresh():
	"""
	Do a refresh of the database

	This function 
	"""
	print("---- PERFORMING A FULL REFRESH OF THE SNOOKER DATABASE ----")
	drop_all()
	raw_refresh()
	tournament_refresh()
	player_refresh()
	game_refresh()
	print("THE WHOLE DATABASE WAS SUCCESSFULLY REFRESHED!\n")

def half_refresh():
	"""
	Do a half refresh of the database

	This function refreshes the tournament, player and game table in the database. It reads the raw_game and
	raw_tournament tables and converts them into the formatted tables.
	If any of the three tables are present, this function will delete the current table and replace it with a new one.
	In order for this function to work properly, the raw_game and raw_tournament table should be updated to contain
	the latest raw data on the games and tournaments. 
	"""
	print("---- REFRESHING THE FORMATTED TABLES IN THE SNOOKER DATABASE ----\n")
	drop_all_formatted()
	tournament_refresh()
	player_refresh()
	game_refresh()
	print("TOURNAMENT, PLAYER AND GAME REFRESHES COMPLETE!\n")






