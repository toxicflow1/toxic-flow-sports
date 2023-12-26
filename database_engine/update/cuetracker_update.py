"""
Module for updating the CueTracker tables in the database
"""

import pandas as pd
import numpy as np
from ..scrapers import cuetracker
from ..transformers.cuetracker import tournament_transform
from ..transformers.cuetracker import player_transform
from ..transformers.cuetracker import game_transform

from ..engine_config import SNOOKER_ENGINE
from ..read import *
from ..write import to_database
from ..delete import delete_from_raw_game

__all__ = ["update",
"update_rating"]

def get_new_rows(df_one, database_df, columns = None):
	"""
	Get new rows from a DataFrame

	This function returns a DataFrame containing all the rows that appear in
	the 1st DataFrame that do not appear in the database DataFrame.

	Parameters
	----------
	df_one : pandas.DataFrame
		The DataFrame containing both new information and information that
		already appears in the database DataFrame.
	database_df : pandas.DataFrame
		The DataFrame containing information from the database
	columns : column label or sequence of labels, default = None
		The columns that are used to identify whether a row is a duplicated or is unique

	Returns
	-------
	new_rows_df : pandas.DataFrame
		DataFrame containing only new information that appears in 'df_one' but does not appear in the database
	"""
	combined_df = pd.concat([df_one, database_df, database_df], axis = 0)
	new_rows_df = combined_df.drop_duplicates(subset = columns, keep = False)
	new_rows_df.index = pd.RangeIndex(start = len(database_df), stop = len(database_df) + len(new_rows_df), name = database_df.index.name)
	return new_rows_df

def get_new_tournament_unindexed(latest_raw_tournament_df, raw_tournament_df):
	"""
	Return a DataFrame with new raw tournament data that in not currently in the database (without proper tournament
	IDs assigned)

	This function takes a DataFrame containing all the tournaments from "https://cuetracker.net/latest-matches" in the
	raw format. It then returns a DataFrame with NEW raw tournament data from "https://cuetracker.net/latest-matches".
	The DataFrame containing new raw tournament data comes in two different forms:
		1. Tournaments that appear in the current database, however CueTracker has changed some information related to the tournamnet,
	e.g, prize fund or dates. These tournaments will have the same tournament id as they do in the current database.
	The criteria used to determine if a tournament appears in the current database is if the new tournament has the same URL 
	as the URL in the database.
		2. New tournaments that are not in the current database. These tournaments will have tournamnet ids starting
	from the maximum tournament id in the current database. The criteria used to determine if a tournament is new
	is if the new tournament does not have the same URL as the URL in the database.

	Parameters
	----------
	latest_raw_tournament_df : pandas.DataFrame
		DataFrame with all the tournaments from "https://cuetracker.net/latest-matches" in raw format
	raw_tournament_df : pandas.DataFrame
		DataFrame representing the current raw_tournament table

	Returns
	-------
	new_raw_tournament_df_unindexed : pandas.DataFrame
		DataFrame with the NEW tournaments from "https://cuetracker.net/latest-matches" in raw format
		Tournaments that are not in the database dot not have tournament IDs and need to be assigned
		appropriate IDs. Tournaments already in the database will have their correct IDs.
	"""
	# Get raw data on the tournaments where the data is different from what currently is in the database
	new_raw_tournament_df = get_new_rows(latest_raw_tournament_df, raw_tournament_df)
	# Merge the different raw tournament data with the current raw tournament
	# If a tournament appears in the database currently, it will have a tournamnet id
	# If it does not appear in the database, the tournament id will be None
	new_raw_tournament_df_unindexed = pd.merge(new_raw_tournament_df,
	             raw_tournament_df.reset_index().loc[:,["tournament_id","url"]],
	             how = "left",
	             left_on = "url",
	             right_on = "url").set_index("tournament_id")
	return new_raw_tournament_df_unindexed

def index_new_tournament_df(new_raw_tournament_df_unindexed, raw_tournament_df):
	"""
	Return a DataFrame with new raw tournament data that is properly indexed

	This function takes a DataFrame containing new tournaments from CueTracker in the raw format, except
	the tournament IDs are wrong. It returns a DataFrame with suitable tournament IDs

	Parameters
	----------
	new_raw_tournament_df_unindexed : pandas.DataFrame
		DataFrame with the NEW tournaments from "https://cuetracker.net/latest-matches" in raw format
		Tournaments that are not in the database dot not have tournament IDs and need to be assigned
		appropriate IDs. Tournaments already in the database will have their correct IDs.
	raw_tournament_df : pandas.DataFrame
		DataFrame representing the current raw_tournament table

	Returns
	-------
	new_raw_tournament_df : pandas.DataFrame
		DataFrame containing the new tournaments in raw format that will be added to the database
	"""
	# Get a Boolean Series that is True at indexes where there is a new tournament (new tournaments have NULL IDs)
	new_tournament_mask = new_raw_tournament_df_unindexed.index.isnull()
	# Get a DataFrame with the new tournaments
	new_tournament_df = new_raw_tournament_df_unindexed.loc[new_tournament_mask,:]
	# Set the index for the new tournaments to start from the correct number
	max_tournament_id = max(raw_tournament_df.index)
	new_tournament_df.index = pd.RangeIndex(start = max_tournament_id + 1,
	                                        stop = (max_tournament_id + 1 + len(new_tournament_df)),
	                                        name = "tournament_id")
	# Get a Boolean Series that is True at indexes where there is an old tourament
	old_tournament_mask = ~new_tournament_mask
	# Get a DataFrame with the old tournaments
	old_tournament_df = new_raw_tournament_df_unindexed.loc[old_tournament_mask,:]
	# Combine the new and old tournaments DataFrames
	new_raw_tournament_df_indexed = pd.concat([old_tournament_df, new_tournament_df], axis = 0)
	new_raw_tournament_df_indexed.index = new_raw_tournament_df_indexed.index.astype(np.int64)
	return new_raw_tournament_df_indexed

def get_new_tournament(latest_raw_tournament_df, raw_tournament_df):
	"""
	Return a DataFrame with new raw tournament data that is not currently in the database with proper
	tournament IDs assigned

	This function takes a DataFrame containing all the new tournaments in raw format, without proper 
	tournament IDs and assigns proper tournament IDs to all the tournaments.

	Parameters
	----------
	latest_raw_tournament_df : pandas.DataFrame
		DataFrame with all the tournaments from "https://cuetracker.net/latest-matches" in raw format
	raw_tournament_df : pandas.DataFrame
		DataFrame with the current raw_tournament table

	Returns 
	-------
	new_raw_tournament_df : pandas.DataFrame
		DataFrame containing the new tournaments in raw format that will be added to the database
	"""
	# Get a DataFrame with new raw tournament data (without proper tournament IDs)
	new_raw_tournament_df_unindexed = get_new_tournament_unindexed(latest_raw_tournament_df, raw_tournament_df)
	new_raw_tournament_df = index_new_tournament_df(new_raw_tournament_df_unindexed, raw_tournament_df)
	return new_raw_tournament_df

def get_changed_game(latest_raw_game_df, raw_game_df):
	"""
	Get DataFrame with raw game data from games where there has been changes
	to match data (without proper game IDs assigned)

	This function takes a DataFrame containing all the games from tournaments
	that appear on the page "https://cuetracker.net/latest-matches" in raw
	format. It then returns a DataFrame with data on all the games which appear
	in tournaments where there has been at least one change to the game
	data

	Parameters
	----------
	latest_raw_game_df : pandas.DataFrame
		DataFrame with all then games from tournaments that appear on
		"https://cuetracker.net/latest-matches" in raw format
	raw_game_df : pandas.DataFrame
		DataFrame representing the current raw_game table in the database

	Returns
	-------
	changed_game_df : pandas.DataFrame
		DataFrame with the all the games from the latest tournaments in raw
		format, where there has been at least one change to a game in the
		tourmanet. The games in the dataframe will all be assigned a wrong
		game id
	"""
	# Get raw data on games where the data is different from what is currently in the database
	new_raw_game_df = get_new_rows(latest_raw_game_df, raw_game_df)
	# Get the tournament URLs for all the games where the data is different
	tournament_urls = set(new_raw_game_df.loc[:,"tournament_url"].values.flatten())
	# Get a boolean Series that is True at indexes where a game occurs in a tournament where a game has changed
	new_raw_game_mask = latest_raw_game_df.loc[:,"tournament_url"].isin(tournament_urls)
	# Get a DataFrame with all the games where there may have been changes
	changed_game_df = latest_raw_game_df.loc[new_raw_game_mask,:]
	return changed_game_df

def get_less_game(latest_raw_game_df, raw_game_df):
	"""
	Get DataFrame with raw game data from games in tournaments where there
	has been a reduction in the number of games in the tournament

	This function takes a DataFrame containing all the games from tournaments
	that appear on the page "https://cuetracker.net/latest-matches" in raw
	format. It then returns a DataFrame with data on all the games which appear
	in tournaments where there has been a reduction in the number of games

	Parameters
	----------
	latest_raw_game_df : pandas.DataFrame
		DataFrame with all then games from tournaments that appear on
		"https://cuetracker.net/latest-matches" in raw format
	raw_game_df : pandas.DataFrame
		DataFrame representing the current raw_game table in the database

	Returns
	-------
	less_game_df : pandas.DataFrame
		DataFrame with all the games from the latest tournaments in raw format,
		where there are less games than in the database
	"""
	list_of_game_dfs = []
	# For each tournament
	for url in get_distinct_tournament_urls(latest_raw_game_df):
		latest_game_mask = (latest_raw_game_df.loc[:,"tournament_url"] == url)
		latest_number_of_games = latest_game_mask.sum()
		db_game_mask = (raw_game_df.loc[:,"tournament_url"] == url)
		db_number_of_games = db_game_mask.sum()
		if latest_number_of_games < db_number_of_games:
			list_of_game_dfs.append(latest_raw_game_df.loc[latest_game_mask,:])
	if list_of_game_dfs == []:
		return pd.DataFrame(columns = raw_game_df.columns)
	else:
		less_game_df = pd.concat(list_of_game_dfs, axis = 0)
		return less_game_df

def get_new_game_unindexed(latest_raw_game_df, raw_game_df):
	"""
	Get DataFrame with new raw game data not currently in the database
	(without proper game IDs assigned)

	This function takes a DataFrame containing all the games from tournaments
	that appear on the page "https://cuetracker.net/latest-matches" in raw
	format. It then returns a DataFrame with new raw game data that is not
	currently in the database. Since it is quite difficult to tell if a game 
	is a new game, or if CueTracker adjusted something such as the score or
	date, a game is classified as new if
	- the game appears in a tournament where there has been a change to at
	least one match (change of score, names, best_of etc...)
	- the number of 

	Parameters
	----------
	latest_raw_game_df : pandas.DataFrame
		DataFrame with all then games from tournaments that appear on
		"https://cuetracker.net/latest-matches" in raw format
	raw_game_df : pandas.DataFrame
		DataFrame representing the current raw_game table in the database

	Returns
	-------
	new_game_df_unindexed : pandas.DataFrame
		DataFrame with the all the games from the latest tournaments in raw
		format, where there has been at least one change to a game in the
		tournament. The games in the dataframe will all be assigned a wrong
		game id.
	"""
	changed_game_df = get_changed_game(latest_raw_game_df, raw_game_df)
	less_game_df = get_less_game(latest_raw_game_df, raw_game_df)
	new_game_df_unindexed = pd.concat([changed_game_df, less_game_df], axis = 0)
	new_game_df_unindexed.drop_duplicates(inplace = True)
	new_game_df_unindexed.index.name = "game_id"
	return new_game_df_unindexed

def get_distinct_tournament_urls(generic_raw_game_df):
	"""
	Get an array containing the distinct tournament urls that appear in a raw game DataFrame

	Parameters
	----------
	generic_raw_game_df : pandas.DataFrame
		DataFrame containing raw game information

	Returns
	-------
	numpy.ndarray
		NumPy array with the distinct tournament URLs that appear in the raw game DataFrame
	"""
	return generic_raw_game_df.loc[:,"tournament_url"].drop_duplicates().values

def get_games_by_tournament_url(generic_raw_game_df, url):
	"""
	Return a DataFrame with all the games from a tournament specified by a tournament url

	Parameters
	----------
	generic_raw_game_df : pandas.DataFrame
		DataFrame containing raw game information (i.e the DataFrame must have a tournament_url column)
	url : str
		The tournament we want data for

	Returns
	-------
	games_from_tournament_df : pandas.DataFrame
		DataFrame containing game information from the specified tournament
	"""
	# Boolean Series that is True at indexes where the game is in the current tournament
	games_from_tournament_mask = generic_raw_game_df.loc[:,"tournament_url"] == url
	# Get a DataFrame of the games in the current tournament 
	games_from_tournament_df = generic_raw_game_df.loc[games_from_tournament_mask,:]
	return games_from_tournament_df

def index_new_raw_game_df(raw_games_from_tournament_df, raw_game_df, current_tournament_urls, tournament_url, max_game_id):
	"""
	Return a DataFrame with new raw game data that is properly indexed with game
	IDs

	This function takes a DataFrame containing games from a subset of tournament in the raw format,
	except the game IDs are wrong. It returns a DataFrame with suitable game_ids for the tournaments.
	The following rules are applide to get the correct game IDs:

	Parameters
	----------
	raw_games_from_tournament_df : pandas.DataFrame
		DataFrame containing all the games from a certain tournament in the raw format. This tournament
		includes at least one game where the data is different from what is currently saved on the database.
		The game IDs for the games are incorrect
	raw_game_df : pandas.DataFrame
		DataFrame representing the raw_game table in the database
	current_tournament_urls : set
		Set of all the tournament URLs in the raw_game table 
	tournament_url : string
		The URL for the tournament which all the games in the 'raw_games_from_tournament_df_unindexed' pandas.DataFrame
		come from
	max_game_id : int
		The maximum game ID currently in the raw_game table in the database. Note that this value may be different
		to the value in the table on MySQL, since it reflects the maximum game ID after considering the games that
		are going to be added to the database
	Returns
	-------
	raw_games_from_tournament_df_indexed : pandas.DataFrame
		DataFrame containing all the games from a certain tournament in the raw format. The game IDs are now
		suitable such that the DataFrame can be uploaded to the DataFrame
	new_max_game_id : int
		The maximum game_id that will be in the raw_game table after this DataFrame is added
	"""
	new_max_game_id = max_game_id
	# If the games are from a tournament that is in the database
	if tournament_url in current_tournament_urls:
		current_game_id_index = get_game_id_index(raw_game_df, tournament_url)
		# If the number of games are the same
		if len(raw_games_from_tournament_df) == len(current_game_id_index):
			raw_games_from_tournament_df.index = current_game_id_index
		# If there are more games for the tournament than the database currently has
		elif len(raw_games_from_tournament_df) > len(current_game_id_index):
			number_of_additional_games = len(raw_games_from_tournament_df) - len(current_game_id_index)
			new_games_index = pd.RangeIndex(start = max_game_id + 1, stop = max_game_id + 1 + number_of_additional_games)
			raw_games_from_tournament_df.index = current_game_id_index.union(new_games_index)
			new_max_game_id = max(raw_games_from_tournament_df.index)
			raw_games_from_tournament_df.index.name = "game_id"
		# If there are less games for the tournament than the database currently has
		else:
			raw_games_from_tournament_df.index = current_game_id_index[0:len(raw_games_from_tournament_df)]
			game_ids_to_delete = tuple(current_game_id_index[len(raw_games_from_tournament_df):])
			delete_from_raw_game(game_ids_to_delete)
			new_game_ids = set(raw_game_df.index) - set(game_ids_to_delete)
			new_max_game_id = max(new_game_ids)
	# If the games come from a tournament that is not in the database
	else:
		raw_games_from_tournament_df.index = pd.RangeIndex(start = max_game_id + 1, stop = max_game_id + 1 + len(raw_games_from_tournament_df), name = "game_id")
		new_max_game_id = max(raw_games_from_tournament_df.index)
	return raw_games_from_tournament_df, new_max_game_id

def get_game_id_index(raw_game_df, tournament_url):
	"""
	Get a range of game IDs for tournament specified by the tournaments URL

	Parameters
	----------
	raw_game_df : pandas.DataFrame
		Table representing the raw game table from the database 
	tournament_url : str
		The URL for the tournament which we want the game IDs for
	Returns
	-------
	game_id_index : pandas.NumericIndex
		Index object with the game_ids for the tournament specified by the URL
	"""
	# Get a boolean Series that is True at indexes where game occurs in the tournament specified
	# by the URL
	correct_tournament_mask = raw_game_df.loc[:,"tournament_url"] == tournament_url
	game_id_index = raw_game_df.index[correct_tournament_mask]
	return game_id_index

def get_new_game(latest_raw_game_df, raw_game_df):
	"""
	Get DataFrame with the new raw game data that is not currently in
	the database.

	This function takes a DataFrame containing all the games from
	"https://cuetracker.net/latest-matches" in raw format. It then returns a
	DataFrame with all the games from tournaments where at least one game was
	different. This is done because it it very hard to detect a new game.
	Instead, it is better to simply delete all the raw game in tournaments
	where some change has occured, and upload the latest data.

	Parameters
	----------
	latest_raw_game_df : pandas.DataFrame
		DataFrame with all the games from "https://cuetracker.net/latest-matches"
		in raw format
	raw_game_df : pandas.DataFrame
		DataFrame representing the current raw_game table in the database
	Returns
	-------
	new_raw_game_df : pandas.DataFrame 
		DataFrame with the NEW games from "https://cuetracker.net/latest-matches"
		in raw format. Games are considered new if they are in a tournament 
		where at least one change occured to the match data.
	"""
	# Get a DataFrame with new raw game data that is not currently in the database (without proper game IDs assigned)
	new_game_df_unindexed = get_new_game_unindexed(latest_raw_game_df, raw_game_df)
	db_distinct_tournament_urls = get_distinct_tournament_urls(raw_game_df)
	new_distinct_tournament_urls = get_distinct_tournament_urls(new_game_df_unindexed)
	max_game_id = max(raw_game_df.index)
	list_of_game_dfs = [] 
	# For each tournament
	for url in new_distinct_tournament_urls:
		games_from_tournament_df = get_games_by_tournament_url(new_game_df_unindexed, url)
		game_df, max_game_id = index_new_raw_game_df(games_from_tournament_df, raw_game_df, db_distinct_tournament_urls, url, max_game_id)
		list_of_game_dfs.append(game_df)
	if len(list_of_game_dfs) == 0:
		new_raw_game_df = pd.DataFrame(columns = raw_game_df.columns)
		new_raw_game_df.index.name = "game_id"
	else:
		new_raw_game_df = pd.concat(list_of_game_dfs, axis = 0)
	return new_raw_game_df

def get_new_raw():
	"""
	Return new raw data on the latest games and tournaments on CueTracker

	This function returns DataFrames with NEW raw data on the latest games and tournaments on CueTracker.
	The IDs for the games and tournaments have also been set to appropriate values so that the DataFrames
	can be appended to their respective raw tables, and so that they can be transformed into formatted tables.

	Returns
	-------
	new_raw_tournament_df : pandas.DataFrame
	new_raw_game_df : pandas.DataFrame
	"""
	# Get raw data on the latest games and tournaments
	latest_raw_game_df, latest_raw_tournament_df = cuetracker.get_latest_raw()
	raw_tournament_df = read_raw_tournament()
	# Get raw data on the latest new tournaments
	new_raw_tournament_df = get_new_tournament(latest_raw_tournament_df,raw_tournament_df)
	raw_game_df = read_raw_game()
	# Get raw data on the latest new games
	new_raw_game_df = get_new_game(latest_raw_game_df, raw_game_df)
	return new_raw_tournament_df, new_raw_game_df

def get_new_player(raw_game_df):
	"""
	Return DataFrame with players not currently in the database

	Parameters
	----------
	raw_game_df : pandas.DataFrame
		DataFrame with raw game data, which may include players not currently in the database

	Returns
	-------
	new_player_df : pandas.DataFrame
		DataFrame with new players that are not in the database
	"""
	player_df = player_transform.raw_game_to_player_transform(raw_game_df)
	database_player_df = read_player()
	new_player_df = get_new_rows(player_df, database_player_df, "url")
	return new_player_df

def update():
	"""
	Update the tournament, player and game tables in the snooker database
	"""
	print("Updating the CueTracker tables in the database (raw_tournament, raw_game, tournament, player and game)\n")
	new_raw_tournament_df, new_raw_game_df = get_new_raw()
	to_database(new_raw_tournament_df, "raw_tournament")
	to_database(new_raw_game_df, "raw_game")
	new_tournament_df = tournament_transform.raw_tournament_transform(new_raw_tournament_df)
	to_database(new_tournament_df, "tournament")
	new_player_df = get_new_player(new_raw_game_df)
	to_database(new_player_df, "player")
	tournament_df = read_tournament()
	player_df = read_player()
	new_game_df = game_transform.raw_game_transform(new_raw_game_df, tournament_df, player_df)
	to_database(new_game_df, "game")
	print("The CueTracker tables have been updated\n")


