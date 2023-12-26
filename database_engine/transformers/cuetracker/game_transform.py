"""
This module is used to turn a raw games table into the games table that is used in the database.
The tournamnet table and the players table must be updated before a raw games table can be transformed.

TODO:
Improve the date transform function
Handling cases where the date for a game is after datetime.max? (invalid_year_modify)
"""
import pandas as pd
import numpy as np

import sys
import os
import datetime as dt

from . import player_transform

GAME_COLUMNS = ["date","tournament_id","round","player_one_id","player_two_id","player_one_frames","player_two_frames","best_of"]

def clean_raw_game(raw_game_df):
	"""
	Drop unnecessary columns from a DataFrame containing raw game data from CueTracker

	This function removes the 'player_one_name', 'player_two', 'tournament_name' and 'tournament_season'.
	These columns are not necessary since URLs are used to join the game DataFrame with the player and
	tournament DataFrames

	Parameters
	----------
	raw_game_df : pandas.DataFrame
		DataFrame containing raw game data from CueTracker

	Return
	------
	clean_raw_game_df : pandas.DataFrame
		DataFrame containing raw game data from CueTracker without the player name columns
	"""
	clean_raw_game_df = raw_game_df.drop(columns = ["player_one_name","player_two_name","tournament_name","tournament_season"])
	return clean_raw_game_df

def invalid_year_modify(game_tournament, year = 1906):
	"""
	Modify DataFrame at indexes where the game date is before 'year'

	This function modifies the input DataFrame in-place where the year given for the game is before 'year'.
	It sets the date of the game equal to the start date of the tournament. This is done to avoid errors
	when trying to convert the date column to a Series of datetime objects, since some of the years for the
	games are before datetime.min

	Parameters
	----------
	games_tournaments : pandas.DataFrame
		DataFrame with game data left joined with the tournament database table.
		The relevant columns present are ["Date","Start Date"].
		"Date" is the date for the game and "Start Date" is the start date for thr tournament.

	year : int
		The year where if a game occurs before this year, it is consdiered invalid
	"""
	# Get a Series of the game years
	year_series = game_tournament.loc[:,"date"].str.extract("(\d\d\d\d)").astype(np.int64)
	# Boolean Series that is true when the game year is before 1906
	invalid_year_mask = (year_series < year)
	# At indexes where the game year is before year, set the date of the game to the start date of the tournament
	game_tournament.loc[invalid_year_mask.values.reshape(-1), "date"] = None
	game_tournament.loc[:,"date"].fillna(game_tournament.loc[:,"start_date"], inplace = True)

def series_to_datetime(date_series):
	"""
	Convert argument to datetime

	This function converts a Series to a pandas datetime DataFrame. The column name for the DataFrame that
	is returned is "date'I tried to do this normally but kept getting weird errors, so this was the best
	makeshift way I could think of doing it.

	Parameters
	----------
	date_series : pandas.Series
		The Series to convert to datetime

	Returns
	-------
	datetime_df : pandas.DataFrame
		A one column DataFrame with datetime objects as values. The column is called "Date"

	See Also
	--------
	games_tournaments_to_datetime(games_tournaments) : Transform the 'Dates' column of the DataFrame with game and tournament date to datetime
	"""
	# Changing the date series to datetime was a mess. The workaround is this werid to csv stuff
	date_series.to_csv("lol.csv")
	date_series = pd.read_csv("lol.csv", index_col = 0)
	os.unlink("lol.csv")
	datetime_df = pd.to_datetime(date_series.stack(), format = "%Y-%m-%d").unstack()
	datetime_df.columns = ["date"]
	return datetime_df

def to_datetime_transform(game_tournament):
	"""
	Transform the 'date' column of the DataFrame with game and tournament date to a Series of datetime objects

	Parameters
	----------
	game_tournament : pandas.DataFrame
		DataFrame containing game information left joined with tournament information. The relevant columns is 'date'.
		The dates are in the raw formats after being scraped from CueTracker.

	Returns
	-------
	game_tournament_transformed : pandas.DataFrame
		DataFrame containing game infomration left joined with tournament information. The game dates have now
		been converted to datetime objects.

	See Also
	--------
	makeshift_series_to_datetime(date_series) : Convert argument to datetime
	"""
	# Get a Series of the game dates
	date_series = game_tournament.loc[:,"date"].str.extract("(\d\d\d\d-\d\d-\d\d)").astype(str)
	datetime_df = series_to_datetime(date_series)
	# Combine the fixed dates with the rest of the game_tournament DataFrame
	game_tournament_transformed = pd.concat([game_tournament.drop(columns = ["date"]) , datetime_df], axis = 1)
	return game_tournament_transformed

def future_date_modify(games_tournaments):
	"""
	Modify game dates where a game occurs in the future

	Parameters
	----------
	games_tournaments : pandas.DataFrame
		DataFrame containing game information left joined with tournament information. Some games may occur
		in the future. The relevant columns are ["Date","Qualifying Start Date","Start Date"]
	"""
	# Boolean Series true at indexes where the game occurs in the future
	future_game_mask = games_tournaments.loc[:,"date"] > dt.datetime.now()
	games_tournaments.loc[future_game_mask,"date"] = None
	games_tournaments.loc[:,"date"].fillna(games_tournaments.loc[:,"qualifying_start_date"].astype(str), inplace = True)
	games_tournaments.loc[:,"date"].fillna(games_tournaments.loc[:,"qualifying_end_date"].astype(str), inplace = True)

def date_transform(game_tournament):
	"""
	Fix the game dates in the DataFrame containing game and tournament data

	This function transforms a DataFrame containing game and tournament data so that the date column contains
	sensible values.
	The following rules are applied to game dates
	- If no game date is given, the game is assumed to have taken place on the start day of the tournament
	- If the game occured before 1906 (the earliest date for a CueTracker game) then the game is assumed to have taken 
	place on the start date of the tournament. This check is done to avoid any weird errors when game dates occur
	before datetime.min
	- If the game is in the future, it is assumed to have taken place on the start date of the tournament
		- If the start date is in the future, it is assumed to have taken place on the start of the qualifying date 
		- If that is also in the future, it is assumed to have taken place yesterday

	Parameters
	----------
	game_tournament : pandas.DataFrame
		DataFrame with game data left joined with the tournament database table.
		The relevant columns present are ["Date","Start Date","End Date"].
		"Date" is the date for the game
		"Start Date" and "End Date" refer to column dates
		The dates for the games may be invalid dates

	Returns
	-------
	game_tournament_transformed : pandas.DataFrame
		DataFrame with the game dates now all valid
	"""
	# If there is no game data, assume the game takes place on the first day of the tournament
	game_tournament.loc[:,"date"].fillna(game_tournament.loc[:,"start_date"].astype(str), inplace = True)
	# Modify DataFrame at indexes where the game date is before 'year'
	invalid_year_modify(game_tournament, year = 1906)
	# Modify the 'date' column so the dates are now datetime objects
	game_tournament_transformed = to_datetime_transform(game_tournament)
	# Modify game dates where a game occurs in the future
	future_date_modify(game_tournament_transformed)
	return game_tournament_transformed

def join_game_tournament(raw_game_df, tournament_df):
	"""
	Join the games DataFrame and the tournaments DataFrame

	This function left joins a raw game DataFrame with a tournament Dataframe.

	Parameters
	----------
	raw_game_df : pandas.DataFrame
		DataFrame containing game data from CueTracker without the player link columns

	tournament_df : pandas.DataFrame
		DataFrame containing the tournament table from the database

	Returns
	-------
	game_tournament : pandas.DataFrame
		DataFrame containing game data along with the tournament IDs
	"""
	# Get the relevant columns from the tournament table
	relevant_tournament_df = tournament_df.loc[:,["url","qualifying_start_date","qualifying_end_date","start_date","end_date"]]
	# Reset the index so that Tournament ID is now a column
	relevant_tournament_df.reset_index(inplace = True)
	# Combine the games and the tournaments DataFrames
	game_tournament = raw_game_df.reset_index().merge(relevant_tournament_df, left_on = "tournament_url", right_on = "url", how = "left").set_index("game_id")
	# Sort out the dates for the games
	game_tournament = date_transform(game_tournament)
	# Remove irrelevant columns
	game_tournament.drop(columns = ["tournament_url","url","qualifying_start_date","qualifying_end_date","start_date","end_date","url"],inplace= True)
	return game_tournament

def best_of_transform(raw_game_df):
	"""
	Modify the 'best_of' column in the game DataFrame so that it only contains valid entries

	This function modifies the values in the best_of column in a DataFrame containing the games inplace.
	It ensures the values are all valid. Valid is defined as being a whole number (this defintion may be 
	extended in the future)

	Parameters
	----------
	raw_game_df : pandas.DataFrame
		DataFrame containing game data from CueTracker. This DataFarme must include a 'best_of' column. The
		values in the best_of column may contain negative entries

	Return
	------
	game_df : pandas.DataFrame
		DataFrame containing game data from CueTracker. The values in the 'best_of' column have now been
		changed so that they are all valid

	TODO
	----
	At some point add additional checks to handle casees where:
	- the number of frames is more than best_of
	- the number of frames is less than best_of
	For this I may want to look at similar games (same tournament, same round) to be able to guess what the
	best_of is?
	"""
	game_df = raw_game_df.astype({"player_one_frames":np.int64,
		"player_two_frames":np.int64,
		"best_of":np.int64})
	negative_best_of_mask = game_df.loc[:,"best_of"] < 0
	game_df.loc[negative_best_of_mask,"best_of"] = 0
	return game_df

def url_transform(raw_game_df):
	"""
	Transform the URL

	Parameters
	----------
	raw_game_df : pandas.DataFrame
		DataFrame containing game data where the player names are kept in the columns 'Player 1 Name' and/or
		'Player 2 Name'

	Returns
	-------
	game_df_transformed : pandas.DataFrame
		DataFrame containing game data where for either player 1 or player 2, the player names are kept in 2 columns
		seperate columns. 'Player player_number First Name' and 'Player player_number Last Name'

	See Also
	---------
	player_transform.name_transform(player_df) : Transform a raw players DataFrame into the database format
	"""
	# Player 1 and 2
	for player_number in ["one","two"]:
		# Rename the player_one_name column to 'name' so it can be passed into the player_transform.name_transform() function
		raw_game_df.rename(columns = {f"player_{player_number}_url":"url"}, inplace = True)
		raw_game_df = player_transform.url_transform(raw_game_df)
		# Rename the columns back
		raw_game_df.rename(columns = {"url":f"player_{player_number}_url"},
							inplace = True)
	return raw_game_df

def join_game_player(game_df, player_df):
	"""
	Left join a DataFrame containing games with a DataFrame containing player names and their IDs
	on the column specified by 'player_number'

	This function joins a DataFrame containing games to a DataFrame containing information on all the players
	by joining on the player urls.

	Parameters
	----------
	game_df : pandas.DataFrame
		DataFrame containing games where one of columns specifying the player names is in the form
		'Player player_number First Name' and 'Player player_number Last Name'. The other column may be in
		the same form or may already by in the Player ID form.

	player_df : pandas.DataFrame
		DataFrame containing the players table from the database

	Returns
	-------
	pandas.DataFrame
		DataFrame containing games where the columns specifying one of the players is in the form
		'Player player_number ID'.  The other column may be in the 2 column form or the Player ID form.
	"""
	# Get the relevant columns from the players DataFrame
	relevant_player_df = player_df.loc[:,["url"]]
	# Reset the index so that player_id is now a column
	relevant_player_df.reset_index(inplace = True)
	raw_game_df = url_transform(game_df)
	# For each player number
	for player_number in ["one","two"]:
		# Join the DataFrames on the URLs
		game_df = game_df.reset_index().merge(relevant_player_df, left_on = f"player_{player_number}_url",
		right_on = "url", how = "left").set_index("game_id")
		# Drop the URLS from the DataFrame
		game_df.drop(columns = [f"player_{player_number}_url","url"], inplace = True)
		# Rename the ID column
		game_df.rename(columns = {"player_id":f"player_{player_number}_id"}, inplace = True)
	return game_df

def join_game_tournament_player(game_tournament, player_df):
	"""
	Join the games DataFrame where tournament ID is a column with the players table from the database

	Parameters
	----------
	game_tournament : pandas.DataFrame
		DataFrame containing game data along with the tournament IDs
	player_df : pandas.DataFrame
		DataFrame containing the players table from the database

	Returns
	-------
	game_df : pandas.DataFrame
		DataFrame containing the game data in the correct format for the games table in the DataBase
	"""
	# Transform the 'Player 1 Name' and 'Player 2 Name' columns into 4 columns:
	game_df = join_game_player(game_tournament, player_df)
	return game_df

def reindex_transform(game_df):
	"""
	Change the column and index in the game DataFrame to the database format

	Parameters
	---------
	game_df : pandas.DataFrame
		DataFrame containing game data. The index is not a range starting from 0 and
		the order of the columns is wrong

	Returns
	-------
	game_df_transformed : pandas.DataFrames
		DataFrame containing all information that will go into the Database
	"""
	game_df_transformed = game_df.reindex(columns = GAME_COLUMNS)
	game_df_transformed = game_df_transformed.astype({"player_one_frames":np.int64,
													  "player_two_frames":np.int64,
													  "best_of":np.int64})
	return game_df_transformed

def raw_game_transform(raw_game_df, tournament_df, player_df):
	"""
	Transform a DataFrame containing games into the database format

	This function takes a DataFrame with raw information about the games and converts it into a form
	ready for the game table in the database.
	 Note that no change is made to the game ids during the transformations.

	Parameters
	----------
	raw_game_df : pandas.DataFrame
		A DataFrame containing game data in the raw format after being scraped from CueTracker
	tournament_df : pandas.DataFrame
		DataFrame representing the tournament table from the database
	player_df : pandas.DataFrame
		DataFrame representing the player table for the database

	Returns
	-------
	game_df : pandas.DataFrame
		DataFrame with the game data in database format

	Raises
	------
	IndexError
		If the length the raw games DataFrame does not game the length of the database format games DataFrame.
		This usually happens due to duplicated entries in the player or tournament DataFrames messing up the joins
	"""
	if raw_game_df.empty:
		print(f"There is no raw game DataFrame to transform to database format.\n")
		game_df = pd.DataFrame(columns = GAME_COLUMNS)
		game_df.index.name = "game_id"
		return game_df
	number_of_rows, number_of_columns = raw_game_df.shape
	print(f"Transforming a {number_of_rows}x{number_of_columns} raw game DataFrame into a game DataFrame in database format...")
	clean_raw_game_df = clean_raw_game(raw_game_df)
	game_tournament_one = join_game_tournament(clean_raw_game_df, tournament_df)
	game_tournament_two = best_of_transform(game_tournament_one)
	game_tournament_player = join_game_player(game_tournament_two, player_df)
	game_df = reindex_transform(game_tournament_player)

	if len(raw_game_df) == len(game_df):
		print("Game transformations complete!\n")
		return game_df
	else:
		raise IndexError("""There was a problem transforming the raw games DataFrame into database format.
		The length of the new DataFrame does not game the length of the raw games DataFrame.""")






