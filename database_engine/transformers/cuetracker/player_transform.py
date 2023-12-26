"""
This module is used to transform a raw games table into the players table that is used in to database

TODO:
Clean up docstrings and code
"""
import pandas as pd

PLAYER_COLUMNS = ["first_name","last_name","url"]

def games_to_players_transform(raw_game_df):
	"""
	Get a DataFrame with information on all the players based on a DataFrame 
	containing raw data from CueTracker on the games

	This function takes in a DataFrame containing raw game information from CueTracker and returns a
	DataFrame with information on all the players tha appear in the games

	Parameters
	----------
	raw_game_df : pandas.DataFrame
		DataFrame with raw data from CueTracker on all the games

	Returns
	-------
	raw_player_df : pandas.DataFrame
		DataFrame containing the names and links of all the players that appear in the games
			The links for the players are incorrect as they are links to season specific pages for the
		players
			The names are duplicated for players who have (Walkover) at the end of their name
			The names are in the 'name' column rather than split into 'first_name' and 'last_name' columns
	"""
	# Get a Series of all the players
	player_series = raw_game_df.loc[:,["player_one_name","player_two_name"]].stack()
	player_series.index = range(len(player_series))
	# Get a Series of all the links
	link_series = raw_game_df.loc[:,["player_one_url","player_two_url"]].stack()
	link_series.index = range(len(link_series))

	# Combine the players and links to form a DataFrame
	raw_player_df = pd.concat([player_series, link_series], axis = 1, keys = ["name","url"])
	return raw_player_df

def url_transform(raw_player_df):
	"""
	Transform the 'url' column in the players DataFrame

	This function transforms the 'url' column the DataFrame containing information on all the players.
	Originally the links are to specific seasons for each player. This removes the final part of the links 
	so the player links are to the general information page for the players

	Parameters
	----------
	raw_player_df : pandas.DataFrame
		DataFrame information on the player. 
			The urls for the players are incorrect as they are urls to season specific pages for the
		players
	Returns
	-------
	pandas.DataFrame
		DataFrame containing information on all the players. The urls for the players are correct
		in this DataFrame as the links are now to general player pages, rather than to season specific pages
	"""
	# Fix the URLS
	pattern = "(https://cuetracker.net/players/[a-zA-Z0-9.-]+)"
	raw_player_df.loc[:,"url"] = raw_player_df.loc[:,"url"].str.extract(pattern).values
	return raw_player_df

def walkover_transform(raw_player_df):
	"""
	Transform the 'name' column in the players DataFrame by removing (Walkover) from names

	This function transforms the 'name' column in the DataFrame containing information on all the players.
	Orginally the players DataFrame contains some players where their name ends in '(Walkover)'. This function
	removes (Walkover) from the players name

	Parameters
	----------
	raw_player_df : pandas.DataFrame
		DataFrame containing the names and links of all the players. Player names are duplicated 
		for players who have (Walkover) at the end of their name

	Returns
	-------
	pandas.DataFrame
		DataFrame containing information on all the players. (Walkover) has been removed from all player names
	"""
	walkover_mask = raw_player_df.loc[:,"name"].str.endswith("(Walkover)")
	raw_player_df.loc[walkover_mask,"name"] = raw_player_df.loc[walkover_mask,"name"].str[0:-10].values
	return raw_player_df

def name_transform(raw_player_df):
	"""
	Transfrom the 'Name' column in the players DataFrame by converting it into a 'First Name' and 'Last Name'
	column

	This function transforms the 'Name' column in the DataFrame containing information on all the players.
	Originally the names are stored in a single column called 'Name'. This functions splits the column
	into 2 parts: a first name and a last name. The first name is considered to first word in the name and the
	last name is considered as the remainder of the name (I think this is the same as the CueTracker standard)

	Parameters
	----------
	players_df : pandas.DataFrame
		DataFrame containing informaton on all the players
		Players names are stored in the 'Name' column rather than in seperate 'First Name' and 'Last Name'
		columns 

	Returns
	-------
	player_df : pandas.DataFrame
		DataFrame containing information on all the players. Player names are now stored in a 'First Name' and
		'Last Name' column

	See Also
	--------
	games_transform.name_transform(raw_game_df, player_number) : Transform the 'Player player_number Name' column 
	into a 'Player player_number First Name' and 'Player player_number Last Name' column
	"""
	# Create a Series of lists where each list has the name of a player
	raw_player_df.loc[:,"name"] = raw_player_df.loc[:,"name"].str.split()

	# Create a list of lists. Each inner list has a players first and last name
	player_name_list = []
	for i,player in enumerate(raw_player_df.loc[:,"name"].values):
		if len(player) == 0:
			print("There is a game which contains a player with no name")
			print(i)
		elif len(player) == 1:
			player_name_list.append([player[0], ''])
		elif len(player) == 2:
			player_name_list.append(player)
		else:
			first_name = player[0]
			last_name = ' '.join(player[1:])
			player_name_list.append([first_name, last_name])
	player_name_df = pd.DataFrame(player_name_list, columns = ["first_name","last_name"])
	raw_player_df.index = range(len(raw_player_df))

	player_df = pd.concat([player_name_df, raw_player_df.drop(columns = ["name"])], axis = 1)
	return player_df

def reindex_transform(player_df):
	"""
	Change the columns and index for the player DataFrame to database format

	This function takes a DataFrame containing all the player information that goes into the database does the following
	- Sort the players by last name
	- Change the index
	- Add the last_played column

	Parameters
	---------
	player_df : pandas.DataFrame
        DataFrame containing all the player information that will go into the DataBase. The last_played column
        is missing, the index is unamed and the index numbers are not a range starting from 0.

    Returns
    --------
    player_df_transformed : pandas.DataFrame
         DataFrame containing all the player information that will go into the DataBase 
	"""
	player_df_transformed = player_df.sort_values("last_name")
	player_df_transformed.index = pd.RangeIndex(stop=len(player_df_transformed), name = "player_id")
	return player_df_transformed

def raw_game_to_player_transform(raw_game_df):
	"""
	Transform a raw games DataFrame into a players DataFrame in database format

	This function takes a DataFrame with raw information about the games and converts this into
	a DataFrame that can be appended to the player table in the database.
	The following transformations are applied to the raw game DataFrame
	1. Create a DataFrame with information on all the players that appear in the raw game DataFrame
	2. Change the URL's so they are to player pages rather than to season specific player pages
	3. Drop players with the same URLs
	4. Remove Walkover from the end of the player's names
	5. Split the names into first names and last names

	Parameters
	----------
	raw_game_df : pandas.DataFrame
		DataFrame with raw data from CueTracker on all the games
		
	Returns
	-------
	player_df : pandas.DataFrame
		DataFrame containing the names and links of all the players in database format
	"""
	if raw_game_df.empty:
		print(f"There is no raw game DataFrame to transform to a player DataFrame.\n")
		player_df = pd.DataFrame(columns = PLAYER_COLUMNS)
		player_df.index.name = "player_id"
		return player_df
	number_of_rows, number_of_columns = raw_game_df.shape
	print(f"Transforming a {number_of_rows}x{number_of_columns} raw game DataFrame into a player DataFrame in database format...")
	raw_player_df = games_to_players_transform(raw_game_df)
	t1 = url_transform(raw_player_df)
	t1.drop_duplicates("url", inplace = True)
	t2 = walkover_transform(t1)
	t3 = name_transform(t2)
	player_df = reindex_transform(t3)
	print("Game to player transformations complete!\n")
	return player_df

