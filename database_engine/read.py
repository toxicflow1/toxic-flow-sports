"""
Module to retrieve information tables from the Database

This modules is used to retrieve information from the database. This can
either be a full table from the database, a specific piece of information
from the database
"""
import pandas as pd
import sqlalchemy
from .engine_config import SNOOKER_ENGINE

__all__ = ["read",
	"read_raw_tournament",
	"read_raw_game",
	"read_tournament",
	"read_player",
	"read_rating",
	"read_game",
	"read_game_player_filtered",
	"read_snookerorg_player",
	"read_upcoming_game",
	"get_upcoming_rating",
	"get_name_rating",
	"get_tables"]

def read(table_name):
	"""
	Read a table from the database into a DataFrame

	Parameters
	----------
	table_name : str
		The name of the table to read

	Returns
	-------
	df : pandas.DataFrame
		DataFrame representing the specified table
	"""
	if table_name == "raw_tournament":
		return read_raw_tournament()
	elif table_name == "raw_game":
		return read_raw_game()
	elif table_name == "tournament":
		return read_tournament()
	elif table_name == "player":
		return read_player()
	elif table_name == "game":
		return read_game()
	elif table_name == "rating":
		return read_rating()
	elif table_name == "snookerorg_player":
		return read_snookerorg_player()
	elif table_name == "upcoming_game":
		return read_upcoming_game()
	else:
		print(f"Failed to read table. There is no table in the snooker database called {table_name}.\n")
		return None
def read_raw_tournament():
	"""
	Read the raw_tournament table from the database into a DataFrame

	Returns
	-------
	raw_tournament_df : pandas.DataFrame
		DataFrame representing the raw tournament table.
	"""
	raw_tournament_df = pd.read_sql_table("raw_tournament", SNOOKER_ENGINE, index_col = "tournament_id")
	return raw_tournament_df

def read_raw_game(game_ids = None):
	"""
	Read the raw_game table from the database into a DataFrame

	Returns
	-------
	raw_game_df : pandas.DataFrame
		DataFrame representing the raw games csv file.
	"""
	if game_ids is None:
		raw_game_df = pd.read_sql_table("raw_game", SNOOKER_ENGINE, index_col = "game_id")
	else:
		if len(game_ids) == 1:
			mysql_query = f"SELECT * FROM raw_game WHERE game_id = {game_ids[0]}"
			raw_game_df = pd.read_sql_query(mysql_query, SNOOKER_ENGINE)
		else:
			mysql_query = f"SELECT * FROM raw_game WHERE game_id = {tuple(game_ids)}"
			raw_game_df = pd.read_sql_query(mysql_query, SNOOKER_ENGINE)
	return raw_game_df

def read_tournament():
	"""
	Read the tournament table from the database into a DataFrame

	Returns
	-------
	tournament_df : pandas.DataFrame
		DataFrame representing the tournaments table from the database
	"""
	date_columns = ['qualifying_start_date','qualifying_end_date','start_date','end_date']
	tournament_df = pd.read_sql_table("tournament", SNOOKER_ENGINE, index_col = "tournament_id", parse_dates = date_columns)
	return tournament_df

def read_player():
	"""
	Read the players table from the database into a DataFrame

	Returns
	-------
	player_df : pandas.DataFrame
		DataFrame representing the players table from the snooker database
	"""
	player_df = pd.read_sql_table("player", SNOOKER_ENGINE, index_col = "player_id")
	return player_df

def read_rating():
	"""
	Read the rating table from the database into a DataFrame
s
	Returns
	-------
	rating_df : pandas.DataFrame
		DataFrame representing the rating table from the snooker database
	"""
	rating_df = pd.read_sql_table("rating", SNOOKER_ENGINE, index_col = "player_id")
	return rating_df

def read_game():
	"""
	Read the game table from the database into a DataFrame

	Returns
	-------
	game_df : pandas.DataFrame
		DataFrame representing the game table from the database
	"""
	game_df = pd.read_sql_table("game", SNOOKER_ENGINE, index_col = "game_id", parse_dates = ["date"])
	return game_df

def read_game_player_filtered(last_played_filter = "2010-01-01", game_date_filter = "2010-01-01", minimum_games_filter = 10):
	"""
	Read the game and player table from the database into a DataFrame and filters the tables in the database for certain conditions

	This function reads the game and player table from the snooker database and filters them for certain conditions.
	The players which appear in the player DataFrame that is returned:
		- Have played more recently than a certain date
		- Have played more than a certain number of games after a certain date
	The games which appear in the game DataFrame that is returned:
		- Include two players which satisfy the recency and number of games conditions
		- Occur after a certain date

	Parameters
	----------
	last_played_filter : str, default: "2010-01-01"
		The date which all the players must have played on/after
	game_date_filter : str, default: "2010-01-01"
		The date which all games must occur on/after
	minimum_games_filter : int, default: 10
		The minimum number of games the player must play in

	Returns
	-------
	filtered_player_df : pandas.DataFrame
		DataFrame containing the player table from the database with the 3 filters applied
	filtered_game_df : pandas.DataFrame
		DataFrame containing the game table from the database with the 3 filters applied
	"""
	# Get all the players that last played a game on/after a certain date
	mysql_player_query = f"""SELECT p.player_id AS 'player_id', MAX(p.date) AS 'last_played'
							FROM (
							SELECT game.player_one_id AS 'player_id', game.date AS 'date'
							FROM game
							UNION 
							SELECT game.player_two_id AS 'player_id', game.date AS 'date'
							FROM game
							) AS p
							GROUP BY p.player_id
							HAVING last_played >= '{last_played_filter}'"""
	last_played_filtered_player_df = pd.read_sql_query(mysql_player_query, SNOOKER_ENGINE, index_col = "player_id", parse_dates = ["last_played"])
	# Get all the games that occured on/after a certain date
	mysql_game_query = f"SELECT * FROM game WHERE date >= '{game_date_filter}'"
	game_date_filtered_game_df = pd.read_sql_query(mysql_game_query, SNOOKER_ENGINE, index_col = "game_id", parse_dates = ["date"])
	# Only keep the games which are played by two players that appear in the last played filtered DataFrame
	lp_gd_filtered_df = generic_game_filter(last_played_filtered_player_df, game_date_filtered_game_df)
	# Add a total_games column to player DataFrame and 
	filtered_player_df, filtered_game_df = total_games_filter(last_played_filtered_player_df, lp_gd_filtered_df, minimum_games_filter)
	return filtered_player_df, filtered_game_df

def read_snookerorg_player():
	"""
	Read the snookerorg_player table from the database into a DataFrame

	Returns
	-------
	snookerorg_player_df : pandas.DataFrame
		DataFrame representing the snookerorg_player table from the database
	"""
	snookerorg_player_df = pd.read_sql_table("snookerorg_player", SNOOKER_ENGINE, index_col = "player_id")
	return snookerorg_player_df

def read_upcoming_game():
	"""
	Read the upcoming_game table from the database into a DataFrame

	Returns
	-------
	upcoming_game_df : pandas.DataFrame
		DataFrame representing the upcoming_game table from the database
	"""
	upcoming_game_df = pd.read_sql_table("upcoming_game", SNOOKER_ENGINE)
	return upcoming_game_df

def get_upcoming_rating():
	"""
	Get a table with the upcoming games and the ratings for the players in the
	upcoming games

	Returns
	-------
	upcoming_rating_df : pandas.DataFrame
	"""
	mysql_query ="""SELECT date,
					CONCAT(sp1.first_name, ' ', sp1.last_name) AS 'player_one_name',
					p1.player_id AS 'player_one_id',
					r1.rating AS 'player_one_rating',
					CONCAT(sp2.first_name,' ', sp2.last_name) AS 'player_two_name',
					p2.player_id AS 'player_two_id',
					r2.rating AS 'player_two_rating',
					best_of
					FROM upcoming_game
					LEFT JOIN snookerorg_player AS sp1
					ON upcoming_game.player_one_id = sp1.player_id
					LEFT JOIN snookerorg_player AS sp2
					ON upcoming_game.player_two_id = sp2.player_id
					LEFT JOIN player AS p1
					ON CONCAT(sp1.first_name,' ', sp1.last_name)
					= CONCAT(p1.first_name, ' ', p1.last_name)
					LEFT JOIN player AS p2
					ON CONCAT(sp2.first_name,' ', sp2.last_name)
					= CONCAT(p2.first_name, ' ', p2.last_name)
					LEFT JOIN rating AS r1
					ON p1.player_id = r1.player_id
					LEFT JOIN rating AS r2
					ON p2.player_id = r2.player_id
					ORDER BY upcoming_game.game_id ASC
					"""
	upcoming_rating_df = pd.read_sql_query(mysql_query, SNOOKER_ENGINE)
	return upcoming_rating_df

def get_name_rating():
	"""
	Get a dictionary containing player names and their ratings

	Returns
	-------
	player_rating_dict : dict of str:float
		Dictionary where the keys are player names and the values are player
		ratings
	"""
	mysql_query = """SELECT CONCAT(first_name, ' ', last_name),
					rating.rating
					FROM player
					LEFT JOIN rating ON player.player_id = rating.player_id"""
	player_rating_dict = pd.read_sql_query(mysql_query, SNOOKER_ENGINE)
	return player_rating_dict

def get_tables():
	"""
	Return a list of all the tables in the database

	Returns
	------
	list of str
		A list where each element is the name of a table in the snooker database
	"""
	metadata_obj = sqlalchemy.MetaData()
	metadata_obj.reflect(bind=SNOOKER_ENGINE)
	return list(metadata_obj.tables.keys())

def generic_game_filter(filtered_player_df, game_df):
	"""
	Returns games where both players appear in the filtered players DataFrame

	This function is used to remove games from a game DataFrame where the players involved do not appear in the
	player DataFrame that is passed as an argument. 
	For example, if the player DataFrame that is passed as an arugment only contains players who last played after 2010, 
	then this function would return a DataFrame containing games where both players last played after 2010.
	If one player last played after 2010 and one did not, then it would not include that game.
	Additionally, games where both players last played before 2010 would also not be included

	Parameters
	----------
	filtered_player_df : pandas.DataFrame
		DataFrame containing the players table from the database after some filter condition has been applied
		which removes some players
	game_df : pandas.DataFrame
		DataFrame containing the games table from the database

	Returns
	-------
	games_db_filtered_df : pandas.DataFrame
		DataFrame containing games where both players in the game appear in 'filtered_player_df' 
	"""
	# Make "Player ID" a column
	players_merger = filtered_player_df.reset_index()
	# Perform an inner join so only games where Player 1 appears in the players DataFrame remain
	m1 = pd.merge(game_df, players_merger.loc[:,"player_id"], how = "inner", left_on = "player_one_id", right_on = "player_id")
	m1.drop(columns = ["player_id"], inplace = True)
	# Perform an inner join so only games where Player 2 appears in the players DataFrame remain
	games_db_filtered_df = pd.merge(m1, players_merger.loc[:,"player_id"], how = "inner", left_on = "player_two_id", right_on = "player_id")
	games_db_filtered_df.drop(columns = ["player_id"], inplace = True)
	return games_db_filtered_df

def total_games_filter(player_df, game_df, minimum_games_filter):
	"""
	Returns the players and games after the minimum game filter has been applied

	This function takes DataFrame containing players and games as arguments. It then removes the players who have
	played in under 'min_games' games and removes games which involve one or more players who have played in
	under 'min_games' games.
	Note that the number of games is based on the games DataFrame that is passed as an argument. This DataFrame
	may already be filtered so that it does not include games before a certain date. In a case like this, players
	who played ages ago would get filtered out from this function, as non of there games would be in the games
	DataFrame that is passed as an argument

	Parameters 
	----------
	player_df : pandas.DataFrame
		DataFrame containing the players table from the database
	game_df : pandas.DataFrame
		DataFrame containing the games table from the database
	minimum_games_filter : int
		The minimum number of games a player must play to be included in the players and games DataFrames that are
		returnd

	Returns
	-------
	players_db_filtered_df : pandas.DataFrame
		DataFrame containing the players table from the database. Players who
		have played in less than 'min_games' are not included
	games_db_filtered_df : pandas.DataFrame
		DataFrame containing the games table from the database,
		Only games where both players played in more than 'min_games' are included
	"""
	# Add the "Total games" to the players DataFrame
	total_games_series = get_total_games(game_df)
	player_df_w_total_games = pd.merge(player_df, total_games_series, how = "left", left_index = True, right_index = True)
	# Boolean Series that is True at indexes where players have played less than 'min_games' games
	not_enough_games_mask = (player_df_w_total_games.loc[:,"total_games"] < minimum_games_filter)
	# Drop players who have not played enough games
	players_db_filtered_df = player_df_w_total_games.drop(player_df_w_total_games.loc[not_enough_games_mask,:].index)
	games_db_filtered_df = generic_game_filter(players_db_filtered_df, game_df)
	return players_db_filtered_df, games_db_filtered_df

def get_total_games(game_df):
	"""
	Get a Series containing the total number of games each player has played

	Parameters
	----------
	game_df : pandas.DataFrame
		DataFrame containing game data

	Returns
	-------
	total_games_series : pandas.Series
		Series containing the total number of games each player has played
	"""
	games_player_ids = pd.concat([game_df.loc[:,"player_one_id"], game_df.loc[:,"player_two_id"]], axis = 0)
	total_games_series = games_player_ids.value_counts()
	total_games_series.name = "total_games"
	total_games_series.index.name = "player_id"
	return total_games_series

