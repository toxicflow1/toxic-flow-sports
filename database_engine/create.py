"""
Module for creating new tables
"""
from .engine_config import SNOOKER_ENGINE
from .write import to_database

__all__ = ["create_tournament_table",
"create_player_table",
"create_game_table",
"create_rating_table"]

def create_table(table_df, table_name, table_create_statement):
	"""
	Create a table in the database

	Parameters
	----------
	table_df : pandas.DataFrame
		DataFrame representing the table to create
	table_name : str
		The name of the table to create
	table_create_statement : str
		The MySQL statement to create the table
	"""
	print(f"Creating the {table_name} table in the snooker database...")
	SNOOKER_ENGINE.execute("SET FOREIGN_KEY_CHECKS = 0")
	SNOOKER_ENGINE.execute(f"DROP TABLE IF EXISTS {table_name}")
	SNOOKER_ENGINE.execute(table_create_statement)
	to_database(table_df, table_name)
	SNOOKER_ENGINE.execute("SET FOREIGN_KEY_CHECKS = 1")
	print(f"The {table_name} table was successfully created!\n")

def create_tournament_table(tournament_df):
	"""
	Create the tournament table in the database

	Parameters
	----------
	tournament_df : pandas.DataFrame
		DataFrame representing the tournament table
	"""
	tournament_create_statement = """CREATE TABLE tournament (
									tournament_id SMALLINT UNSIGNED,
									name VARCHAR(160),
									season CHAR(9),
									type VARCHAR(60),
									qualifying_start_date DATE,
									qualifying_end_date DATE,
									start_date DATE,
									end_date DATE,
									country VARCHAR(40),
									city VARCHAR(60),
									prize_fund_gbp MEDIUMINT UNSIGNED,
									url VARCHAR(200),
									CONSTRAINT pk_tournament PRIMARY KEY (tournament_id)
									)"""
	create_table(tournament_df, "tournament", tournament_create_statement)

def create_player_table(player_df):
	"""
	Create the player table in the database

	Parameters
	----------
	player_df : pandas.DataFrame
		DataFrame representing the player table
	"""
	player_create_statement = """CREATE TABLE player (
								player_id SMALLINT UNSIGNED,
								first_name VARCHAR(80),
								last_name VARCHAR(80),
								url VARCHAR(200),
								CONSTRAINT pk_player PRIMARY KEY (player_id)
								)"""
	create_table(player_df, "player", player_create_statement)

def create_game_table(game_df):
	"""
	Create the game table in the database

	Parameters
	----------
	game_df : pandas.DataFrame
		DataFrame representing the game table
	"""
	game_create_statement = """CREATE TABLE game (
								game_id MEDIUMINT UNSIGNED,
								date DATE,
								tournament_id SMALLINT UNSIGNED,
								round VARCHAR(40),
								player_one_id SMALLINT UNSIGNED,
								player_two_id SMALLINT UNSIGNED,
								player_one_frames SMALLINT UNSIGNED,
								player_two_frames SMALLINT UNSIGNED,
								best_of SMALLINT,
								CONSTRAINT pk_game PRIMARY KEY (game_id),
								CONSTRAINT fk_tournament_game FOREIGN KEY (tournament_id) REFERENCES tournament(tournament_id),
								CONSTRAINT fk_player_game_one FOREIGN KEY (player_one_id) REFERENCES player(player_id),
								CONSTRAINT fk_player_game_two FOREIGN KEY (player_two_id) REFERENCES player(player_id)
								)"""
	create_table(game_df, "game", game_create_statement)

def create_rating_table(rating_df):
	"""
	Create the rating table in the database

	Parameters
	----------
	rating_df : pandas.DataFrame
		DataFrame representing the rating table
	"""
	rating_create_statement = """CREATE TABLE rating (
								player_id SMALLINT UNSIGNED,
								rating FLOAT(7,5),
								CONSTRAINT fk_player_rating FOREIGN KEY (player_id) REFERENCES player(player_id)
								)"""
	create_table(rating_df, "rating", rating_create_statement)

def create_snookerorg_player_table(snoookerorg_player_df):
	"""
	Create the snookerorg_player table in the database

	Parameters
	----------
	snoookerorg_player_df : pandas.DataFrame
		DataFrame representing the player table
	"""
	player_create_statement = """CREATE TABLE snookerorg_player (
								player_id SMALLINT UNSIGNED,
								first_name VARCHAR(80),
								middle_name VARCHAR(80),
								last_name VARCHAR(80),
								date_of_birth DATE,
								turned_professional YEAR,
								nationality VARCHAR(50),
								CONSTRAINT pk_snookerorg_player
								PRIMARY KEY (player_id)
								)"""
	create_table(snoookerorg_player_df,
		"snookerorg_player",
		player_create_statement)

def create_upcoming_game_table(upcoming_game_df):
	"""
	Create the upcoming_game table in the database

	Parameters
	----------
	upcoming_game_df : pandas.DataFrames
		DataFrame representing the upcoming games table
	"""
	upcoming_create_statement = """CREATE TABLE upcoming_game (
									game_id SMALLINT UNSIGNED,
									date VARCHAR(80),
									player_one_id SMALLINT UNSIGNED,
									player_two_id SMALLINT UNSIGNED,
									best_of SMALLINT UNSIGNED,
									CONSTRAINT pk_upcoming_game
									PRIMARY KEY (game_id)
									)"""
	create_table(upcoming_game_df,
		"upcoming_game",
		upcoming_create_statement)




