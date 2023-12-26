"""
Module for transforming the raw player DataFrame into the table in database
format
"""
import pandas as pd
import re

GAME_COLUMNS = ["date","player_one_id","player_two_id","best_of"]

def url_transform(raw_game_df):
	"""
	Transform the player URLs into player IDs

	Parameters
	----------
	raw_game_df : pandas.DataFrame
		DataFrame wtih raw game data on upcomimng games

	Returns
	-------
	game_df : pandas.DataFrame
	"""
	p_one_id_df = pd.DataFrame(raw_game_df
									.loc[:,"player_one_url"]
									.str.extract("([0-9]+)")
									.values,
									columns = ["player_one_id"])
	p_two_id_df = pd.DataFrame(raw_game_df
									.loc[:,"player_two_url"]
									.str.extract("([0-9]+)")
									.values,
									columns = ["player_two_id"])
	game_df = pd.concat([raw_game_df
						.drop(columns = ["player_one_url",
						"player_two_url"]),
						p_one_id_df,
						p_two_id_df], axis = 1)
	return game_df

def raw_game_transform(raw_game_df):
	"""
	Transfrom a raw game DataFrame from snooker.org

	Parameters
	----------
	raw_game_df : pandas.DataFrame
		DataFrame wtih raw game data on upcomimng games
	"""
	raw_game_df.dropna(subset = ["player_one_url","player_two_url"],
						inplace = True)
	game_df = url_transform(raw_game_df)
	game_df.drop(columns = ["round","player_one_name","player_two_name"],
		inplace = True)
	game_df.index.name = "game_id"
	game_df.dropna(subset = ["player_one_id","player_two_id"],
						inplace = True)
	return game_df

