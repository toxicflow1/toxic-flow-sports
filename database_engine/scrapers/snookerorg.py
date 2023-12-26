import requests
import bs4

import pandas as pd

from ..read import read_snookerorg_player

def get_raw_player():
	"""
	Return a DataFrame with raw data on all the players from snooker.org

	Returns
	-------
	raw_player_df : pandas.DataFrame
		DataFrame with raw data on all the players from snooker.org
	"""
	players_url = "https://api.snooker.org/?t=10&s=-1"
	response = requests.get(players_url)
	raw_player_df = pd.read_json(response.text)
	raw_player_df.set_index("ID",inplace = True)
	raw_player_df.index.name="player_id"
	return raw_player_df

def get_missing_player_ids():
	"""
	Get a set of player IDs which are not currently in the snookerorg_player table

	Returns
	------
	missing_ids : set
		Player IDs that are not in the snookerorg_player table that may need to be
		added
	"""
	snookerorg_player = read_snookerorg_player()
	player_ids = set(snookerorg_player.index)
	player_id_gap = max(player_ids) - len(player_ids)
	possible_player_ids = set(range(0,max(player_ids) + player_id_gap))
	missing_ids = possible_player_ids - player_ids
	return missing_ids

def get_single_player_df(player_id):
	"""
	Get a DataFrame with information on a player from snooker.org

	Parameters
	----------
	player_id : int
		The ID for the player that we want data for

	Returns
	-------
	single_raw_player_df : pandas.DataFrame
		DataFrame with data on the player with the specified ID
	"""
	url = f"https://api.snooker.org/?p={player_id}"
	response = requests.get(url)
	single_raw_player_df = pd.read_json(response.text)
	return single_raw_player_df


def get_new_raw_player():
	"""
	Return DataFrame with raw data on all new players from snooker.org

	Returns
	------
	raw_player_df : pandas.DataFrame or None
		DataFrame with raw data on all players from snooker.org that are not 
		currently in the snookerorg_player table.
	"""
	print("Searching for information on players not currently in the"
		+ " snookerorg_player table using the snooker.org API...")
	missing_ids = get_missing_player_ids()
	number_of_ids = len(missing_ids)
	print(f"There are {number_of_ids} new player IDs to try and find.")
	list_of_player_dfs = []
	count = 1
	for player_id in missing_ids:
			if count % 10 == 0:
				print(f"ID #{count} of {number_of_ids}: {player_id}")
			count += 1
			single_raw_player_df = get_single_player_df(player_id)
			if len(single_raw_player_df.columns) < 2:
				continue
			else:
				list_of_player_dfs.append(single_raw_player_df)
	print(f"Data on {len(list_of_player_dfs)} new players was found.\n")
	if len(list_of_player_dfs) == 0:
		return None
	raw_player_df = pd.concat(list_of_player_dfs, axis = 0)
	raw_player_df.set_index("ID", inplace = True)
	raw_player_df.index.name="player_id"
	return raw_player_df

def get_upcoming_game_tags():
	"""
	Get a list of tags which each contain all information about a game on
	snooker.org

	Returns
	-------
	tags : list of bs4.Tag
		A list of bs4.Tag objects where each element contains the information
		on a game
	"""
	upcoming_games_url = "https://www.snooker.org/res/index.asp?template=24"
	response = requests.get(upcoming_games_url)
	soup = bs4.BeautifulSoup(response.text, "html.parser")
	tags = soup.select("tr.gradeA.even.oneonone")
	return tags

def get_game_tag_dict(tag):
	"""
	Get a dictionary of tags which each contain one piece of information
	about a game on CueTracker

	Parameters
	----------
	tag : bs4.Tag
		bs4.Tag object containing all the information on a single game from
		snooker.org

	Returns
	-------
	game_tags_dictionary : dict of str:bs4.Tag
		Dictionary containing all the information one a single game on snooker.org
	"""
	date_tag = tag.find("td","scheduled editcell")
	round_best_of_tag = tag.find("td","round")
	round_tag = round_best_of_tag.find("a")
	best_of_tag = round_best_of_tag.find("span")
	player_one_name_tag, player_two_name_tag, *_ = tag.find_all("td","player")
	game_tags_dictionary = {
	"date":date_tag,
	"round":round_tag,
	"player_one_name":player_one_name_tag,
	"player_two_name":player_two_name_tag,
	"best_of":best_of_tag
	}
	return game_tags_dictionary

def game_tags_to_strings(game_tags_dictionary):
  """
  Retrive the text from the tags for a game

  Parameters
  ----------
  game_tags_dictionary : dict of str:bs4.Tag
    Dictionary containing all the information one a single game on CueTracker. The keys describe what the
    piece of information is and the values are bs4.Tag objects which contain the information

  Returns
  -------
  game_info_dict : dict of str:str
    Dictionary containing all the information one a single game on CueTracker. The keys describe what the
    piece of information is and the values contain the information
  """
  round_ = game_tags_dictionary["round"].getText(strip = True)
  player_one_name = game_tags_dictionary["player_one_name"].getText(strip = True)
  player_two_name = game_tags_dictionary["player_two_name"].getText(strip = True)

  if game_tags_dictionary["date"] is None:
  	date = None
  else:
  	date = game_tags_dictionary["date"].getText(strip = True)
  if game_tags_dictionary["best_of"] is None:
  	best_of = None
  else:
  	best_of = game_tags_dictionary["best_of"].getText(strip = True).lstrip("(").rstrip(")")
  if game_tags_dictionary["player_one_name"].find('a') is None:
  	player_one_url = None
  else:
  	player_one_url = game_tags_dictionary["player_one_name"].find('a')["href"]
  if game_tags_dictionary["player_two_name"].find('a') is None:
  	player_two_url = None
  else:
  	player_two_url = game_tags_dictionary["player_two_name"].find('a')["href"]

  game_info_dict = {
  "date":date,
  "round":round_,
  "player_one_name":player_one_name,
  "player_two_name":player_two_name,
  "best_of":best_of,
  "player_one_url":player_one_url,
  "player_two_url":player_two_url
  }
  return game_info_dict
  
def get_game_dict():
	"""
	Get all the relevant information on upcoming games from snooker.org

	Returns
	--------
	game_dict : dict of {str : list of str}
		Dictionary containing all the information on upcoming games. The keys
		specify the type of information and the values specify the information.
	"""
	game_tags = get_upcoming_game_tags()
	game_dict = {
	"date":[],
	"round":[],
	"player_one_name":[],
	"player_two_name":[],
	"best_of":[],
	"player_one_url":[],
	"player_two_url":[]
	}

	for tag in game_tags:
		game_tag_dict = get_game_tag_dict(tag)
		game_info_dict = game_tags_to_strings(game_tag_dict)

		game_dict["date"].append(game_info_dict["date"])
		game_dict["round"].append(game_info_dict["round"])
		game_dict["player_one_name"].append(game_info_dict["player_one_name"])
		game_dict["player_two_name"].append(game_info_dict["player_two_name"])
		game_dict["best_of"].append(game_info_dict["best_of"])
		game_dict["player_one_url"].append(game_info_dict["player_one_url"])
		game_dict["player_two_url"].append(game_info_dict["player_two_url"])
	return game_dict

def game_dict_to_df(game_dict):
	"""
	Transform a dictionary with data on games from snooker.org to a DataFrame

	Parameters
	----------
	game_dict : dict of {str : list of str}
		Dictionary containing all the information snooker.org games. The keys
	specify the type of information and the values specify the information

	Returns
	-------
	game_df : pandas.DataFrame
		DataFrame containing all the information the games in the tournamet
	"""
	game_df = pd.DataFrame(game_dict,
		columns = ["date","round","player_one_name","player_two_name","best_of",
		"player_one_url","player_two_url"])
	return game_df

def get_raw_upcoming_game():
	"""
	Return a DataFrame with raw data on the upcoming games from snooker.org
	"""
	print("Scraping data on the upcoming games from snooker.org...")
	game_dict = get_game_dict()
	game_df = game_dict_to_df(game_dict)
	print("Upcoming game data succesfully retrieved!\n")
	return game_df