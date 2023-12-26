"""
This module is used to scrape raw tournament and game data from CueTracker.

This module can be used to either scrape all raw tournamnet and game data from
CueTracker, or some subset of the data. This module is purely for scraping raw 
data from CueTracker, therefore the data will NOT be clean. There may be missing
values, wrong values and values that don't make sense. The transform modules are
designed for handling the data cleaning.
"""
import requests
import bs4

import pandas as pd

import itertools
from pathlib import Path
import os
import shutil

from .helpers import page_downloader, fill_folder_html

# These are the general functions used for scraping data from CueTracker
def get_all_game_tags(response):
  """
  Get a list of tags which each contain all the information about a game on CueTracker

  This function returns a list of tags where each element in the list contains
  all the information about a game on CueTracker

  Paramters
  ---------
  response : requests.Response
    Response object which contains the webpage from a given CueTracker tournament URL

  Returns
  -------
  all_tags : list of bs4.Tag
    A list of bs4.Tag objects where each element contains the information on a game
  """
  soup = bs4.BeautifulSoup(response.text, "html.parser")
  even_game_tags = soup.find_all("div", class_ = "match row even")
  odd_game_tags = soup.find_all("div", class_ = "match row odd")
  all_tags = []
  for i in range(len(even_game_tags + odd_game_tags)):
      if i % 2 == 0:
          all_tags.append(even_game_tags[i//2])
      else:
          all_tags.append(odd_game_tags[i//2])
  return all_tags

def get_game_tag_dict(tag):
  """
  Get a dictionary with tags which each contain one piece of information
  about a game on CueTracker

  Parameters
  ----------
  tag : bs4.Tag
    bs4.Tag object containing all the information on a single game from CueTracker

  Returns
  -------
  game_tags_dictionary : dict of str:bs4.Tag
    Dictionary containing all the information one a single game on CueTracker. The keys describe the piece
    of information and the values are bs4.Tag objects which contain the information
  """
  date_tag = tag.find("div","col-12 played_on")
  round_tag = tag.find("div","col-md-12 round_name")
  player_one_name_tag = tag.find("div","player_1_name matchResultText mx-auto")
  player_two_name_tag = tag.find("div","player_2_name matchResultText mx-auto")
  player_one_frames_tag = tag.find("span","matchResultText text-nowrap float-left player_1_score")
  player_two_frames_tag = tag.find("span","matchResultText text-nowrap float-right player_2_score")
  best_of_tag = tag.find("span","best_of text-nowrap")

  game_tags_dictionary = {
  "date":date_tag,
  "round":round_tag,
  "player_one_name":player_one_name_tag,
  "player_two_name":player_two_name_tag,
  "player_one_frames":player_one_frames_tag,
  "player_two_frames":player_two_frames_tag,
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
  game_info_dictionary : dict of str:str
    Dictionary containing all the information one a single game on CueTracker. The keys describe what the
    piece of information is and the values contain the information
  """
  if game_tags_dictionary["date"] is None:
      date = None
  else:
      date = game_tags_dictionary["date"].getText(strip = True)
  round_ = game_tags_dictionary["round"].getText(strip = True)
  player_one_name = game_tags_dictionary["player_one_name"].getText(strip = True)
  player_two_name = game_tags_dictionary["player_two_name"].getText(strip = True)
  player_one_frames = game_tags_dictionary["player_one_frames"].getText(strip = True)
  player_two_frames = game_tags_dictionary["player_two_frames"].getText(strip = True)
  best_of = game_tags_dictionary["best_of"].getText(strip = True).lstrip("(").rstrip(")")

  player_one_url = game_tags_dictionary["player_one_name"].find('a')["href"]
  player_two_url = game_tags_dictionary["player_two_name"].find('a')["href"]

  game_info_dictionary = {
  "date":date,
  "round":round_,
  "player_one_name":player_one_name,
  "player_two_name":player_two_name,
  "player_one_frames":player_one_frames,
  "player_two_frames":player_two_frames,
  "best_of":best_of,
  "player_one_url":player_one_url,
  "player_two_url":player_two_url
  }
  return game_info_dictionary

def get_game_dict(response):
  """
  Get all the relevant game information from a requests.Response object

  This function returns a dictionary containing the information on all the games in a tournament.
  The tournament is specified by the CueTracker URL that is passed into the function as an argument

  Parameters
  ----------
  response : requests.Response
    Response object which contains the webpage from a given CueTracker tournament URL

  Returns
  --------
  game_dict : dict of {str : list of str}
    Dictionary containing all the information on the games in the tournament specified by 'url'. The keys specify the type of
    information and the values specify the information
  """
  # Get a list of tags which each contain information about a game on CueTracker
  game_tags = get_all_game_tags(response)
  game_dict = {
  "date":[],
  "round":[],
  "player_one_name":[],
  "player_two_name":[],
  "player_one_frames":[],
  "player_two_frames":[],
  "best_of":[],
  "player_one_url":[],
  "player_two_url":[]
  }
  for tag in game_tags:
    game_tags_dictionary = get_game_tag_dict(tag)
    game_info_dictionary = game_tags_to_strings(game_tags_dictionary)

    game_dict["date"].append(game_info_dictionary["date"])
    game_dict["round"].append(game_info_dictionary["round"])
    game_dict["player_one_name"].append(game_info_dictionary["player_one_name"])
    game_dict["player_two_name"].append(game_info_dictionary["player_two_name"])
    game_dict["player_one_frames"].append(game_info_dictionary["player_one_frames"])
    game_dict["player_two_frames"].append(game_info_dictionary["player_two_frames"])
    game_dict["best_of"].append(game_info_dictionary["best_of"])

    game_dict["player_one_url"].append(game_info_dictionary["player_one_url"])
    game_dict["player_two_url"].append(game_info_dictionary["player_two_url"])
  return game_dict

def get_tournament_dict(response):
  """
  Get all the relevant tournament information from a requests.Response object

  Parameters
  ----------
  response : requests.Response
    Response object which contains the webpage from a given CueTracker tournament URL
  tournament_data : dict of {str : str}

  Returns
  -------
  tournament_data : dict of {str : str}
    Dictionary containing all the information about the tournament on the webpage specified by the requests.Response object
    The keys specify the what the information is and the values specify the actual information on the tournament.
  """
  data = pd.read_html(response.text)
  df1 = data[0]
  df2 = data[2]
  tournament_data = {}
  tournament_data["type"] = df1.values[2,1]
  tournament_data["season"] = df1.values[3,1]
  tournament_data["dates"] = df1.values[4,1]
  tournament_data["qualifying_dates"] = df1.values[5,1]
  tournament_data["location"] = df1.values[6,1]
  tournament_data["prize_fund"] = df2.values[4,1]
  return tournament_data

def game_dict_to_df(game_dict, tournament_name, tournament_season, tournament_url):
  """
  Get all the relevant game information from a tournament as a DataFrame

  Parameters
  ---------
  game_dict : dict of {str : list of str}
    Dictionary containing all the information on the games in a tournament. The keys specify the type of
    information and the values specify the information
  tournament_name : str
    The name of the tournament
  season : str
    The season in which the tournament occured
  tournament_url : str
    The URL for the tournament
  Returns
  -------
  game_df : pandas.DataFrame
    DataFrame containing all the information the games in the tournamet
  """
  game_df = pd.DataFrame(game_dict,
  columns = ["date","round","player_one_name","player_two_name","player_one_frames",
  "player_two_frames","best_of","player_one_url","player_two_url"] )
  game_df.loc[:,"tournament_name"] = tournament_name
  game_df.loc[:,"tournament_season"] = tournament_season
  game_df.loc[:,"tournament_url"] = tournament_url
  return game_df

def tournament_dict_to_df(tournament_data, name, url):
  """
  Get all the relevant tournament information as a DataFrame

  Parameters
  ---------
  tournament_data : dict of {str : str}
    Dictionary containing all the information about a tournament that was scraped from the tournament's webpage
  name : str
    The name of the tournament
  url : str
    The URL for the tournament

  Returns
  -------
  tournament_df : pandas.DataFrame
    DataFrame containing all hte information about a tournament that was scraped from the tournament's webpage
  """
  tournament_df = pd.DataFrame(tournament_data, columns = ["season","type","dates","qualifying_dates","location","prize_fund"], index = [0])
  tournament_df.loc[:,"name"] = name
  tournament_df.loc[:,"url"] = url
  return tournament_df

def get_raw_game_tournament(tournament_urls):
  """
  Get two DataFrames with information on all the games and tournaments from a set of CueTracker tournament URLs 

  This function takes a dictionary containing tournamnet URLS as an input. It then loops through
  all the URLs in the dictionary and scrapes the data from the URL. I

  Parameters
  ----------
  tournament_urls : dict of str:str
    Dictionary containing all the URLs to the tournament pages on CueTracker. The keys of the dictionary are the URLs
    for the tournaments and the values are the names of the tournaments

  Returns
  -------
  raw_game : pandas.DataFrame
    DataFrame containing raw data on all the games from the tournament URLs
  raw_tournament : pandas.DataFrame
    DataFrame containing raw data on all the tournaments from the tournament URLs 
  """
  print("Webscraping tournament URLs from CueTracker...")

  number_of_urls = len(tournament_urls)
  print(f"There are {number_of_urls} URLs to scrape.")
  five_percent = number_of_urls//20

  list_of_games_dfs = [] 
  list_of_tournament_dfs = []
  count = 1
  # For each tournament
  for tournament_url, tournament_name in tournament_urls.items():
    print(f"Tournament #{count} of {number_of_urls}: {tournament_name}")
    response = requests.get(tournament_url)
    # Get the game data
    game_dict = get_game_dict(response)
    # If there is game data for the tournament
    if len(game_dict["date"]) > 0:
      # Get the tournament data
      tournament_data = get_tournament_dict(response)
      tournament_df = tournament_dict_to_df(tournament_data, tournament_name, tournament_url)
      list_of_tournament_dfs.append(tournament_df)
      game_df = game_dict_to_df(game_dict, tournament_name, tournament_data["season"], tournament_url)
      list_of_games_dfs.append(game_df)
    count += 1
  raw_tournament = pd.concat(list_of_tournament_dfs, axis = 0)
  raw_tournament.index = range(len(raw_tournament))
  raw_tournament.index.name = "tournament_id"
  raw_game = pd.concat(list_of_games_dfs, axis = 0)
  raw_game.index = range(len(raw_game))
  raw_game.index.name = "game_id"
  game_number_of_rows = raw_game.shape[0]
  tournament_number_of_rows = raw_tournament.shape[0]
  print("Webscraping successful!")
  print(f"Data on {game_number_of_rows} matches and {tournament_number_of_rows} tournaments was scraped.\n")
  return raw_game, raw_tournament

# --- Latest CueTracker WebScraping Functions ---
# These are the functions used to get the latest data from CueTracker
def get_latest_games_df():
  """
  Get the table with all the latest games from "https://cuetracker.net/latest-matches"

  Returns
  -------
  latest_game_df : pandas.DataFrame
    The table from "https://cuetracker.net/latest-matches" containing information about the latest matches
  """
  latest_path = Path.cwd() / Path("Latest")
  latest_path.mkdir(exist_ok = True)
  os.chdir(latest_path)
  latest_games_url = "https://cuetracker.net/latest-matches"
  page_downloader(latest_games_url, "lastest_games")
  os.chdir(latest_path.parent)
  latest_game_df = pd.read_html("Latest/lastest_games.html", extract_links = "body")[0]
  shutil.rmtree(latest_path)
  return latest_game_df

def get_latest_tournaments(latest_game_df):
  """
  Get a dictionary with all the tournaments that appear in the table with the latest games

  Parameters
  ----------
  lastest_tournamnets_df : pandas.DataFrame
    The table from "https://cuetracker.net/latest-tournaments" containing information about the latest tournaments

  Returns
  -------
  tournaments_dict : dict of str:str
    Dictionary containing all the tournamnets that appear on the latest games page. The keys are the URLS for the
    tournaments and the values are the names of the tournaments
  """
  tournaments = set(latest_game_df.loc[:,"Tournament"].values)
  tournaments_dict = {url:name for name, url in tournaments}
  return tournaments_dict

def get_latest_raw():
  """
  Get two DataFrames with raw data on all the games and tournaments that appear on "https://cuetracker.net/latest-matches"

  Returns
  -------
  raw_latest_game_df : pandas.DataFrame
    DataFrame with all the games from "https://cuetracker.net/latest-matches" in raw format
  raw_latest_tournament_df : pandas.DataFrame
    DataFrame with all the tournaments from "https://cuetracker.net/latest-matches" in raw format
  """
  # Get the table with all the latest games from "https://cuetracker.net/latest-matches"
  latest_games_table = get_latest_games_df()
  # Get a dictionary with all the tournaments that appear in the table with the latest games
  tournaments_dict = get_latest_tournaments(latest_games_table)
  # Get two DataFrames with information on all the latest games and tournaments
  raw_latest_game_df, raw_latest_tournament_df = get_raw_game_tournament(tournaments_dict)
  return raw_latest_game_df, raw_latest_tournament_df

# --- Reset Functions ---
# These are the functions used to when performing a full restart of the database
def get_seasons_df():
  """
  Get the table from the url https://cuetracker.net/seasons which contains information an all the snooker seasons

  Returns
  -------
  seasons_df : pandas.DataFrame
    The table from the URL https://cuetracker.net/seasons which contains information an all the snooker seasons
  """
  seasons_url = "https://cuetracker.net/seasons"
  # Change the path to the Seasons folder
  seasons_path = Path.cwd() / Path("Seasons")
  seasons_path.mkdir(exist_ok = True)
  os.chdir(seasons_path)
  # Download the page to the Seasons folder and save it as seasons.html
  page_downloader(seasons_url,"seasons")
  # Get the table from the webpage
  seasons_df = pd.read_html("seasons.html")[0]
  # Change back to the path containing this Python script
  os.chdir(Path.cwd().parent)
  return seasons_df

def get_seasons_list():
  """
  Get a list of seasons where CueTracker has data for the snooker games

  This function returns a list of all the seasons where Cue Tracker has data for the 
  tournamnets. It does this by getting the table from the URL https://cuetracker.net/seasons
  and then extracting the seasons listed in the table

  Returns
  -------
  seasons : list of str
    A list of all the seasons where CueTracker has data for the tournaments
  """
  # Get the table from https://cuetracker.net/seasons 
  seasons_df = get_seasons_df()
  # Create a boolean Series which is True where no tournaments occured in a season
  no_tournament_mask = seasons_df.loc[:,"Total Tournaments"] == 0
  # Drop the rows where no tournments occured
  seasons_df.drop(index = seasons_df.loc[no_tournament_mask,:].index, inplace = True)
  # Get a list of all the seasons in the DataFrame
  seasons = list(seasons_df.loc[:,"Season"].values)
  return seasons

def get_end_urls():
  """
  Return a list of the end URLs for the "https://cuetracker.net/seasons" webpage

  Returns
  ------
  end_urls : list of str
    A list of the end URLs for the "https://cuetracker.net/seasons" webpage
  """
  # Get a list of all the seasons on CueTracker "https://cuetracker.net/seasons" webpage
  seasons = get_seasons_list()
  # Get the possible URL endings for the professional tournaments webpage and non-professional tournaments webpage for each season
  statuses = ["?status=professional","?status=non-professional"]
  # A list to store the possible URL endings for all the season webpages on CueTracker
  end_urls = []
  for season, status in itertools.product(seasons, statuses):
    end_urls.append(season + status)
  return end_urls

def fill_seasons_folder():
  """
  Fill the Seasons folder with all the season webpages
  """
  end_urls = get_end_urls()
  fill_folder_html("Seasons","https://cuetracker.net/seasons",end_urls)

def get_season_files():
  """
  Get a list of the HTML files in the Season folder which contain the webpages for each season on CueTracker

  This function returns a list of all the HTML files in the Season folder which contains webpages for each 
  season on CueTracker

  Returns
  -------
  season_files : list of str
    A list of all the HTML files in the Season folder
  """
  # Change to the folder Seasons folder
  seasons_path = Path.cwd() / Path("Seasons")
  os.chdir(seasons_path)
  # List the files in the folder Seasons folder
  all_files = os.listdir(seasons_path)
  season_files = []
  for file in all_files:
    if file.endswith("professional.html"):
      season_files.append(file)
  # Change the current working directory back to the programs directory
  os.chdir(seasons_path.parent)
  return season_files

def get_all_tournaments():
  """
  Get a dictionary with all the tournament names and URLs from the seasons folder

  This function returns a dictionary which contains all the name and URLs for the tournament pages on CueTracker.
  It finds these links by going through all the files in the html Seasons folder, reading the tables in the files,
  and extracting the tournament names and URLs from the table. When using this function ensure the html files in 
  the Seasons folder are up to date.

  Returns
  -------
  tournament_urls : dict of str:str
    Dictionary containing all the URLs to the tournament pages on CueTracker. The keys of the dictionary are the URLs
    for the tournaments and the values are the names of the tournaments

  Examples
  --------
  >>> get_tournament_urls()
  {...
  'https://cuetracker.net/tournaments/british-open/2021/4494':2021 British Open',
  'https://cuetracker.net/tournaments/champion-of-champions/2021/4613':'2021 Champion of Champions',
  'https://cuetracker.net/tournaments/championship-league/2021/4441','2021 Championship League'
  ...}
  """
  season_files = get_season_files()
  tournament_urls = {}
  seasons_path = Path.cwd() / Path("Seasons")
  os.chdir(seasons_path)
  for season_file in season_files:
    # Remove the .html extension to get the name of the season
    season = season_file[:-5]
    # Read the table containing all the tournaments from the season
    df = pd.read_html(season_file, extract_links = "body")[2]
    if df.empty == False:
      # Get the names of all the tournaments in the table
      tournamets = df.loc[:,"Tournament"].apply(lambda x: x[0]).values
      # Get the links to all the tournaments from the table
      links = df.loc[:,"Tournament"].apply(lambda x: x[-1]).values
      # Create a dictionary where the keys are the names of the tournaments and the values are the links to the tournaments
      temp_dict = dict(zip(links, tournamets))
      tournament_urls.update(temp_dict)
  os.chdir(seasons_path.parent)
  shutil.rmtree(seasons_path)
  return tournament_urls

def get_all_raw():
  """
  Get two DataFrames with all the raw data on all the games and tournaments on CueTracker

  Returns
  -------
  raw_game_df : pandas.DataFrame
    DataFrame with all the games from CueTracker in raw format
  raw_tournament_df : pandas.DataFrame
    DataFrame with all the tournaments from CueTracker in raw format
  """
  fill_seasons_folder()
  # Get a dictionary with all the tournament names and URLs from the seasons folder
  tournament_urls = get_all_tournaments()
  # Scrape all the tournament URLS to get all the game and tournament data
  raw_game, raw_tournament = get_raw_game_tournament(tournament_urls)
  return raw_game, raw_tournament




