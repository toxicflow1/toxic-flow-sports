"""
Module for transforming the raw player DataFrame into the table in database
format
"""
import pandas as pd

PLAYER_COLUMNS = ["first_name","middle_name",
"last_name","date_of_birth",
"turned_professional","nationality"]

def raw_player_transform(raw_player_df):
	"""
	Transform a raw player DataFrame 

	Parameters
	----------
	raw_player_df : pandas.DataFrame
		DataFrame with raw data on the players from snooker.org in raw format

	Returns
	-------
	player_df : pandas.DataFrame
		DataFrame containing data on the players from snooker.org in database
		format
	"""
	if raw_player_df.empty:
		print(f"There is no raw game DataFrame to transform to a player DataFrame.\n")
		player_df = pd.DataFrame(columns = PLAYER_COLUMNS)
		player_df.index.name = "player_id"
		return player_df
	t1 = raw_player_df.loc[:,["FirstName","MiddleName","LastName",
	"Nationality","FirstSeasonAsPro"]].copy()
	player_df = t1.rename(columns = 
	{"FirstName":"first_name",
	"MiddleName":"middle_name",
	"LastName":"last_name",
	"Born":"date_of_birth",
	"FirstSeasonAsPro":"turned_professional",
	"Nationality":"nationality"})
	return player_df



