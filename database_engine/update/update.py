"""
Module for updating the database with the latest information
"""

from ..create import create_rating_table

from . import snookerorg_update
from . import cuetracker_update

def update():
	"""
	"""
	print("---- UPDATING THE DATABASE WITH THE LATEST DATA ----\n")
	cuetracker_update.update()
	snookerorg_update.update()
	print("---- DATABASE UPDATE COMPLETE! ----\n")

def update_rating(rating_df):
	"""
	Update the rating table in the database with the latest ratings

	Parameters
	---------
	rating_df : pandas.DataFrame
		DataFrame with the player ratings. The index should contain player ids and is named 'player_id'
		The column should contain ratings and be named 'rating'
	"""
	create_rating_table(rating_df)