"""
This module is used to turn a raw tournaments table into the format used for the tournament table in the database.

TODO:
Clean up docstrings and code
- Add a check to ensure the season for the tournament is valid
- Get a werid bug when trying to transform a 1x8 raw tournament table
"""
import datetime as dt
from dateutil.parser import parse

import pandas as pd
import numpy as np

# The columns for the tournament table
TOURNAMENT_COLUMNS = ["name","season","type","qualifying_start_date","qualifying_end_date",
"start_date","end_date","country","city","prize_fund_gbp","url"]
# The date columns in the tournament table
DATE_COLUMNS = ['qualifying_start_date','qualifying_end_date','start_date','end_date']

def is_iter(obj):
    """
    Check if an object is iterable
    
    Parameters
    ----------
    obj : object
        The object for which we are checking if it is iterable
    
    Returns
    -------
    bool
        Whether the input object is iterable
    """
    try:
        iter(obj)
        return True
    except:
        return False

def is_date(string):
	"""
	Check if a string is a date

	This function determines if a string is a date by looking for a 't' or 'd'.
	If there is a t, then assume the string is a date since it contains 1st, 2nd, 3rd, 4th etc...
	This condition is sufficient for detecting dates when the data comes from CueTracker however in general,
	this function will not work for detecting dates.

	Parameters
	----------
	string : str
		String to check if it is a date

	Returns:
	bool
		True if the string is a date, False is it is not a date
	"""
	if "t" in string or "d" in string:
	    return True
	else:
	    return False

def find_dates(lst):
	"""
	Get the elements from a list which are dates

	Parameters
	----------
	lst : list of str
		List to search for dates within

	Returns
	-------
	dates_list : list of str
		List containing the elements from the input list that are dates
	"""
	dates_list = []
	for date in lst:
		if is_date(date):
			dates_list.append(date)
	return dates_list

def get_list_of_dates(date_series):
	"""
	Get a list of lists containing the start date and end dates of tournaments

	This function takes in a Series where the values are lists of dates. The dates in the list of the dates on which the tournaments
	start and end. It then transforms this Series into a nested list where the inner lists contain the start date and end date for the tournament.
	This nested list can then be used to create a 2 DataFrame.
	The raw dates from CueTracker come in weird formats sometimes so some checks have to be done first in order to get the correct information. In some
	cases the dates will be None. In this case we want the inner list to be [None, None].
	In some cases the dates will be a single date. In this case we want to inner list to be [date, date] (the date repeated twice)
	In some cases the dates will be a start date, end date and some junk information. In this case we want to be able to filter out the junk and just have
	the dates

	Parameters
	----------
	date_series : pandas.Series
		A Series containing lists of dates on which tournamnets

	Returns
	-------
	list_of_dates : list of list of str
		A nested list containing the start dates and end dates of the tournaments
	"""
	list_of_dates = []
	for lst in date_series.values:
	    if is_iter(lst):
	        if len(lst) == 1:
	            list_of_dates.append([lst[0], lst[0]])
	        elif len(lst) == 2:
	            list_of_dates.append(lst)
	        else:
	            list_of_dates.append(find_dates(lst))
	    else:
	        list_of_dates.append([lst, lst])
	return list_of_dates 

def date_transform(raw_tournament_df):
	"""
	Transform the 'qualifying_dates' and 'dates' column from the CueTracker format to the database format

	This function transforms the 'qualifying_dates' and 'dates' column into 4 columns: 'qualifying_start_date','qualifying_end_date',
	'start_date' and 'end_date'.
    Initially, the raw date's columns consist of two dates seperated by a hyphen. This function splits the date's columns into a start
    date and end date column. It then returns the transformed DataFrame.

    Additionally, the 'qualifying_dates' column contains values of 'No qualifier dates available' in some places. This function also replaces
    these values with None.

    Parameters
    ----------
    raw_tournament_df : pandas.DataFrame
        DataFrame containing tournament information from CueTracker. The 'qualifying_dates' column is in the raw format
    
    Returns
    -------
    tournaments_df_transformed : pandas.DataFrame
        DataFrame containing the tournament information from CueTracker. It contains the columns 'qualifying_start_date' and
        'qualifying_end_date' columns instead of a 'qualifying_dates' column

    See Also
    --------
    dates_transform() : Transform the 'Dates' column from the CueTracker format to the database format
	"""
	tournaments_df_transformed = raw_tournament_df.replace({"No qualifier dates available":None})
	prefixes = ["","qualifying_"]
	# For each column (qualiying_dates and dates)
	for prefix in prefixes:
		# Get a Series of lists where each list contains a start date and end date
	    date_series = tournaments_df_transformed.loc[:,f"{prefix}dates"].str.split('-')
	    # Turn the Series into a list of dates
	    list_of_dates = get_list_of_dates(date_series)
	    # Create a DataFrame with all the tournament dates, with the dates split into two seperate columns
	    dates_df = pd.DataFrame(list_of_dates,
	    						columns = [f"{prefix}start_date",f"{prefix}end_date"],
	    						index = tournaments_df_transformed.index)
	    # Form the transformed DataFrame by dropping the original single date column and concatenating with the new DataFrame 
	    tournaments_df_transformed = pd.concat([tournaments_df_transformed.drop(columns =[f"{prefix}dates"]), dates_df],
	                    axis = 1)
	return tournaments_df_transformed

def invalid_date_finder(date_series):
	"""
	Return a Boolean Series that is True at indexes where the date for the tournament is an invalid format

	This function returns a boolean Series that is True at indexes where the the date series does not
	have a valid date. Valid dates are detected by looking for the regex pattern \d\w\w \d\d\d\d or 
	by seeing None. For example, if a date
	of December 27th 2022 is seen then the date with be valid, however if a date of December 27t 2022 is seen then
	the date will be classified as invalid.

	Parameters
	----------
	date_series : pandas.Series
		Series of dates. These dates may come from the following columns from the
		DataFrame containing information on the tournaments:
		Qualifying Start Date, Qualifying End Dates, Start Date, End Date

	Returns
	-------
	invalid_date_mask : pandas.Series
	    Series of booleans which are True at indexes where the date format is invalid
	"""
	not_null_mask = pd.notnull(date_series)
	not_null_series = date_series[not_null_mask]
	if len(not_null_series.values) > 0:
		invalid_date_mask = ~not_null_series.str.contains("\d\w\w \d\d\d\d")
		invalid_date_mask = invalid_date_mask.combine_first(not_null_mask)
	else:
		invalid_date_mask = not_null_mask
	return invalid_date_mask

def invalid_date_modify(invalid_tournament_df, date_column):
	"""
	Change the dates where the tournament date is invalid to Janunary 1st of that season.

	This function modifies one of the date columns in the tournament DataFrame inplace. It modifies all the entries
	in the specified column where the date is invalid. 
	A value is classified according to the invalid_date_finder() function

	Parameters
	----------
    invalid_tournament_df : pandas.DataFrame
        DataFrame containing the tournament information where the dates column may have invalid entries

	date_column : str
		The date column which is being modified

	See Also
	--------
	invalid_date_finder(date_series) : Return a Boolean Series that is True at indexes where the date for the tournament is an invalid format

	TODO
	----
	- Improve this function by being able to infer what date a CueTracker date was meant to be
	- Improve this function by changing the date of the match when a match is invalid to 
	"""
	# Boolean Series thatt is True at indexes where the date is invalid
	invalid_date_mask = invalid_date_finder(invalid_tournament_df.loc[:,date_column])
	year_series = invalid_tournament_df.loc[invalid_date_mask,"season"].str[0:4]
	invalid_tournament_df.loc[invalid_date_mask,date_column] = "January 1st " + year_series

def invalid_date_transfom(invalid_tournament_df):
	"""
	Transform invalid dates in the tournamnet DataFrame into the format %B %e %Y 

	This function transforms the 4 date columns in the tournament DataFrame ('qualifying_start_date',
	'qualifying_end_date','start_date' and 'end_date') into a format which allows the columns to be
	later converted into a Series of datetimes.

	Parameters
    ----------
    invalid_tournament_df : pandas.DataFrame
        DataFrame containing tournament information. The DataFrame must have 'qualifying_start_date',
	'qualifying_end_date','start_date' and 'end_date' as columns.
    
    Returns
    -------
    tournament_df : pandas.DataFrame
        DataFrame containing the tournament information. All the dates in the qualifying_start_date',
	'qualifying_end_date','start_date' and 'end_date' are of the format %B %e %Y
	"""
	# For each date column
	for column in DATE_COLUMNS:
		# Change the dates where the tournament date is 
		invalid_date_modify(invalid_tournament_df, column)
	tournament_df = invalid_tournament_df
	return tournament_df

def to_datetime_transform(tournaments_df):
	"""
	Transform the dates columns in the tournaments DataFrame to datetime columns.

	This function transforms the 'qualifying_start_date','qualifying_end_date','start_date' and 'end_date'
	from Series of strings to Series of datetime objects.

	Parameters
    ----------
    tournaments_df : pandas.DataFrame
        DataFrame containing tournament information where the dates columns have dates in string format
    
    Returns
    -------
    tournaments_df_transformed : pandas.DataFrame
        DataFrame containing the tournament information with the 'Qualifying Start Date','Qualifying End Date',
        'Start Date' and 'End Date' columns in datetime format
    """
	df = pd.to_datetime(tournaments_df.loc[:,DATE_COLUMNS].stack(dropna = False)).unstack()
	tournaments_df_transformed = pd.concat(
		[tournaments_df.drop(columns = DATE_COLUMNS), df],
		axis = 1)
	return tournaments_df_transformed

def date_order_transform(datetime_tournament_df):
	"""
	Transform DataFrame containing information about the tournaments so the start dates are always before or on
	the end dates

	This function transforms the columns containing date information in the DataFrame. It ensures that all
	start dates are either on or before end dates for both the qualifying dates and normal dates. 
	This function requires that the date columns are Series of datetime objects, rather than strings so that
	the comparison operators work properly on dates.

	Parameters
	----------
	datetime_tournament_df : pandas.DataFrame
        DataFrame containing tournament information.
        The four date columns must be in the DataFrame and they must be Series of datetime objects rather than
        strings

    Returns
    -------
    pandas.DataFrame
    	DataFrame containing tournament information. The date columns have been adjusted so that start dates
    	are always before or on end dates.
	"""
	# Boolean Series that is True at indexes where the start date comes after the end date
	wrong_start_mask = (datetime_tournament_df.loc[:,"start_date"] > datetime_tournament_df.loc[:,"end_date"])
	# Set the start date to None at indexes where the start date is after the end date
	datetime_tournament_df.loc[wrong_start_mask,"start_date"] = None
	# Fill those start dates with the end date of the tournament
	datetime_tournament_df.loc[:,"start_date"].fillna(datetime_tournament_df.loc[:,"end_date"], inplace = True)

	# Boolean Series that is True at indexes where the qualifying start date comes after the end date
	wrong_qual_start_mask = (datetime_tournament_df.loc[:,"qualifying_start_date"] > datetime_tournament_df.loc[:,"qualifying_end_date"])
	datetime_tournament_df.loc[wrong_qual_start_mask,"qualifying_start_date"] = datetime_tournament_df.loc[wrong_qual_start_mask,"qualifying_end_date"]
	return datetime_tournament_df

def location_transform(raw_tournament_df):
	"""
	Transform the 'location' column from the CueTracker format to the database format

	This function transforms the 'location' column in a DataFrame which contains raw data from CueTracker.
	The locations come in a single column and consist of a venue, a city and country (in some cases there is an extra piece
	of data such as that town). First the location column is split and then the city and country data is taken and put into the
	transformed DataFrame. The venue data is ignored.

	Parameters
	---------
	raw_tournament_df : pandas.DataFrame
        DataFrame containing tournament information from CueTracker where the 'location' column is in the raw format

    Returns
    --------
    tournament_df : pandas.DataFrame
        DataFrame containing the tournament information with 'city' and 'country' columns instead of a 'location' column
	"""
	# Get a Series of lists where each list contains the location of a tournament
	location_series = raw_tournament_df.loc[:,"location"].str.split(",")

	# Create a list of lists where each inner list contains the location of a tournament
	list_of_locations = []
	# For each location
	for lst in location_series.values:
		# Add the last two elements of the list to the list of locations (this ensures we always get city and country)
		# This is because sometimes the locations on CueTracker consist of 4 elements and sometimes they consist of 2,
		# but in all cases the country is the final element and the city is the penultimate element
		list_of_locations.append(lst[-2:])


	# Create a DataFrame with the locations
	locations_df = pd.DataFrame(list_of_locations,
		columns = ["city","country"],
		index = raw_tournament_df.index)

	raw_tournament_df.drop(columns = ["location"], inplace = True)
	tournament_df = pd.concat([raw_tournament_df, locations_df], axis = 1)
	return tournament_df

def prize_fund_remove_gbp(prize_fund_series):
	"""
	Remove the 'GBP ' prefix from all the values of a Series containing the prize funds of tournaments

	This function is used for transforming the raw data that is scraped from CueTracker into the format
	used in the database. 

	Parameters
	----------
	prize_fund_series : pandas.Series
		Series with prize fund data with a 'GBP ' prefix in front of all the values

	Return
	------
	prize_fund_series_transformed : pandas.Series
		Series with prize fund data with the 'GBP ' prefix removed
	"""
	prize_fund_series_transformed = prize_fund_series.str[4:]
	return prize_fund_series_transformed

def prize_fund_remove_commas(prize_fund_series):
	"""
	Remove commas from all the values of a Series containing the prize funds of tournaments.

	This function is used for transforming the raw data that is scraped from CueTracker into the format
	used in the database. It removes commas from the numbers and changes the type of the column from 
	object to np.int64

	Parameters
	----------
	prize_fund_series : pandas.Series
		Series with prize fund data with commas including in the numbers

	Return
	------
	prize_fund_series_transformed : pandas.Series
		Series with prize fund data with the commas removed from the numbers and 
	"""
	prize_fund_series_transformed = prize_fund_series.str.replace(',','').astype(np.int64)
	return prize_fund_series_transformed

def prize_fund_transform(tournaments_df):
	"""
	Transform the 'Prize Fund' from the CueTracker format to the database format

	This function takes a DataFrame which contains raw data from CueTracker and transforms
	the 'Prize Fund' column. The 'Prize Fund' column always is of the form 'GBP '. 
	This function 
		1. Removes 'GBP ' from the start of the columns
		2. Removes commas from the numbers
		3. Converts the type to np.int64

	Parameters
	---------
	tournaments_df : pandas.DataFrame
        DataFrame containing tournament information from CueTracker where the 'Prize Fund' column is in the raw format

    Returns
    --------
    tournaments_df_transformed : pandas.DataFrame
        DataFrame containing the tournament information with the 'Prize Fund/£' column instead of the raw 'Prize Fund' column
	"""
	prize_fund_series = tournaments_df.loc[:,"prize_fund"]
	pf1 = prize_fund_remove_gbp(prize_fund_series)
	pf2 = prize_fund_remove_commas(pf1)
	tournaments_df_transformed = pd.concat([tournaments_df.drop(columns = ["prize_fund"]),
		pf2], axis = 1)
	tournaments_df_transformed.rename(columns = {"prize_fund":"prize_fund_gbp"}, inplace = True)
	return tournaments_df_transformed

def reindex_transform(tournaments_df):
	"""
	Reorder the columns in the tournaments DataFrame to set it to the database format

	This function takes a DataFrame containing all the tournament information that goes into the database and
	simply reorders it

	Parameters
	---------
	tournaments_df : pandas.DataFrame
        DataFrame containing all the tournament information that will go into the DataBase except the columns are in the
        wrong order

    Returns
    --------
    tournaments_df_transformed : pandas.DataFrame
         DataFrame containing all the tournament information that will go into the DataBase where the columns have
         been reordered
	"""
	tournaments_df_transformed = tournaments_df.reindex(columns = TOURNAMENT_COLUMNS)
	tournaments_df_transformed.index.name = "tournament_id"
	return tournaments_df_transformed 

def raw_tournament_transform(raw_tournament_df):
    """
    Transform a DataFrame containing raw information about the tournaments into a format ready for the database

    This function takes a DataFrame with raw information about the tournaments. It then converts this 
    into a form ready for the database. The following transformations are applied to the dataframe
    - Split the 'dates' and 'qualifying_dates' columns into 'start_date','end_date','qualifying_start_date' and 'qualifying_end_date'
    - Check for invalid date formats and ensure start dates are before or on end dates
    - Split the 'location' column into 'city' and 'country'
    - Change the 'prize_fund' column so it is a Series of integers and so the units are in the heading
    Note that no change is made to the tournament ids during the transformations
        
    Parameters
    ----------
    raw_tournament_df : pandas.DataFrame
        DataFrame containing raw tournament information from CueTracker
    Returns
    --------
    tournament_df : pandas.DataFrame
         DataFrame containing all the tournament information that will go into the DataBase
    """
    if raw_tournament_df.empty:
    	print(f"There is no raw tournament DataFrame to transform to database format.\n")
    	tournament_df = pd.DataFrame(columns = TOURNAMENT_COLUMNS)
    	tournament_df.index.name = "tournament_id"
    	return tournament_df

    number_of_rows, number_of_columns = raw_tournament_df.shape
    print(f"Transforming a {number_of_rows}x{number_of_columns} raw tournament DataFrame into a tournament DataFrame in database format...")
    if raw_tournament_df.empty:
    	return pd.DataFrame(columns = TOURNAMENT_COLUMNS)
    t1 = date_transform(raw_tournament_df)
    t2 = invalid_date_transfom(t1)
    t3 = to_datetime_transform(t2)
    t4 = date_order_transform(t3)
    t5 = location_transform(t4)
    t6 = prize_fund_transform(t5)
    tournament_df = reindex_transform(t6)
    print("Tournament transformations complete!\n")
    return tournament_df



