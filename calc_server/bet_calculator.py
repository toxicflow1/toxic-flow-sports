import numpy as np
import pandas as pd
import difflib

import math
from math import comb

from .transform_data import add_columns, extract_relevant_odds, extract_live_players, extract_db_players
from .lay_calcs import calc_max_lay_odds
from .back_calcs import calc_min_back_odds
from .probability_calcs import calc_p1_frame_prob, calc_p2_frame_prob
from .probability_calcs import calc_game_prob
from .bet_selector import find_value, calculate_ev, narrow_actions

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

def calc_fair_odds(df):
    """Re-calc back min odds calculator for EV as 0"""

    df['P1_FAIR_BACK_ODDS'] = df.apply(lambda x: ((1)/x['p1_GW_prob']), axis=1).values
    df['P2_FAIR_BACK_ODDS'] = df.apply(lambda x: ((1)/x['p2_GW_prob']), axis=1).values

    df['P1_FAIR_LAY_ODDS'] = df['P2_FAIR_BACK_ODDS']
    df['P2_FAIR_LAY_ODDS'] = df['P1_FAIR_BACK_ODDS']

    return df

def append_fair_and_ev_odds(df, comm):

    df = calc_min_back_odds(df, comm)
    df = calc_max_lay_odds(df, comm)
    df = calc_fair_odds(df)

    return df

def append_bet_suggestions(df, p1_handicap, commission):

    p1_frame_win_prob = calc_p1_frame_prob(df)
    p2_frame_win_prob = calc_p2_frame_prob(df)

    # if not np.all(np.isclose(p1_frame_win_prob + p2_frame_win_prob, 1)):
    #     raise ValueError("Frame win probabilities do not sum to 1! Error!")

    # updating the cols of the df we ultimately return
    df['p1_frame_win_prob'] = p1_frame_win_prob
    df['p2_frame_win_prob'] = p2_frame_win_prob

    p1_GW_prob = calc_game_prob(df, p1_handicap)

    df['p1_GW_prob'] = p1_GW_prob
    df['p2_GW_prob'] = 1 - df['p1_GW_prob']

    df = append_fair_and_ev_odds(df, commission)

    return df



def return_bet_sizes(df, ev, commission, p1_handicap):
    """
    Calculates our EV for given matches, and the minimum odds we wish to place a bet at given the EV

    Parameters
    ----------
    df : pd.DataFrame
        a dataframe of today's upcoming matches
    ev : float
        a number specified by the user which directly determines the market odds needed for us to bet with this EV

    Returns
    -------
    df : pd.DataFrame
        the existing dataframe, now with our EV if price achieved and min price we want to bet at columns
    """

    df = add_columns(df, ev, p1_handicap)

    df['best_of'] = df['best_of'].fillna(1)

    #df = df[['date', "player_one_name", "player_one_id", "player_one_rating",  "player_two_name", "player_two_id", "player_two_rating"]]
   
    df.dropna(inplace=True)
    # df.fillna(0, inplace=True)

    df = append_bet_suggestions(df, p1_handicap, commission)

    return df


def describe_actions(df, ev):
    """This is the function which has our live market odds, and our fair values, and tells us what the EV is like and stake sizes etc
    Also tells us what actions to take to maximise the EV shown on screen
    
    The input is our df which are the live avaibale games, having gone through the checks to match db players, and with OUR fair values appended etc
    Then, we can iterate over this to find actions 
    And return a final dataframe which tells us which games to place which bets on!

    """
    
    bet_suggestions = pd.DataFrame(columns=['data', 'p1', 'p2', 'action', 'EV for unit stake/liability'])
    
    columns_to_check = ['p1_LIVE_best_BACK', 'p1_LIVE_best_LAY', 'p2_LIVE_best_BACK', 'p2_LIVE_best_LAY']
    df = df[~(df[columns_to_check] == 0.00).any(axis=1)]

    actions = []
    for idx, row in df.iterrows():

        print('change/ check EV...')
        action_set = find_value(row, EV=ev)

        exact_action = narrow_actions(action_set, row)

        actions.append(exact_action)

        # ev = calculate_ev(exact_action, row)

        print('APPEND THE VALUES (ESPECIALLY EV) CALC ABOVE TO THE BET_SUGGESTIONS DF.')
        # bet_suggestions.append()

    df['ACTIONS'] = actions
    return df


def find_matching_name(p1, database_players):
    matching_db_playername = difflib.get_close_matches(p1, database_players['player_one_name'])
    print()
    print('Matching Player Names from our DB - cycling through p1 names in our DB of games, seeing if P1 betfair names match')
    print(matching_db_playername)
    print()

    if len(matching_db_playername) > 1:
        print('2 matching player names. we need to either update database, if we are looking at old games, or only select first if we have future games too')
        

    if len(matching_db_playername) == 0:
        # perhaps player names are reveresed on betfair Vs our database
        matching_db_playername = difflib.get_close_matches(p1, database_players['player_two_name'])
        


    if matching_db_playername == []: # if we have no match, try again and reverse the string

        split_name = p1.split()
        reversed_name = split_name[1] + " " + split_name[0]

        matching_db_playername = difflib.get_close_matches(reversed_name, database_players['player_one_name'])

        if len(matching_db_playername) > 1:
            print('2 matching player names. we need to either update database, if we are looking at old games, or only select first if we have future games too')
            print(matching_db_playername)

        if len(matching_db_playername) == 0:
            # perhaps player names are reveresed on betfair Vs our database
            matching_db_playername = difflib.get_close_matches(reversed_name, database_players['player_two_name'])

    if matching_db_playername == []:
        match = False
    else:
        match = True

    return match



def generate_bets(market_odds, df, ev, commission, p1_handicap):
    
    betting_df = return_bet_sizes(df=df, ev=ev, commission=commission, p1_handicap=p1_handicap)

    betting_df = extract_relevant_odds(betting_df)

    # players names directly from Betfair website, as appears in our live-market odds connection
    betfair_game_players = extract_live_players(market_odds)

    # players name directly from our Database of (upcoming) games
    # we have already dropped the rows with NaN from the betting_df ; may be because we dont have player strength data
    database_players = extract_db_players(betting_df)
  
    # CHECK DB PLAYERS DATE == TODAYS DATE.

    matching_indexes = []
    live_game_odds_df = pd.DataFrame(columns=['p1_name','p2_name','p1_best_BACK', 'p1_best_LAY', 'p2_best_BACK', 'p2_best_LAY'])
    
    # -------------------------------------------
    # This whole for loop deals with getting the live games which match our players in our database (to ensure we select games which we have 
    # player data on). 
    # going to turn it into its own function

    # names may be mis-spelt between Database and Betfair, so find closest match
    for p1 in list(betfair_game_players['p1_name'].values):

        match = find_matching_name(p1, database_players)

        if match: # we have found a match
            matching_db_playername = p1 # set matching player name to BETFAIR NAME
        else: # no match
            continue

        # record the players LIVE best back, LIVE best lay to add to the betting df.
        # store these in a df, adds rows each iteration, then we just add it to the .loc after we do a .loc at the end
        live_game_odds = market_odds[ market_odds['p1_name'] == matching_db_playername][['p1_name', 'p2_name', 'p1_best_BACK', 'p1_best_LAY', 'p2_best_BACK', 'p2_best_LAY']]

        # live_game_odds_df = pd.concat([live_game_odds_df, live_game_odds], axis=0, ignore_index=True)

        matching_index_in_database = database_players[database_players['player_one_name'] == matching_db_playername].index.values

        if len(matching_index_in_database) != 0:
            live_game_odds_df = pd.concat([live_game_odds_df, live_game_odds], axis=0, ignore_index=True)
            matching_indexes.append(matching_index_in_database[0])
            print('-----LGODF------')
            print()
            print(live_game_odds_df)
            print()
            print('-----LGODF-^----')
   
    print()
    print()
    print(' -- betting df --')
    print(betting_df)
    print()
    print()

    print()
    print()
    print(matching_indexes)
    print()
    print()

    print()
    print()

    betfair_and_database_matching_games = betting_df.loc[matching_indexes]
    print(betfair_and_database_matching_games)
    print()
    print()

    merged_df = betfair_and_database_matching_games.merge(live_game_odds_df, left_on=["player_one_name", "player_two_name"],
                                      right_on=["p1_name", "p2_name"],
                                      how="inner")
    merged_df.drop(columns=["p1_name", "p2_name"], inplace=True)

    available_games = merged_df
    available_games.rename(columns={'p1_best_BACK':'p1_LIVE_best_BACK', 'p1_best_LAY':'p1_LIVE_best_LAY',
                            'p2_best_BACK':'p2_LIVE_best_BACK', 'p2_best_LAY':'p2_LIVE_best_LAY'}, inplace=True)

    bet_suggestions = describe_actions(available_games, ev)

    return bet_suggestions














