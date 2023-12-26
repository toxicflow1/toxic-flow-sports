import pandas as pd

def add_columns(df, ev, p1_handicap):

    df[['p1_frame_win_prob', 'p2_frame_win_prob','P1_FAIR_BACK_ODDS', 'P2_FAIR_BACK_ODDS', 'P1_FAIR_LAY_ODDS', 
        'P2_FAIR_LAY_ODDS', 'EV_if_achieved (%)', 'P1_BACK_MIN_ODDS', 'P2_BACK_MIN_ODDS', 'P1_LAY_MAX_ODDS', 'P2_LAY_MAX_ODDS']] = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, ev, 0.0, 0.0, 0.0, 0.0]
    
    df['p1_handicap'] = p1_handicap

    return df

def extract_relevant_odds(df):

    df.reset_index(drop=True, inplace=True) #TODO: check print statemenbt below
    print('I think the reset index means we loose the columns with None date...')
    df = df[['date', 'player_one_name', 'player_two_name', 'P1_FAIR_BACK_ODDS', 'P2_FAIR_BACK_ODDS', 'P1_FAIR_LAY_ODDS', 'P2_FAIR_LAY_ODDS',
                                           'EV_if_achieved (%)',  'P1_BACK_MIN_ODDS',  'P2_BACK_MIN_ODDS', 'P1_LAY_MAX_ODDS',  'P2_LAY_MAX_ODDS',  'p1_handicap']]
    
    return df

def extract_live_players(df):
    live_players = df[['p1_name', 'p2_name']]
    return live_players

def extract_db_players(df):
    db_players = df[['date', 'player_one_name', 'player_two_name']]
    return db_players

