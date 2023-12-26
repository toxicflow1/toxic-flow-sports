import numpy as np

def calc_p1_frame_prob(df):
    player_strength_differential_vec = df['player_one_rating'].values - df['player_two_rating'].values
    p1_frame_win_prob = 1./(1+np.exp(-player_strength_differential_vec)) #TODO: check this formula

    return p1_frame_win_prob

def calc_p2_frame_prob(df):

    # NOTE: the negative sign for player 2 (subtract the ratings the other way around)
    player_strength_differential_vec = - df['player_one_rating'].values + df['player_two_rating'].values
    p2_frame_win_prob = 1./(1+np.exp(-player_strength_differential_vec)) #TODO: check this formula

    return p2_frame_win_prob

