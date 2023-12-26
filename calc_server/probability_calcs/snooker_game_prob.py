import numpy as np
import pandas as pd
import math
from math import comb

def calc_game_prob(df, p1_handicap):
    """Returns a vector for P1 GW prob, given P1 Frame Win Prob"""

    p1_GW_prob_vec = []

    for idx, row in df.iterrows():
        p1_frame_win_prob = row['p1_frame_win_prob']
        wins_to_win = math.ceil(row['best_of']/2)
        total_games = row['best_of']
        p1_game_win_prob = 0
        
        for winning_game in np.arange(wins_to_win, total_games+1):
            #(n-1)C(r-1) p^n (1-p)^(r)
            combi = comb(int(winning_game)-1, wins_to_win-1)
            failures = (1-p1_frame_win_prob)**(winning_game-wins_to_win)
            successes = (p1_frame_win_prob)**(wins_to_win)

            gw_prob = combi * failures * successes

            # if p1_handicap: # if we are interested in handicap market
            if p1_handicap <= 0:
                if 2*wins_to_win - winning_game > np.abs(p1_handicap): # then in this case, p1 wins so add the prob to p1_game_win_prob
                # if (winning_game -1) - (wins_to_win) > np.abs(p1_handicap):
                    p1_game_win_prob += gw_prob
                else: # else p1 looses for this score line with handicap applied so dont add the prob
                    p1_game_win_prob = p1_game_win_prob

            if p1_handicap > 0: #TODO:
                raise ValueError('HANDICAP > 0. CHECK. needs fixing')
                pass

        p1_GW_prob_vec.append(p1_game_win_prob)

    return p1_GW_prob_vec