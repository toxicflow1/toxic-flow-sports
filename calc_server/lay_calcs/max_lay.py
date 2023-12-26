import pandas as pd

def calc_max_lay_odds(df, comm):
    """For a lay bet:
        EV = Model Lay Prob * C (Net if Bet Wins) - (1 - Model Lay Prob)(Payout if bet looses) (where C = 1 - commission)
        EV = C S p - (1-p)(L - 1) S where p = model LAY prob, S = Stake (we set to 1 for calcs), L = Lay Odds
        set S to 1 then: EV = C p - (1-p)(L-1) = p ( C + L - 1) + (1 - L)

        -> rearrange for L: L = (EV + p - 1 - pC)/(p - 1)
    """

    comm = 1 - comm
    #NOTE - switcheed probabilities for game win as a P1 lay occurs with probability p2_game_win
    df['P1_LAY_MAX_ODDS'] = df.apply(lambda x: (x['EV_if_achieved (%)']+ x['p2_GW_prob']*(1-comm) - 1)/(x['p2_GW_prob']-1), axis=1).values

    df['P2_LAY_MAX_ODDS'] = df.apply(lambda x: (x['EV_if_achieved (%)']+ x['p1_GW_prob']*(1-comm) - 1)/(x['p1_GW_prob']-1), axis=1).values
    return df