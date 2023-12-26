
import pandas as pd

def calc_min_back_odds(df, comm):
    """Populates P1_MIN_ODDS and P2_MIN_ODDS cols
    EV = Model Prob * (C x - C + 1) - 1 ; C = (1 - commission) ie user inputs 0.05 for comm, C = 0.95, x = payout if we win. live exchange odds

    -> rearrange for x, min odds to back at: ( (EV + 1)/p + C - 1 ) / (C)

    User specifies EV, have Model Prob therefore Offered Price = (EV + 1)/(Model Prob)

    Returns
        The populated 'Offered Price' column; For any price we see on the XChange above this, EV only increases
    """
    comm = 1-comm
    df['P1_BACK_MIN_ODDS'] = df.apply(lambda x: ( ((x['EV_if_achieved (%)']+1)/x['p1_GW_prob']) + comm - 1)/comm, axis=1).values

    df['P2_BACK_MIN_ODDS'] = df.apply(lambda x: ( ((x['EV_if_achieved (%)']+1)/x['p2_GW_prob']) + comm - 1)/comm, axis=1).values

    return df

