def find_value(pd_row, EV):
    # action = 'None'
    # if 1./ pd_row['P1_FAIR_BACK_ODDS'] > 1./pd_row['p1_LIVE_best_BACK']:
    #     action = 'Back P1 or Lay P2'
    # else:
    #     action = 'Lay P1 or Back P2'
    
    actions = []

    ##model_p1_win = pd_row['p1_GW_prob']
    ##model_p2_win = pd_row['p2_GW_prob']

    model_p1_win = 1./pd_row['P1_FAIR_BACK_ODDS']
    model_p2_win = 1./pd_row['P2_FAIR_BACK_ODDS']

    if model_p1_win > (EV + 1)/pd_row['p1_LIVE_best_BACK']:
        actions.append('BACK_P1')

    if model_p1_win < ( 1 - EV*(pd_row['p1_LIVE_best_LAY'] - 1) )/pd_row['p1_LIVE_best_LAY']:
        actions.append('LAY_P1')

    if model_p2_win > (EV + 1)/pd_row['p2_LIVE_best_BACK']:
        actions.append('BACK_P2')

    if model_p2_win < ( 1 - EV*(pd_row['p2_LIVE_best_LAY'] - 1) )/pd_row['p2_LIVE_best_LAY']:
        actions.append('LAY_P2')

    return actions

def narrow_actions(action_set, row): # old argument row used to be df
    "Function to maximise our profit"
    # if action_set == 'Back P1 or Lay P2':
    #     if 1./df['p1_LIVE_best_BACK'] + 1./df['p2_LIVE_best_LAY'] < 1:
    #         final_action = 'BACK_P1'
    #     else:
    #         final_action = 'LAY_P2'
    # if action_set == 'Lay P1 or Back P2':
    #     if 1./df['p1_LIVE_best_LAY'] + 1./df['p2_LIVE_best_BACK'] > 1:
    #         final_action = 'LAY_P1'
    #     else:
    #         final_action = 'BACK_P2'

    # THIS ACTION

    # find which action, if multiple, has max EV
    if len(action_set) > 1:
        
        max_ev = 0
        action_with_max_ev = ''

        for action in action_set:
            ev = calculate_ev(action, row)
            if ev > max_ev:
                max_ev = ev
                action_with_max_ev = action
    

        final_action = action_with_max_ev

    elif len(action_set) == 1:
        final_action = action_set[0]
    else:
        final_action = 'NOTHING'

    return final_action


def calculate_ev(exact_action, row):
    # ev = prob_bet_winning * payout + prob_bet_loosing * loss
    
    ##model_p1_win = pd_row['p1_GW_prob']
    ##model_p2_win = pd_row['p2_GW_prob']

    model_p1_win = 1./row['P1_FAIR_BACK_ODDS']
    model_p2_win = 1./row['P2_FAIR_BACK_ODDS']
    
    if exact_action == 'BACK_P1':
        p_win = 1./row['P1_FAIR_BACK_ODDS'] # this is OUR model odds
        p_loss = 1 - p_win
        ev = p_win * (row['p1_LIVE_best_BACK'] -1) - p_loss

    if exact_action == 'LAY_P1':
        #p_win = 1. / row['P1_FAIR_LAY_ODDS'] # this is OUR model odds
        #p_loss = 1 - p_win
        #ev = p_win + p_loss
        ev = (1 - model_p1_win)/(row['p1_LIVE_best_LAY'] - 1) - model_p1_win

    if exact_action == 'BACK_P2':
        print('Hi! Check me!!')
        p_win = 1. / row['P2_FAIR_BACK_ODDS'] # this is OUR model odds
        p_loss = 1 - p_win
        ev = p_win * (row['p2_LIVE_best_BACK'] - 1) - p_loss

    if exact_action == 'LAY_P2':
        p_win = 1. / row['P2_FAIR_LAY_ODDS'] # this is OUR model odds - this is what 
        p_loss = 1 - p_win
        # ev = p_win + p_loss

        ev = (1 - model_p2_win)/(row['p2_LIVE_best_LAY'] - 1) - model_p2_win

    return ev

