# MASTER is going to eventually be the script we call for scheduling etc...

from model import ModelA
import database_engine as database_engine
import mli
import calc_server

# This function is to be called by the schedule module once a day to update the model ratings
def daily_model_update():

    player_df, game_df = database_engine.read_game_player_filtered(last_played_filter="2012-01-01",
                                                                  game_date_filter="2012-01-01",
                                                                  minimum_games_filter=25)
    SnookerModel = ModelA(game_df, player_df)
    SnookerModel.fit_model(iterations=100_000)

    database_engine.update_rating(SnookerModel.optimised_ratings)

    return None


# This function is to be called every 30 minutes from 6am to 10 pm, to get the latest odds from the market & update our actions
def give_me_actions():

    # connect to the market
    market_odds = mli.retrieve_current_odds_and_games(show_available_markets=True, marketType="MATCH_ODDS")

    # pull down our best-guess player ratings
    upcoming_games_player_ratings = database_engine.get_upcoming_rating()

    # use the market and our player ratings to give us bet suggestions
    bet_suggestions = calc_server.generate_bets(market_odds, 
                                                upcoming_games_player_ratings,
                                                ev=0.03, 
                                                commission=0.05, 
                                                p1_handicap=0)
    
    return bet_suggestions






