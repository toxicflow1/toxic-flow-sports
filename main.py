import calc_server
import database_engine as database_engine
from model import ModelA
import mli

# code which loads from database

# player_df, game_df = database_engine.read_game_player_filtered(last_played_filter="2012-01-01",
#                                                                  game_date_filter="2012-01-01",
#                                                                  minimum_games_filter=25)

# # print('collected games')
# SnookerModel = ModelA(game_df, player_df)
# SnookerModel.fit_model(iterations=100_000)
# # SnookerModel.plot_LL_and_norm_evolution_tf()

# database_engine.update_rating(SnookerModel.optimised_ratings)

# code which makes predictions based on model, and upcoming games
market_odds = mli.retrieve_current_odds_and_games(show_available_markets=True, marketType="MATCH_ODDS")

# print('---------------')
# print()
# print()
# print()
# print()
# print()
# # print(market_odds)
# print()
# print()
# print()
# print()
# print('------------------')
print()
print()
print()
print()
print()
upcoming_games_player_ratings = database_engine.get_upcoming_rating()
print(upcoming_games_player_ratings)
print()
print()
print('------------------')
print('upcoming ratings retrieved ^^^ ')
print('------------------')
print()
print()
bet_suggestions = calc_server.generate_bets(market_odds, upcoming_games_player_ratings,
                                              ev=0.03, commission=0.05, p1_handicap=0)


print()
print()
print('------ final bet suggestions ------')
print()
print()
print(bet_suggestions)





