import streamlit as st
# from model import ModelA
import database_engine as database_engine
import mli
import calc_server
import pandas as pd
import datetime


# Use Streamlit to display the DataFrame

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



def my_function():
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    st.write(f"Bets Refreshed at {current_time}")

# Streamlit app title
st.markdown("<h1 style='text-align: center;'>🚨 ☣️ ⚠️ Toxic Flow ⚠️ ☣️ 🚨</h1>", unsafe_allow_html=True)

# Streamlit app content
st.write("Execute these bets to introduce toxic flow to the markets")


# Add a button to the app
if st.button("Refresh Bets"):
    # Call the function to get new bet suggestions
    bet_suggestions = give_me_actions()

    # Display the time of the last update
    my_function()

    # Display the new DataFrame
    st.dataframe(bet_suggestions)