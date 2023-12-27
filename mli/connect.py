import requests
import json, datetime, urllib, urllib.request, urllib.error, requests
import pandas as pd
import certifi, ssl
import streamlit as st


pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

def connect_API(username, password, app_key):
    """
    Connects to the Betfair API and obtains a session token (SSOID).

    Parameters
    ----------
    username : str
        Betfair account username.
    password : str
        Betfair account password.
    app_key : str
        Betfair application key obtained when registering an app.

    Returns
    -------
    SSOID : str or None
        Session token (SSOID) if the connection is successful, otherwise None.
    """

    payload = 'username=' + username + '&password=' + password
    headers = {'X-Application': app_key, 'Content-Type': 'application/x-www-form-urlencoded'}

    certs = (st.secrets["TLS_CERTIFICATE_PATH"], st.secrets["TLS_KEY_PATH"])

    resp = requests.post('https://identitysso-cert.betfair.com/api/certlogin',data=payload,
                        #  cert=('../api-ng-ssl/client-2048.crt',
                        #        '../api-ng-ssl/client-2048.key'),
                         cert=certs,
                         headers=headers)

    if resp.status_code == 200:

        resp_json = resp.json()

        print('---------------------------------')
        print('STATUS: {}'.format(resp_json['loginStatus']))
        print(resp_json)
        print('Session Token: {}'.format(resp_json['sessionToken']))
        print('---------------------------------')
    else:

        print('!!!!!!!!!!!!!!!')
        print("Request failed.")
        print('!!!!!!!!!!!!!!!')
        print()
        print('Check your PASSWORD VARIABLE is up to date')
        print()
    # json_resp=resp.json()

    SSOID = resp_json['sessionToken']

    return SSOID

def extract_in_play_markets_and_ids(bet_url, my_app_key, session_SSOID):
    """
    Extracts in-play markets and their IDs from the Betfair API.

    Parameters
    ----------
    bet_url : str
        The URL for the Betfair API endpoint.
    my_app_key : str
        Betfair application key obtained when registering an app.
    session_SSOID : str
        Session token (SSOID) obtained through authentication.

    Returns
    -------
    sport_id_dict : dict
        A dictionary mapping event type names to their corresponding event type IDs.
    """

    event_req = '{"jsonrpc": "2.0", "method": "SportsAPING/v1.0/listEventTypes", "params": {"filter":{ }}, "id": 1}'
    headers = {'X-Application': my_app_key, 'X-Authentication': session_SSOID, 'content-type': 'application/json'}
    req = requests.post(bet_url, data=event_req.encode('utf-8'), headers=headers)
    eventTypes = req.json()

    sport_id_dict = {sport['eventType']['name']:sport['eventType']['id'] for sport in eventTypes['result']}

    return sport_id_dict

def extract_sport_id(sport_dict, sport_name):
    """
    Extracts the ID of a sport from a dictionary of Betfair sport mappings.

    Parameters
    ----------
    sport_dict : dict
        A dictionary containing sport name to ID mappings.
    sport_name : str
        The name of the sport for which you want to extract the ID.

    Returns
    -------
    id : str
        The ID of the specified sport.
    """

    id = sport_dict[sport_name]
    return id

def extract_available_marketTypes(bet_url, sport_id, my_app_key, session_SSOID):
    """
    Extracts available market types (controlled ny "method" of market request) for a specific sport from the Betfair API. MarketType 
    means MatchOdds, OutrightWinner, ... etc

    Parameters
    ----------
    bet_url : str
        The URL for the Betfair API endpoint.
    sport_id : str
        The ID of the sport for which market types are to be extracted.
    my_app_key : str
        Betfair application key obtained when registering an app.
    session_SSOID : str
        Session token (SSOID) obtained through authentication.

    Returns
    -------
    MarketTypes : dict
        A dictionary containing available market types for the specified sport.
    """

    market_req = '{"jsonrpc": "2.0", "method": "SportsAPING/v1.0/listMarketTypes", "params": {"filter":{"eventTypeIds":["'+sport_id+'"]}}, "id":'+sport_id+'}'
    headers = {'X-Application': my_app_key, 'X-Authentication': session_SSOID, 'content-type': 'application/json'} #REMEMBER: use headers with the SSOID

    req = requests.post(bet_url, data=market_req.encode('utf-8'), headers=headers)
    MarketTypes = req.json()
    return MarketTypes

def marketCatalogue_OddsMarket(bet_url, market_type, sport_id, my_app_key, session_SSOID):
    """
    Retrieves market catalog data for a specific market type and sport from the Betfair API. Market Catalogue gives
    info on who or what teams are playing

    Parameters
    ----------
    bet_url : str
        The URL for the Betfair API endpoint.
    market_type : str
        The market type for which market catalog data is to be retrieved (e.g., 'MATCH_ODDS').
    sport_id : str
        The ID of the sport associated with the market.
    my_app_key : str
        Betfair application key obtained when registering an app.
    session_SSOID : str
        Session token (SSOID) obtained through authentication.

    Returns
    -------
    market_catalogue_df : pd.DataFrame
        A Pandas DataFrame containing market catalog data with columns 'p1', 'p2', 'p1_ID', 'p2_ID',
        representing runner names and their corresponding selection IDs.

    """
    market_type = str(market_type)

    if not market_type.isupper():
        raise ValueError('Make sure you write market_type as a UPPER CASE & DOUBLE QUOTED STR')

    MarketType = '["'+market_type +'"]'

    print()
    print('------------------------------------------------')
    print('Returning catalogue data for the markets associated with {}'.format(MarketType))
    print('-------------------------------------------------')
    print()

    #marketStartTime - restricts to markets with start times before or after teh specified date
    MarketStartTime = (datetime.datetime.now() - datetime.timedelta(hours=12)).strftime('%Y-%m-%dT%H:%M:%SZ')
    
    #marketendtime ie end time for which we want to gather data for markets starting in this interval
    MarketEndTime = (datetime.datetime.now() + datetime.timedelta(hours=24)).strftime('%Y-%m-%dT%H:%M:%SZ')

    headers = {'X-Application': my_app_key, 'X-Authentication': session_SSOID, 'content-type': 'application/json'} #REMEMBER: use headers with the SSOID

    # INPLAY ONLY
    # user_req = '{"jsonrpc": "2.0", "method": "SportsAPING/v1.0/listMarketCatalogue",\
    #            "params": {"filter":{"eventTypeIds":["'+str(sport_id)+'"],"marketTypeCodes":' + MarketType + ',\
    #             "inPlayOnly":"true",\
    #              "marketStartTime":{"from":"' + MarketStartTime + '", "to":"' + MarketEndTime + '"}},"sort":"FIRST_TO_START",\
    #            "maxResults":"1000", "marketProjection":["RUNNER_METADATA", "MARKET_START_TIME"]}, "id": 1}'

    # inPlayOnly filter not specified therefore we get both inplay and non-inplay markets starting in our interval!
    user_req = '{"jsonrpc": "2.0", "method": "SportsAPING/v1.0/listMarketCatalogue",\
                   "params": {"filter":{"eventTypeIds":["' + str(sport_id) + '"],"marketTypeCodes":' + MarketType + ',\
                     "marketStartTime":{"from":"' + MarketStartTime + '", "to":"' + MarketEndTime + '"}},"sort":"FIRST_TO_START",\
                   "maxResults":"1000", "marketProjection":["RUNNER_METADATA", "MARKET_START_TIME"]}, "id": 1}'

    ssl_context = ssl.create_default_context(cafile=certifi.where())
    req = urllib.request.Request(bet_url, data=user_req.encode('utf-8'), headers=headers)
    response = urllib.request.urlopen(req, context=ssl_context)
    jsonResponse = response.read()
    pkg = jsonResponse.decode('utf-8')
    market_catalogue = json.loads(pkg)

    dct_selection = {market['marketId']: [runner['selectionId'] for runner in market['runners']] for market in
               market_catalogue['result']}
    dct_names = {market['marketId']: [runner['runnerName'] for runner in market['runners']] for market in
                 market_catalogue['result']}
    market_id_dict = {k: dct_selection[k] + dct_names[k] for k in dct_selection.keys()}

    #market catalogue contains marketID (starts w a 1) which is essentially the ID of our market (game) of interest. Use this market id when we want to get odds
    #create a market catalogue df: index = market id | p1  (runnerName) | p2 (runenrName) | p1 ID (= selection ID) | p2 ID|

    market_catalogue_df = pd.DataFrame.from_dict(market_id_dict, orient='index', columns=['p1_ID', 'p2_ID', 'p1', 'p2'])
    market_catalogue_df = market_catalogue_df[['p1', 'p2', 'p1_ID', 'p2_ID']]
    market_catalogue_df.index.name = '{} marketId'.format(MarketType)

    return market_catalogue_df

def return_odds(bet_url, market_catalogue_df, my_app_key, session_SSOID):
    """
    Retrieves and returns odds data for player markets from the Betfair API.

    Parameters
    ----------
    bet_url : str
        The URL for the Betfair API endpoint.
    market_catalogue_df : pd.DataFrame
        A Pandas DataFrame containing market catalog data for player markets.
    my_app_key : str
        Betfair application key obtained when registering an app.
    session_SSOID : str
        Session token (SSOID) obtained through authentication.

    Returns
    -------
    market_odds_df : pd.DataFrame
        A Pandas DataFrame containing odds data for player markets. Columns include 'p1_name', 'p1_best_BACK',
        'p1_best_LAY', 'p2_name', 'p2_best_BACK', and 'p2_best_LAY'.

    """

    headers = {'X-Application': my_app_key, 'X-Authentication': session_SSOID,
               'content-type': 'application/json'}  # REMEMBER: use headers with the SSOID

    priceProjection = '["EX_BEST_OFFERS"]'

    marketIDList = list(market_catalogue_df.index)

    market_odds_df = pd.DataFrame(columns=['p1_name', 'p1_best_BACK', 'p1_best_LAY', 'p2_name', 'p2_best_BACK', 'p2_best_LAY'])
    for marketID in marketIDList: #loop through all markets
        odds_dict = {marketID:[]}
        p1_selection_id = market_catalogue_df.loc[str(marketID)]['p1_ID']
        p2_selection_id = market_catalogue_df.loc[str(marketID)]['p2_ID']

        for selectionId in [p1_selection_id, p2_selection_id]: #loop through player 1 and player 2 of each market
            price_req = '{"jsonrpc": "2.0", "method": "SportsAPING/v1.0/listRunnerBook", "params": {"locale":"en",\
                        "marketId":"' + str(marketID) + '", "selectionId":"' + str(selectionId) + '", ' \
                       '"priceProjection":{"priceData":' + str(priceProjection) + '},"orderProjection":"ALL"},"id":1}'

            req = urllib.request.Request(bet_url, data=price_req.encode('utf-8'), headers=headers)
            ssl_context = ssl.create_default_context(cafile=certifi.where())
            price_response = urllib.request.urlopen(req, context=ssl_context)
            jsonResponse = price_response.read()
            price_pkg = jsonResponse.decode('utf-8')
            market_odds = json.loads(price_pkg)

            # sometimes we cant back or lay a certain selection. Check that the len is > 0 to continue extracting the right data
            can_back, can_lay = False, False
            if len(market_odds['result'][0]['runners'][0]['ex']['availableToBack']) > 0:
                can_back = True
            if can_back:
                back_odds = market_odds['result'][0]['runners'][0]['ex']['availableToBack'][0]['price']
            else:
                back_odds = 0.0

            if len(market_odds['result'][0]['runners'][0]['ex']['availableToLay']) > 0: 
                can_lay = True
            if can_lay:
                lay_odds = market_odds['result'][0]['runners'][0]['ex']['availableToLay'][0]['price']
            else:
                lay_odds = 0.0

            player_name = market_catalogue_df.loc[str(marketID)]['p1' if selectionId == p1_selection_id else 'p2']

            odds_dict[marketID].append(player_name)
            odds_dict[marketID].append(back_odds)
            odds_dict[marketID].append(lay_odds)

        market_odds_df.loc[list(odds_dict.keys())[0]] = odds_dict[marketID]
    market_odds_df.index.name = market_catalogue_df.index.name

    # market_odds_df: (idx = [market type] marketId) || player 1 name | best back | best lay | player 2 name | best back | best lay
    # market_odds_df = pd.DataFrame(columns=['p1_name', 'p1_best_BACK', 'p1_best_LAY', 'p2_name', 'p2_best_BACK', 'p2_best_LAY'])

    return market_odds_df

def retrieve_current_odds_and_games(show_available_markets, marketType):
    """
    Retrieves current odds and market data for a specified sport and market type from the Betfair API.

    Parameters
    ----------
    show_available_markets : bool
        Whether to display available market types for the specified sport.
    marketType : str
        The market type for which odds and market data are to be retrieved (e.g., 'MATCH_ODDS').

    Returns
    -------
    pd.DataFrame or None
        A Pandas DataFrame containing odds data for player markets if show_available_markets is True,
        otherwise, None.
        
    """
    my_username = "z.tiller99@gmail.com"
    my_password = "T0x1cT0x1cT0x1c!"
    my_app_key = "JfojaSzPrIIwAGNj"

    session_SSOID = connect_API(my_username, my_password, my_app_key) # this changes each time we log in
    bet_url = "https://api.betfair.com/exchange/betting/json-rpc/v1"

    sport_id_dict = extract_in_play_markets_and_ids(bet_url, my_app_key, session_SSOID)
    snooker_id = extract_sport_id(sport_id_dict, sport_name='Snooker')
    if show_available_markets:
        MarketTypes = extract_available_marketTypes(bet_url, snooker_id, my_app_key, session_SSOID)
        print(MarketTypes)
    market_catalogue_df = marketCatalogue_OddsMarket(bet_url, marketType, snooker_id, my_app_key, session_SSOID)

    market_odds = return_odds(bet_url, market_catalogue_df, my_app_key, session_SSOID)
    print(market_odds)
    return market_odds

# if __name__=="__main__":

#     retrieve_current_odds_and_games(show_available_markets=True, marketType="MATCH_ODDS")
#     print()