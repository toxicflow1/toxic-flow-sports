# Upload new model ratings
import pandas as pd
import numpy as np
from math import comb
import tensorflow as tf
import matplotlib.pyplot as plt


class ModelA:
    """
    A class for the implementation of our adaptation of the Bradley-Terry Model, for sports
    in which a player is assigned a 'strength' variable, which is used to calculate the probability of a win
    for the sport in question

    Parameters
    ----------
    mahtch_df : pandas.DataFrame
        DataFrame containing the matches we wish to train the model on
    player_df : pandas.DataFrame
        DataFrame containing the players and their current (pre-trained or trained) model rating
    
    Attributes
    ----------
    prop_product : float
        A float storing the product likelihood of the games in the training set, for this Model object
    log_likelihood : float
        A float storing the log likelihood of the games in the training set, for this Model object
    match_data : pandas.DataFrame
    
    player_data : pandas.DataFrame

    optimised_ratings : 

    LL_evolution : list

    final_LL : float

    iter : list

    norm : list

    idx_mapping : dict
    
    Methods
    -------
    method1(arg1)
        Perform a specific operation.

    """


    def __init__(self, match_df, player_df):
        self.prob_product = float('NaN')
        self.log_likelihood = float('NaN')

        self.match_data = match_df
        self.player_data = player_df
        self.optimised_ratings = float('NaN')

        self.match_data.dropna(inplace=True)

        # attributes of the ML optimiser to allow inspection of the opt process
        self.LL_evolution = []
        self.final_LL = 0
        self.iter =[]
        self.norm = []

        self.idx_mapping = {}

    def re_index_player_IDs(self):
        """
        Re-calculates and re-assigns player IDs for the match_df and player_df attributes inplace. Useful as avoids
        conflicts when players are added / dropped from the database

        Parameters
        ----------
        None

        Returns
        -------
        map_dict : dictionary
            Dictionary of the new ID mappings with k, v pairs of { old ID : new ID }

        """
 
        player_ids = self.player_data.index
        map_dict = dict(zip(player_ids, range(0, len(player_ids))))
        self.match_data.loc[:, "player_one_id"].replace(map_dict, inplace=True)
        self.match_data.loc[:, "player_two_id"].replace(map_dict, inplace=True)

        self.idx_mapping = map_dict # update the idx_mapping class attribute
        
        return map_dict

    def add_necessary_columns_to_df(self, df):
        """
        Add and initialise necessary columns to a DataFrame required by our model

        Parameters
        ----------
        df : pandas.DataFrame
            This is the matches DataFrame, our training dataset

        Returns
        -------
        None
        """
        df['nCr'] = 1.00
        df['time_decay'] = 1.00

        df['p1_strength'] = 1.00
        df['p2_strength'] = 1.00
        df['Player Strength Differential'] = 1.00
        df['Prob Frame P1'] = 1.00
        df['Prob Frame P2'] = 1.00
        df['Probability of Result'] = 1.00

    def update_nCr_col(self, df, NegBin):
        """
        Updates the nCr component of our model calculations, either for the Negative-Binomial configuration, or
        standard nCr approach

        Parameters
        ----------
        df : pandas.DataFrame
            This is the matches DataFrame, our training dataset
        negBin : bool
            Controls whether to run calculations for the Negative-Binomial case, or normal nCr. Default to True

        Returns
        -------
        None
        """
        if NegBin:
            df['nCr'] = df.apply(
                lambda x: comb(x['player_one_frames'] + x['player_two_frames'] - 1
                               if x['player_one_frames'] + x['player_two_frames'] - 1 >= 0 else 0,
                               x['player_one_frames'] - 1 if x['player_one_frames'] - 1 >= 0 else 0),
                axis=1).values
        else:
            df['nCr'] = df.apply(lambda x: comb(x['player_one_frames'] + x['player_two_frames'], x['player_one_frames']),
                                 axis=1).values

    def update_time_decay_col(self, df):
        """
        Updates the time decay factor, which is applied to all likelihoods of results, down-weighting older
        games in importance 

        Parameters
        ----------
        df : pandas.DataFrame
            This is the matches DataFrame, our training dataset

        Returns
        -------
        None
        """
        t_days_ago = (pd.to_datetime(df['date']) - pd.Timestamp.today()).dt.days

        decay_factor = 1 / 725
        time_decay = np.exp(decay_factor * t_days_ago)
        
        df['time_decay'] = time_decay

    def extract_constant_tensors(self, df):
        """
        Extracts the desired columns of our model (a DataFrame) so that we can access these in the 
        model-fitting stage to tune to maximise the Log-Likelihood 

        Parameters
        ----------
        df : pandas.DataFrame
            This is the matches DataFrame, our training dataset

        Returns
        -------
        p1_ID_extraction_vector : array
            An array with player 1 IDs from all games in the matches df

        p2_ID_extraction_vector : array
            An array with player 2 IDs from all games in the matches df

        nCr_col : vector
            An array with the nCr values of the match scorelines, from all games in the matches df

        time_decay_col : array
            An array with the time decay constants for each game in our the matches df

        p1_frames : array
            An array holding the frames P1 won in each game, for all the games in our matches df

        p2_frames : array
            An array holding the frames P2 won in each game, for all the games in our matches df
        """
        p1_ID_extraction_vector = df['player_one_id']
        p2_ID_extraction_vector = df['player_two_id']
        
        nCr_col = df['nCr']
        
        time_decay_col = df['time_decay']
        
        p1_frames = df['player_one_frames']
        p2_frames = df['player_two_frames']

        return p1_ID_extraction_vector, p2_ID_extraction_vector, nCr_col, time_decay_col, p1_frames, p2_frames
    
    def apply_model_transforms(self, NegBin=True):
        """
        Function that applies transforms to the matches and players DataFrames to configure it into our 
        desired model, contains calls to relevant sub-functions

        Parameters
        ----------
        df : pandas.DataFrame
            This is the matches DataFrame, our training dataset

        Returns
        -------
        p1_ID_extraction_vector : array
            An array with player 1 IDs from all games in the matches df

        p2_ID_extraction_vector : array
            An array with player 2 IDs from all games in the matches df

        nCr_col : vector
            An array with the nCr values of the match scorelines, from all games in the matches df

        time_decay_col : array
            An array with the time decay constants for each game in our the matches df

        p1_frames : array
            An array holding the frames P1 won in each game, for all the games in our matches df

        p2_frames : array
            An array holding the frames P2 won in each game, for all the games in our matches df
        """
        
        idx_mapping = self.re_index_player_IDs()
        self.add_necessary_columns_to_df(self.match_data)
        self.update_nCr_col(self.match_data, NegBin)
        self.update_time_decay_col(self.match_data)

        p1_ID_t, p2_ID_t, nCr_t, time_t, p1_frames_t, p2_frames_t = self.extract_constant_tensors(self.match_data)
        
        return p1_ID_t, p2_ID_t, nCr_t, time_t, p1_frames_t, p2_frames_t
    
    def x_to_probabilities(self, x, data_constants):
        """
        A function, used in the Loss function, to use the player ratings vector, x, to calculate the probabiluty of P1 and P2
        winning each game in the matches df

        Parameters
        ----------
        x : tf.Variable (containing dtype tf.float32)
            This is a vector of length (Number of Players) where each entry represents the players strength variable

        data_constants : list (of arrays)
            This is the model-specific arrays calculated as part of the model generation stage (recall: nCr, P1_ID, ...)

        Returns
        -------
        game_win_prob : tf.Vector # TODO: check this is actually correct
            A tensorflow Vector containing the probability of P1 winning each game
        """


        p1_ID_t, p2_ID_t, nCr_t, time_decay, p1_frames_t, p2_frames_t = data_constants

        p1_strength = tf.gather(x, p1_ID_t)
        p2_strength = tf.gather(x, p2_ID_t)

        player_strength_differential = tf.math.subtract(p1_strength, p2_strength)

        p1_frame_win_prob_t = tf.math.sigmoid(player_strength_differential)
        p2_frame_win_prob_t = tf.math.sigmoid(tf.math.scalar_mul(-1, player_strength_differential))

        # game_win_prob = nCr x (p1_frame_win_prob)^r X (p2_frame_win_prob)^(n-r) , where n-r = number of p2 frames, r = number of p1 frames

        p1_contribution = tf.math.pow(p1_frame_win_prob_t, p1_frames_t)
        p2_contribution = tf.math.pow(p2_frame_win_prob_t, p2_frames_t)
        raw_probability = tf.math.multiply(p1_contribution, p2_contribution)

        game_win_prob = tf.math.multiply(nCr_t, raw_probability)

        return game_win_prob
    
    def transform_tensor(self, vec):
        """
        A function which takes a Vector and returns sum of the logs of each element. In this case, it is the negative LL

        Parameters
        ----------
        vec : tf.Vector (containing dtype tf.float32)
            This is a vector of game-win probabilities

        Returns
        -------
        negative_LL : tf. Scalar
            This is a scalar showing the current negative Log Likelihood (LL) of the model
        """
        log_vec = tf.math.log(vec)
        overall_LL = tf.math.reduce_sum(log_vec)
        negative_LL = tf.math.scalar_mul(-1, overall_LL)
        return negative_LL
    
    def loss(self, x, data_constants):
        """
        Our Loss function for which we minimise in the optimisation

        Parameters
        ----------
        x : tf.Variable (containing dtype tf.float32)
            This is a vector of length (Number of Players) where each entry represents the players strength variable

        data_constants : list (of arrays)
            This is the model-specific arrays calculated as part of the model generation stage

        Returns
        -------
        None
        """
        with tf.GradientTape() as tape:
            tape.watch(x)

            prob_results_tensor = self.x_to_probabilities(x, data_constants)
            result = self.transform_tensor(prob_results_tensor)

        return result
    
    def fit_model(self, iterations, lr=0.0001):
        """
        Function that uses the Adam tensorflow optimiser to find the optimal player-rating vector that minimises
        our loss function, given the matches data

        Parameters
        ----------
        lr : float
            This is learning rate which can be tuned to change optimisation characteristics

        iterations : int
            This is the number of interations which we run the optimisation for

        Returns
        -------
        None
        """
        p1_ID_t, p2_ID_t, nCr_t, time_t, p1_frames_t, p2_frames_t = self.apply_model_transforms(NegBin=True)
        data_constants = [p1_ID_t, p2_ID_t, nCr_t, time_t, p1_frames_t, p2_frames_t]

        x = tf.Variable([0.5] * len(self.player_data), trainable=True, dtype=tf.float32)

        optimizer = tf.keras.optimizers.legacy.Adam(learning_rate=lr)

        variables = [x]

        # Use the optimizer to minimize the loss
        for _ in range(iterations):
            with tf.GradientTape() as tape:
                tape.watch(variables)
                loss_value = self.loss(x, data_constants)
                self.LL_evolution.append(loss_value)

            grads = tape.gradient(loss_value, variables)

            optimizer.apply_gradients(zip(grads, variables))

            if _ % 100 == 0:
                self.norm.append(self.track_tensor_norm(x))

            if _ % 1_000 == 0:
                norm = self.track_tensor_norm(x)
                print('Iteration: {}, Loss: {}, Norm: {}'.format(_, loss_value.numpy(), norm.numpy()))  # , X: {}'.format(loss_value.numpy(), x.numpy()))
        
        self.final_LL = self.LL_evolution[-1]
        optimised_player_df = self.return_opt_player_strength_df(x.numpy())
        optimised_player_df.to_csv('ratings')

        self.optimised_ratings = optimised_player_df
    
    def return_opt_player_strength_df(self, strength_vector):
        """
        Function that returns a pandas.DataFrame of the players new-found optimised strength values and their ID

        Parameters
        ----------
        strength_vector : np.Array
            This is an array of the new-found player strength values

        Returns
        -------
        df : pandas.DataFrame
            A pandas dataframe mapping the new strength ratings back to original player ID
        """
        
        map_dict = self.idx_mapping
        original_ID_and_strength = {original_ID : strength for original_ID, strength in zip(list(map_dict.keys()), list(strength_vector))}
        df = pd.DataFrame.from_dict(data=original_ID_and_strength, orient='index', columns=['rating'])
        df.index.name = 'player_id'

        return df
    
    def track_tensor_norm(self, x):
        """
        Function that calculates the L2 norm of the player strength vector
        Parameters
        ----------
        x : np.Array
            This is an array of the current player strength values

        Returns
        -------
        norm : tf.float
            A value representing the norm of the player strength vector (measure of size)
        """
        norm = tf.norm(x, ord='euclidean')
        return norm
    
    def plot_LL_and_norm_evolution_tf(self):
        """
        Helper function to plot quantities of interest during the model optimisation
        """
        plt.plot(np.arange(len(self.LL_evolution)), self.LL_evolution)  # we are minimizing the negative LL
        plt.xlabel('Iterations')
        plt.ylabel('Negative LL')
        plt.title('Evolution of the LL With Optimisation Iteration')
        plt.savefig('Neg_LL_Evolution_Snooker')
        plt.show()

        plt.plot(np.arange(len(self.norm)), self.norm)  # we are minimizing the negative LL
        plt.xlabel('Iterations')
        plt.ylabel('Euclidean Norm')
        plt.title('Euclidean Norm Evolution With Optimisation Iteration')
        plt.savefig('Norm_Evolution_Snooker')
        plt.show()



