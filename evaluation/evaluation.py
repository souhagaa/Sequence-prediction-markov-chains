import pandas as pd
from tqdm import tqdm
from itertools import zip_longest
from rpy2.robjects.packages import importr
import rpy2.robjects as robjects
import rpy2.robjects.packages as rpackages
from rpy2.robjects.vectors import StrVector
import time
pd.options.mode.chained_assignment = None
package_names = ('igraph', 'reshape2', 'clickstream', 'MASS')
if all(rpackages.isinstalled(x) for x in package_names):
    have_packages = True
else:
    have_packages = False
if not have_packages:
    utils = rpackages.importr('utils')
    utils.chooseCRANmirror(ind=1)
    packnames_to_install = [
        x for x in package_names if not rpackages.isinstalled(x)]
    if len(packnames_to_install) > 0:
        utils.install_packages(StrVector(packnames_to_install))
igraph = importr('igraph')
reshape2 = importr('reshape2')
clickstream = importr('clickstream')
MASS = importr('MASS')
r = robjects.r


def session_identification(df_actions, x, output_file_user_sessions, output_user_test_sessions):
    ''' ********* INTERNAL FUNCTION *********

        identify sessions of each user and save them to a file
        delete redundant consequent screens (skewed data)
    '''
    df = df_actions.groupby('session')
    user_sessions = df_actions['session'].unique().tolist()
    sessions_dict = {}
    test_dict = {}
    for x in user_sessions:
        all_actions = df.get_group(x)['screen_id'].tolist()
        actions = [i for i, j in zip_longest(all_actions, all_actions[1:]) if i != j]
        n = round(len(actions)*0.7)
        del all_actions
        if len(actions) != 1:
            test_actions = actions[n:]
            actions = actions[:n]
            sessions_dict[str(x)] = actions
            test_dict[str(x)] = test_actions
    user_sessions_df = pd.DataFrame.from_dict(sessions_dict, orient='index')
    test_df = pd.DataFrame.from_dict(test_dict, orient='index')
    user_sessions_df.to_csv(output_file_user_sessions, header=False, index=False)
    if test_df.shape[0] != 0:
        test_df.to_csv(output_user_test_sessions, header=False, index=False)
        return True


def actions_to_sessions(clean_pres_log="/home/souhagaa/Bureau/test/server/UX/UX/data/interm/clean/clean_presentation_log.csv",
                        sessions_period_file="/home/souhagaa/Bureau/test/server/UX/UX/data/interm/sessions_periods.csv",
                        output_file="/home/souhagaa/Bureau/test/server/UX/UX/data/evaluation_data/train/user_{}_sessions.csv",
                        output_test_file='/home/souhagaa/Bureau/test/server/UX/UX/data/evaluation_data/test/user_{}_test_sessions.csv'):

    created_train_files = []
    created_test_files = []
    print('assigning sessions to actions')
    df_pres = pd.read_csv(clean_pres_log)
    df_pres.sort_values(['username', 'datetime'], inplace=True)
    df_pres.reset_index(drop=True, inplace=True)
    df_pres['session'] = ''
    df_sessions = pd.read_csv(sessions_period_file)
    df_sessions.sort_values('username', inplace=True)
    df_sessions.reset_index(drop=True, inplace=True)
    group_sessions = df_sessions.groupby('username')
    group_actions = df_pres.groupby('username')
    usernames = df_sessions['username'].unique().tolist()
    del df_sessions
    del df_pres
    # ********* assigning sessions to actions *************
    for x in tqdm(usernames):
        df_user = group_sessions.get_group(x)
        df_actions = group_actions.get_group(x)
        for row_pres in df_actions.itertuples():
            for row_session in df_user.itertuples():
                if row_session.start_session <= row_pres.datetime <= row_session.end_session:
                    df_actions.at[row_pres.Index, 'session'] = row_session.session_id
                    break
        user_name_sessions = output_file.format(x)
        user_name_test_sessions = output_test_file.format(x)
        created_train_files.append(user_name_sessions)
        test_value = session_identification(df_actions, x, user_name_sessions, user_name_test_sessions)
        if test_value:
            created_test_files.append(user_name_test_sessions)
    return created_train_files, created_test_files


def execute_session_identification(clean_access_log='/home/souhagaa/Bureau/test/server/UX/UX/data/interm/clean/clean_access_log.csv',
                                   sessions_period_file='/home/souhagaa/Bureau/test/server/UX/UX/data/interm/sessions_periods.csv',
                                   clean_pres_log='/home/souhagaa/Bureau/test/server/UX/UX/data/interm/clean/clean_presentation_log.csv',
                                   output_file="/home/souhagaa/Bureau/test/server/UX/UX/data/evaluation_data/train/user_{}_sessions.csv",
                                   output_test_file='/home/souhagaa/Bureau/test/server/UX/UX/data/evaluation_data/test/user_{}_test_sessions.csv'):

    created_train_files, created_test_files = actions_to_sessions(clean_pres_log, sessions_period_file, output_file, output_test_file)
    return created_train_files, created_test_files


def markov_order(dataset_file):
    df = pd.read_csv(dataset_file, header=None)
    nulls = df.loc[0, :].isnull().sum()
    nulls
    for i in range(1, df.shape[0]):
        if nulls < df.loc[i, :].isnull().sum():
            nulls = df.loc[i, :].isnull().sum()
        print("row", i, df.loc[i, :].isnull().sum())
    return df.columns.tolist()[-1]-nulls


def fitting_model(training_set_path, output_model_path):
    start_time = time.time()
    r = robjects.r
    order = markov_order(training_set_path)
    try:
        r('train_set = "{}"'.format(training_set_path))
        r('cls <- readClickstreams(file = train_set, sep=",", header=FALSE)')
    except IOError:
        print("could not open model file")
    else:
        try:
            print("fitting the markov model to all clickstream with order:", order)
            r('mc <- fitMarkovChain(clickstreamList = cls, order = {}, control = list(optimizer = "quadratic"))'.format(order))
        except:
            print("fitting the markov model to all clickstreams with order 0")
            r('mc <- fitMarkovChain(clickstreamList = cls, order = 0, control = list(optimizer = "quadratic"))')
    finally:
        r('saveRDS(mc, "{}")'.format(output_model_path))
        print('fitted in', time.time()-start_time)


def training(session_files, output_dir):
    created_files = []
    for f in session_files:
        user = f.split('_')[1]
        print(user, "sessions")
        output_file = output_dir + "user_{}_markov_model.rds".format(user)
        # print(output_file)
        created_files.append((user, output_file))
        fitting_model(f, output_file)
    return created_files


def evaluate(training_set_path, test_set_path, model_path):
    df_test = pd.read_csv(test_set_path, header=None)
    df_training = pd.read_csv(training_set_path, header=None)
    test_data = []
    number_err = 0
    training_data = []
    for i in range(df_test.shape[0]):
        test_data.append(df_test.iloc[i, :].dropna().tolist())
    del df_test
    for i in range(df_training.shape[0]):
        training_data.append(df_training.iloc[i, :].dropna().tolist())
    del df_training
    # run through input elements
    r("mc <- readRDS('{}')".format(model_path))
    error_freq = 0
    for test, training in zip(test_data, training_data):
        r('startPattern <- new("Pattern", sequence = c("{}"))'.format(training[-1]))
        length_prediction = str(len(test))
        resultPattern = r("predict(mc, startPattern, dist={})".format(length_prediction))
        predicted = []
        for i in resultPattern.slots['sequence']:
            predicted.append(i)
        number_err = sum(i != j for i, j in zip(predicted, test))
        error_freq = error_freq + (number_err/len(predicted))
    return error_freq / len(test_data)


def execute_evaluation(test_files, train_files, data_path):
    sum_errors = 0
    number_evaluation = 0
    error_in_evaluation = 0
    for test, train in zip(test_files, train_files):
        user = test.split('_')[1]
        # print("evaluation model of ", user)
        model_file = data_path + "/evaluation_data/model/user_{}_markov_model.rds".format(user)
        try:
            error_rate = evaluate(train, test, model_file)
            # if error_rate < 0.5:
            sum_errors = sum_errors + error_rate
            number_evaluation += 1
        except:
            print("error while evaluating user", user)
            error_in_evaluation += 1
    print(error_in_evaluation)
    print(number_evaluation)
    return sum_errors/number_evaluation
