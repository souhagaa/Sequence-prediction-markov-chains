import pandas as pd
from sklearn.preprocessing import LabelEncoder
import json
from tqdm import tqdm
import numpy as np
pd.options.mode.chained_assignment = None


def save_to_json(data, filename):
    ''' *********** INTERNAL FUNCTION ***********

    this function saves a dict into a json file
    '''
    try:
        with open(filename, 'w') as fp:
            json.dump(data, fp)
        print("saved to json successfully")
    except IOError:
        print("couldn't save json file")
        return None


def assign_usernames(df_access):
    '''  *********** INTERNAL FUNCTION ***********

    This function aims to delete sessions without usernames,
    session rows not belonging to any user and to assign username rows
    containing '-' by the respective username (owner of that session)
    '''
    sessions = df_access['session_id'].unique().tolist()
    print("delete sessions without users")
    print(len(sessions))
    group_sessions = df_access.groupby('session_id')
    new_df = pd.DataFrame()
    for x in tqdm(sessions):
        df_temp = group_sessions.get_group(x)
        usernames = df_temp['username'].unique().tolist()
        if usernames != ['-'] and usernames != [np.nan, '-'] \
                and usernames != [np.nan] and usernames != ['-', np.nan]:
            df_temp.sort_values('datetime', inplace=True)
            df_temp.reset_index(drop=True, inplace=True)
            j = 1
            # we delete session rows not belonging to any user
            if df_temp.loc[0]['username'] == '-':
                print("first user in session", x, "is -")
                while (df_temp.loc[j]['username'] == '-'):
                    print("deleting")
                    df_temp = df_temp.loc[j:, :]
                    j += 1
                print("done deleting")
                df_temp = df_temp.loc[j:, :]
            df_temp.reset_index(drop=True, inplace=True)
            # assigning to each session its corresponding username
            last_user = df_temp.loc[0]['username']
            for k in range(1, df_temp.shape[0]):
                current_user = df_temp.loc[k]['username']
                if (current_user != last_user and current_user == '-'):
                    df_temp.loc[k]['username'] = last_user
                else:
                    if current_user != last_user:
                        last_user = current_user
            new_df = new_df.append(df_temp)
    # del df_temp
    del sessions
    del group_sessions
    new_df.shape
    df_access.shape
    df_access = new_df
    del new_df
    print("after deleting sessions without users", df_access.head())
    print("number of sessions", len(df_access['session_id'].unique().tolist()))
    return df_access


def label_encoding_sessions(df_access):
    '''  *********** INTERNAL FUNCTION ***********

    Since session ids are not unique, the server can assign the same
    session id to a different user once that session is released so we need
    to give a unique id to each pair (user, session)
    '''
    group_sessions = df_access.groupby('session_id')
    len(df_access['session_id'].unique().tolist())
    df_access['s_id'] = 0
    df_access.head(10)
    df_access['s_id'] = df_access.groupby(['username', 'session_id']).ngroup()
    len(df_access['s_id'].unique())
    df_access.columns = ['datetime', 'username', 's_id', 'session_id']
    del group_sessions
    return df_access


def keep_users_having_sessions(df_access, df_pres):
    ''' *********** INTERNAL FUNCTION ***********

    From the presentation logs we keep the users that only exist in the
    access log, other users are deleted because we don't have their sessions on
    our access logs.
    '''
    # we need to get a list of the users only having sessions
    is_in = pd.DataFrame(df_pres.username.isin(df_access.username).values.astype(int), df_pres.username.values)
    is_in.reset_index(drop=False, inplace=True)
    is_in.columns = ['username', 'is_in']
    is_in = is_in[is_in['is_in'] == 1]
    users_having_sessions = []
    users_having_sessions = is_in['username'].unique().tolist()
    # users_having_sessions
    print("pres dataframe before deleting users who don't have sessions", df_pres.shape)
    df_pres = df_pres[df_pres['username'].isin(users_having_sessions)]
    print("pres dataframe after deleting users who don't have sessions", df_pres.shape)
    del is_in
    return df_pres, users_having_sessions


def clean_parsed_logs(input_access="/home/souhagaa/Bureau/test/server/UX/UX/data/interm/parsed_access_log.csv",
                      input_pres="/home/souhagaa/Bureau/test/server/UX/UX/data/interm/parsed_presentation_log.csv",
                      output_pres="/home/souhagaa/Bureau/test/server/UX/UX/data/interm/clean/clean_presentation_log.csv",
                      output_access="/home/souhagaa/Bureau/test/server/UX/UX/data/interm/clean/clean_access_log.csv",
                      screen_ids_file="/home/souhagaa/Bureau/test/server/UX/UX/data/final/screen_ids.json"):
    '''
    This function executes the cleaning of both the presentation logs and
    the access logs by:
        *** deleting duplicate rows to optimize memory usage
        *** deletings rows with null session ids
        *** deleting sessions without users and assigning usernames to each row
        *** label encoding session ids for uniqueness
        *** deleting from presentation logs users not belonging to access logs
        *** deleting from access logs users not belonging to presentation logs
        *** giving an id to each screen and saving the mapping between ids
            and screen names
    '''
    # *************** deleting useless rows of data ***************
    df_access = pd.read_csv(input_access)
    print("shape of access df before deleting duplicate rows", df_access.shape)
    df_access.drop_duplicates(inplace=True)
    print("shape of access df after deleting duplicate rows", df_access.shape)

    print("showing shape before deleting null session ids", df_access.shape)
    df_access.dropna(subset=['session_id'], inplace=True)
    df_access.head()
    print("showing shape after deleting null session ids", df_access.shape)

    # ** deleting sessions without users and assigning usernames to each row**
    df_access = assign_usernames(df_access)

    # ************ label encoding session ids for uniqueness ************
    df_access = label_encoding_sessions(df_access)

    # *********** deleting users that don't belong to presentation logs *******
    df_pres = pd.read_csv(input_pres)
    df_pres, users_having_sessions = keep_users_having_sessions(df_access, df_pres)

    # ************* giving ids to each screen ************
    le = LabelEncoder()
    df_pres['screen'] = df_pres['screen'].astype('str').apply(lambda x: x.lower())
    df_pres['screen_id'] = le.fit_transform(df_pres['screen'].astype(str))
    # screen_mapping serves as  map between the screens and their ids
    screen_mapping = dict(zip(le.transform(le.classes_), le.classes_))
    d = {"P" + str(k): v for k, v in screen_mapping.items()}
    save_to_json(d, screen_ids_file)
    df_pres['screen_id'] = df_pres['screen_id'].apply(lambda x: f"P{x}")
    print(df_pres.head(10))
    df_pres.to_csv(output_pres, index=False)
    del df_pres

    # ***** cleaning access logs now ******
    print(df_access.head())
    # users_having_sessions
    print("          ***************************************")
    print("access dataframe before deleting users who are not in presentation logs", df_access.shape)
    df_access = df_access[df_access['username'].isin(users_having_sessions)]
    print("shape of access dataframe after deleting users who are not in presentation logs", df_access.shape)
    print(df_access.head())
    df_access.to_csv(output_access, index=False)
    del users_having_sessions
    del df_access
