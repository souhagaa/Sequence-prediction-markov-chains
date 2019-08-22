import pandas as pd
from tqdm import tqdm
from itertools import zip_longest
pd.options.mode.chained_assignment = None


def session_period_identification(clean_access_log="/home/souhagaa/Bureau/test/server/UX/UX/data/interm/clean/clean_access_log.csv",
                                  sessions_period_file="/home/souhagaa/Bureau/test/server/UX/UX/data/interm/sessions_periods.csv"):
    '''
        identify for each session its start time and end time
    '''
    print("inside session_period_identification")
    df_access = pd.read_csv(clean_access_log)
    del df_access['s_id']
    print('assigning to each session its start date and its end date')
    sessions = df_access['session_id'].unique().tolist()
    sessions[0:10]
    sessions.sort()
    user_sessions = df_access.loc[:, 'username':'session_id']
    user_sessions.head()
    user_sessions.shape
    user_sessions.drop_duplicates(inplace=True)
    user_sessions.shape
    user_sessions.reset_index(inplace=True, drop=True)
    user_sessions.head()
    df_access.sort_values('session_id', inplace=True)
    df_access.head()
    grouped = df_access.groupby(['session_id'])
    user_sessions['start_session'] = ''
    user_sessions['end_session'] = ''
    for x in tqdm(sessions):
        df_sessions = grouped.get_group(x)
        df_sessions.sort_values('datetime', inplace=True)
        row_nb = user_sessions[user_sessions['session_id'] == x].index[0]
        user_sessions.at[row_nb, 'start_session'] = df_sessions.iloc[0]['datetime']
        user_sessions.at[row_nb, 'end_session'] = df_sessions.iloc[df_sessions.shape[0]-1]['datetime']
    del df_sessions
    user_sessions.head(10)
    user_sessions.to_csv(sessions_period_file, index=False)
    del df_access
    del grouped
    # del user_sessions


def session_identification(df_actions, x, output_file_user_sessions):
    ''' ********* INTERNAL FUNCTION *********

        identify sessions of each user and save them to a file
        delete redundant consequent screens (skewed data)
    '''
    # print('identifying ', x, 'sessions')
    df = df_actions.groupby('session')
    user_sessions = df_actions['session'].unique().tolist()
    sessions_dict = {}
    for x in user_sessions:
        all_actions = df.get_group(x)['screen_id'].tolist()
        actions = [i for i, j in zip_longest(all_actions, all_actions[1:]) if i != j]
        del all_actions
        sessions_dict[str(x)] = actions
    user_sessions_df = pd.DataFrame.from_dict(sessions_dict, orient='index')
    user_sessions_df.to_csv(output_file_user_sessions, header=False, index=False)


def actions_to_sessions(clean_pres_log="/home/souhagaa/Bureau/test/server/UX/UX/data/interm/clean/clean_presentation_log.csv",
                        sessions_period_file="/home/souhagaa/Bureau/test/server/UX/UX/data/interm/sessions_periods.csv",
                        output_file="/home/souhagaa/Bureau/test/server/UX/UX/data/interm/identified/user_{}_sessions.csv"):
    created_files = []
    print('assigning sessions to actions')
    df_pres = pd.read_csv(clean_pres_log)
    df_pres.sort_values(['username', 'datetime'], inplace=True)
    df_pres.reset_index(drop=True, inplace=True)
    # df_pres.head()
    df_pres['session'] = ''
    df_sessions = pd.read_csv(sessions_period_file)
    df_sessions.sort_values('username', inplace=True)
    df_sessions.reset_index(drop=True, inplace=True)
    # df_sessions.head()
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
        created_files.append(user_name_sessions)
        session_identification(df_actions, x, user_name_sessions)
    return created_files


def execute_session_identification(clean_access_log, sessions_period_file, clean_pres_log, output_file):

    session_period_identification(clean_access_log, sessions_period_file)
    created_files = actions_to_sessions(clean_pres_log, sessions_period_file, output_file)
    return created_files
