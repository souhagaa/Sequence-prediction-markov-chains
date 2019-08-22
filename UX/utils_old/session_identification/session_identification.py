import pandas as pd
import datatable as dt
from tqdm import tqdm


def datatable_session_split(datatable_df, map_url):
    """ This function splits the given dataset into sessions
    A a session timeout is defined by a 30 minutes inactivity.

    The function returns a map linking each host to his sessions and the last
    session id in order to use it as a start in the next iteration
    """

    session_id = 0
    dt_result = dt.Frame()  # empty datatable
    last_time = datatable_df[0, 0]
    session = [session_id, datatable_df[0, 1]]
    map_url[datatable_df[0, 2]] = str(session_id)

    for i in tqdm(range(1, datatable_df.shape[0])):
        host_id = datatable_df[i, 2]
        if (datatable_df[i, 2] == datatable_df[i - 1, 2]):
            current_time = datatable_df[i, 0]
            period = current_time - last_time

            if period > 1800:
                session_id += 1
                map_url[host_id] = map_url[host_id] + ", " + str(session_id)
                dt_result.cbind(dt.Frame(session), force=True)
                session = [session_id, ]
            session.append(datatable_df[i, 1])
            last_time = current_time
        else:
            session_id += 1
            map_url[host_id] = str(session_id)
            dt_result.cbind(dt.Frame(session), force=True)
            session = [session_id, datatable_df[i, 1]]
            last_time = datatable_df[i, 0]
    dt_result.cbind(dt.Frame(session), force=True)
    return dt_result, map_url


def delete_redundancy_sessions(df):
    dt_result = dt.Frame()
    for i in range(df.shape[0]):
        a = df.iloc[i, :]
        d = a.loc[a.shift() != a]
        b = pd.Series(d).dropna()
        dt_result.cbind(dt.Frame(b), force=True)
    del df
    dt_result.head(30)
    df2 = dt_result.to_pandas()
    del dt_result
    df2 = df2.transpose()
    return df2


def execute_session_identification(dataset_file, output_file):
    map_url = {}
    try:
        datatable_df = dt.fread(dataset_file)
        if 'C0' in datatable_df.names:
            del datatable_df[:, 'C0']

        dt_result, map_url = datatable_session_split(
            datatable_df, map_url)
        df = dt_result.to_pandas()
        del dt_result
        df = df.transpose()
        print(df.head(10))
        new_index = df.columns[0]
        df = df.set_index(new_index)
        df = delete_redundancy_sessions(df)
        df.to_csv(output_file, index=False, header=False, mode='w')
        return output_file
    except IOError:
        print("Could not read file:", dataset_file)
        return None
