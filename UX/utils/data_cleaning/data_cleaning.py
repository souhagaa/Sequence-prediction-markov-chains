import pandas as pd
from sklearn.preprocessing import LabelEncoder
import json
import sys
import traceback
import numpy as np


def data_cleaning(file):
    print("initial cleaning")
    try:
        col_names = ["host", "method", "url", "protocol", "response", "bytes", "time"]
        col_types = {"host": object, "method": object, "url": object,
            "protocol": object, "response": object, "bytes": np.int64,
            "time": np.int64}
        df = pd.read_csv(file, delimiter=',', names=col_names, dtype=col_types, skiprows=1, error_bad_lines=False, warn_bad_lines=True)

    except IOError:
        print("error while reading log files")
        traceback.print_exc(file=sys.stdout)
        return None
    # reorder columns
    # we can delete columns that we don't need to save memory
    df = df[['host', 'time', 'url']]
    test = df.groupby('host').filter(lambda g: len(g) > 3).drop_duplicates(
        subset=['host', 'time', 'url'], keep="first")
    # we only keep urls having more than 1000 visits
    test = df[df.groupby('url')['url'].transform('size') > 1000]
    # test = df[df.groupby('host')['host'].transform('size') > 1000]
    # print("*********************")
    # print("shape of dataframe after condition added", test.shape)
    test['host'] = test['host'].astype(str)
    test.sort_values(['host', 'time'], axis=0, inplace=True)
    test.reset_index(inplace=True, drop=True)
    print("end of cleaning")
    return test


def save_to_json(data, filename):
    try:
        with open(filename, 'w') as fp:
            json.dump(data, fp)
        print("saved to json successfully")
    except:
        print("couldn't save json file")
        sys.exit(-1)


def preprocessing_for_markov(df):
    url_ids_file = '/home/souhagaa/Bureau/test/server/UX/UX/data/final/url_ids.json'
    host_ids_file = '/home/souhagaa/Bureau/test/server/UX/UX/data/final/host_ids.json'
    print("preprocessing for markov model")
    # WE will give each url an id for more readability
    try:
        le = LabelEncoder()
        df['url_id'] = le.fit_transform(df['url'])
        # url_mapping serves as  map between the url's and the url ids
        url_mapping = dict(zip(le.transform(le.classes_), le.classes_))
        d = {"P" + str(k): v for k, v in url_mapping.items()}
        # url_mapping = dict(zip(le.classes_, le.transform(le.classes_)))
        # print(d)
        df['url_id'] = df['url_id'].apply(lambda x: f"P{x}")
        # save the mapping between urls and ids
        save_to_json(d, url_ids_file)
        del d
        print(df[['url', 'url_id']].head())
        del df['url']
        df['host_id'] = le.fit_transform(df['host'])
        host_mapping = dict(zip(le.transform(le.classes_), le.classes_))
        d = {int(k): v for k, v in host_mapping.items()}
        save_to_json(d, host_ids_file)
        print(df[['host', 'host_id']].head())
        del df['host']
    except:
        print("json dumping not working")
        traceback.print_exc(file=sys.stdout)
        return None
    else:
        # del df['method']
        # del df['response']
        # del df['bytes']
        print("saving the dataset to csv")
        df.to_csv("/home/souhagaa/Bureau/test/server/UX/UX/data/interm/data_clean.csv", index=False)
        print("end of preprocessing")


def sampling(input_file, output_dir, n=999):
    # one file with all cleaned data then split test set in session identication
    training_output_file = output_dir+"/training_user_{}.csv"
    # testing_output_file = output_dir+"/testing_user_{}.csv"
    print("getting hosts dataframes")
    dt = pd.read_csv(input_file)
    if 'Unnamed: 0' in dt.columns:
        del dt['Unnamed: 0']
    grouped = dt.groupby("host_id")
    print("grouping")
    result = [g[1] for g in list(grouped)[:n]]
    files = []
    for i in range(n):
        host_id = result[i].iloc[0, 2]
        # start = int(result[i].shape[0]*0.8)
        result[i].to_csv(training_output_file.format(host_id), index=False)
        # result[i].iloc[start:, :].to_csv(testing_output_file.format(host_id), index=False)
        files.append((host_id,
                      training_output_file.format(host_id)))
    return files

#
# def sample_training_set(input_file="/home/souhagaa/Bureau/test/server/UX/UX/data/interm/data_clean.csv",
#     output_file="/home/souhagaa/Bureau/test/server/UX/UX/data/interm/data_sample.csv", n=999):
#
#     print("getting 1000 hosts")
#     dt = pd.read_csv(input_file)
#     if 'Unnamed: 0' in dt.columns:
#         del dt['Unnamed: 0']
#     df = pd.DataFrame()
#     grouped = dt.groupby("host_id")
#     print("grouping")
#     result = [g[1] for g in list(grouped)[:n]]
#     type(result)
#     len(result)
#     # for i in range(n):
#     #     host = result[0].iloc[0,2]
#     #     result[i].to_csv("output_training_user_{}.csv".format(host))
#     for i in range(n):
#         df = pd.concat([df, result[i]])
#     df.head()
#     df.to_csv(output_file, index=False)
#     return n
#
#
# def sample_test_set(start,input_file="/home/souhagaa/Bureau/test/server/UX/UX/data/interm/data_clean.csv",
#     output_file="/home/souhagaa/Bureau/test/server/UX/UX/data/interm/test_sample.csv", n=10):
#
#     print("grouping for test set")
#     dt = pd.read_csv(input_file)
#     if 'Unnamed: 0' in dt.columns:
#         del dt['Unnamed: 0']
#     df = pd.DataFrame()
#     grouped = dt.groupby("host_id")
#     print("grouping")
#     result = [g[1] for g in list(grouped)[start+1:start+n+2]]
#     type(result)
#     len(result)
#     # for i in range(n):
#     #     host = result[0].iloc[0,2]
#     #     result[i].to_csv("output_test_user_{}.csv".format(host))
#     for i in range(n):
#         df = pd.concat([df, result[i]])
#     df.head()
#     df.to_csv(output_file, index=False)
