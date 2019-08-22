#!/usr/bin/env python
# coding: utf-8
import sys
import pandas as pd
# from sklearn.preprocessing import LabelEncoder
import datatable as dt
import pickle
import json
import os
from tqdm import tqdm
from pathlib import Path


def chunking_dataset(df, chunk_size):
    """ Having large dataset will slow down the computer so we will divide the
    process into several steps.

    This function takes an input df which is our intial dataset and chunk_size
    which is a chunk size given by the user, in our case a chunk of size 100k
    takes about 1.5 minutes and 500k takes about 19minselfself.

    Because our dataset is grouped by hosts and ordered by timestamps so the
    chunking should be done in a way that it gets all of one host's input in
    the same chunk. That's why we check for the next rows when chunking our
    dataset.

    The function returns the chunk and the initial datatset minus the chunk
    also the final chunk size.
    """
    j = chunk_size
    for i in range(chunk_size, df.shape[0]):
        if df[i, 2] == df[i - 1, 2]:
            j += 1
            # print(i)
        else:
            print("Break case: all host entries are in chunk")
            break

    result = df[:j, :]
    del df[:j, :]
    print("Chunk size: ", j)
    return result, df, j


def datatable_session_split(datatable_df, ind, map_url):
    """ This function splits the given dataset into sessions
    A a session timeout is defined by a 30 minutes inactivity.

    The function returns a map linking each host to his sessions and the last
    session id in order to use it as a start in the next iteration
    """

    session_id = ind
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
    return dt_result, map_url, session_id


def session_split_iterations(last_session_id, map_url, dataset_file, size,
                             output_file=None, name="training"):
    """This function does the session split iteration and saves them into
    a csv file
    """
    i = size
    try:
        datatable_df = dt.fread(dataset_file)
    except IOError:
        print("Could not read file:", dataset_file)
        sys.exit(-1)
    else:
        if datatable_df.shape[0] == 0:
            print("All rows have been processed!")
            return -1  # leave it as it is coz used afterwards
        else:
            if 'C0' in datatable_df.names:
                del datatable_df[:, 'C0']
            print("Initial dataset size:", datatable_df.shape[0])
            print("Splitting session starting at session number: ", last_session_id)
            try:
                val = int(i)
            except ValueError:
                print("You provided a chunk size that is not an int, please try again")
                sys.exit(-1)
            else:
                chunked_dt, df, chunk_size = chunking_dataset(
                    datatable_df, int(i))
                print("Datatable size: ", datatable_df.shape)
                print("Chunked dataset size: ", chunked_dt.shape)
                datatable_df.to_csv(dataset_file)
                del datatable_df
                dt_result, map_url, last_session_id = datatable_session_split(
                    chunked_dt, last_session_id, map_url)
                df = dt_result.to_pandas()
                del dt_result
                df = df.transpose()
                print(df.head(10))
                new_index = df.columns[0]
                df = df.set_index(new_index)
                df.to_csv("/home/souhagaa/Bureau/test/server/UX/UX/data/interm/{}_host_sessions.csv".format(name), header=False, mode='a')
                save_to_pickle_file(last_session_id, "/home/souhagaa/Bureau/test/server/UX/UX/data/interm/last_session_id.pkl")
                return map_url, last_session_id


def save_to_pickle_file(obj, filename):
    outfile = open(filename, 'wb')
    pickle.dump(obj, outfile)
    outfile.close()
    print("Session id ", obj, "pickled successfully")


def unpickle_object_pickle(filename):
    infile = open(filename, 'rb')
    obj = pickle.load(infile)
    infile.close()
    print("Session id ", obj, "unpickled successfully")
    return obj


def append_url_map_to_json(host_session_map):
    with open('/home/souhagaa/Bureau/test/server/UX/UX/data/interm/host_session_map.json', 'a') as f:
        json.dump(host_session_map, f)
        f.write(os.linesep)


def copy_csv(filename, name="training"):
    print("making copy of dataset")
    try:
        df = pd.read_csv(filename)
    except IOError:
        print("could not read file to make copy")
    else:
        df.to_csv('/home/souhagaa/Bureau/test/server/UX/UX/data/interm/copy_{}_set.csv'.format(name), index=False)


def execute_session_identification(dataset_file, output_file, name="training"):
    # ="/home/souhagaa/Bureau/test/server/UX/UX/data/interm/data_sample.csv"
    map_url = {}
    session_id_file = "/home/souhagaa/Bureau/test/server/UX/UX/data/interm/last_session_id.pkl"
    my_file = Path(session_id_file)
    # dataset_file = "/home/souhagaa/Bureau/test/server/UX/UX/data/interm/data_sample.csv"
    # we can make a copy of the file by calling
    # copy_csv(dataset_file)
    # this is the variable that allows you to choose the chunk size
    # for reasons of resources and speed I chose 1000 because it takes less
    # than a minute to be processed and it's not memory exhausting
    chunk_auto = 1000
    try:
        if my_file.is_file() is False:
            print("Starting first iteration")
            map_url, last_session_id = session_split_iterations(
                0, map_url, dataset_file, chunk_auto, name)

        if my_file.is_file():
            while True:
                last_session_id = unpickle_object_pickle(session_id_file)
                if session_split_iterations(last_session_id + 1, map_url, dataset_file, chunk_auto, name) == -1:
                    print("Exiting execution all rows have been processed successfully")
                    append_url_map_to_json(map_url)
                    return
                else:
                    last_session_id = unpickle_object_pickle(
                        session_id_file)
                    print("Main: starting at session number ",
                          last_session_id + 1)
                    map_url, last_session_id = session_split_iterations(
                        last_session_id + 1, map_url, dataset_file, chunk_auto, name)
    except:
        print("Something went wrong in the main function or all rows processed")
    finally:
        append_url_map_to_json(map_url)


def delete_redundancy_sessions(name="training", output_file=None):
    file="/home/souhagaa/Bureau/test/server/UX/UX/data/interm/{}_host_sessions.csv".format(name)
    # output_file="/home/souhagaa/Bureau/test/server/UX/UX/data/final/{}_set_no_duplicate.csv".format(name)
    # change name to training_markov
    df = pd.DataFrame()
    i = 0
    with open(file, 'r') as f:
        for line in f:
            df = pd.concat(
                [df, pd.DataFrame([tuple(line.strip().split(','))])], ignore_index=True)
            print(i)
            i += 1
    # deleting consecutive identical urls and using datatable because it is
    # faster than pandas
    dt_result = dt.Frame()
    for i in range(df.shape[0]):
        print("i", i)
        a = df.iloc[i, :]
        d = a.loc[a.shift() != a]
        b = pd.Series(d).dropna()
        dt_result.cbind(dt.Frame(b), force=True)
    del df
    dt_result.head(30)
    df2 = dt_result.to_pandas()
    del dt_result
    df2 = df2.transpose()
    df2.to_csv(output_file, index=False, header=False)
    del df2
    print("end")
