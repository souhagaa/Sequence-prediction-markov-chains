from rpy2.robjects.packages import importr
import rpy2.robjects as robjects
import rpy2.robjects.packages as rpackages
from rpy2.robjects.vectors import StrVector
import json
import os
import time
from rpy2.robjects import globalenv as globalenv
import rpy2.robjects as ro
import pandas as pd


# install R packages if not installed
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


def from_screen_to_id(searched_screen, file):
    ''' This function given the screen the user is in returns the id mapped to
        that screen
    '''
    searched_screen = searched_screen.lower()
    with open(file) as f:
        data = json.load(f)
    for id, screen in data.items():
        if screen == searched_screen:
            return id


def from_id_to_screen(id, file):
    ''' This function given the id of the screen returns the screen mapped to
            that id
    '''
    with open(file) as f:
        data = json.load(f)
    for key, screen in data.items():
        if key == id:
            return screen
    return None


def markov_order(session_file):
    df = pd.read_csv(session_file, header=None)
    nulls = df.loc[0, :].isnull().sum()
    nulls
    for i in range(1, df.shape[0]):
        if nulls < df.loc[i, :].isnull().sum():
            nulls = df.loc[i, :].isnull().sum()
        print("row", i, df.loc[i, :].isnull().sum())
    # return df.columns.tolist()[-1]+1-nulls
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
            print("fitting the markov model to all clickstream with order {}".format(str(order)))
            r('mc <- fitMarkovChain(clickstreamList = cls, order = {}, control = list(optimizer = "quadratic"))'.format(order))
        except:
            print("fitting the markov model to all clickstreams with order 0")
            r('mc <- fitMarkovChain(clickstreamList = cls, order = 0, control = list(optimizer = "quadratic"))')
    finally:
        r('saveRDS(mc, "{}")'.format(output_model_path))
        print('fitted in', time.time()-start_time)
        # return order, time.time()-start_time


def predict(pattern, model_path, screen_ids_file="/home/souhagaa/Bureau/test/server/UX/UX/data/final/screen_ids.json"):
    start_time = time.time()
    new_pattern = from_screen_to_id(pattern, screen_ids_file)
    print("the associated id is:", new_pattern)
    # new_pattern = pattern  # testing
    if not new_pattern:
        raise Exception("url not on the server")
    print("the link id is", new_pattern)
    print("predicting")
    r = robjects.r
    try:
        r("mc <- readRDS('{}')".format(model_path))
    except IOError:
        print("Cannot read model file")
    else:
        globalenv['link'] = ro.StrVector(new_pattern)
        for i in range(1, len(new_pattern)):
            globalenv['link'][0] += globalenv['link'][i]
        # globalenv['new'] = ro.StrVector(globalenv['link'][0])
        globalenv['new'] = globalenv['link'][0]
        print("final pattern", globalenv['new'])
        print("converting link to pattern")
        r("startPattern <- new('Pattern', sequence = c(new))")
        print("getting next link now")
        resultPattern = r("predict(mc, startPattern, dist=1)")
        print(resultPattern)
        # probability = resultPattern.slots['probability']
        # f = probability[0]
        seq = resultPattern.slots['sequence']
        print(seq)
        list_seq = []
        for i in seq:
            list_seq.append(i)
        screen = from_id_to_screen(list_seq[0], screen_ids_file)
        return screen
    end_time = time.time()
    print("execution time:", end_time-start_time)


def write_to_json(file, mode, object):
    with open(file, mode) as f:
        json.dump(object, f)
        f.write(os.linesep)
