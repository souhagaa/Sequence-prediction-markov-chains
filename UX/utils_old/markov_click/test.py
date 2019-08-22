from rpy2.robjects.packages import importr
import rpy2.robjects as robjects
import rpy2.robjects.packages as rpackages
from rpy2.robjects.vectors import StrVector
import pandas as pd
from rpy2.robjects import pandas2ri
import json
import os
from rpy2.robjects import globalenv as globalenv
import rpy2.robjects as ro
import time

igraph = importr('igraph')
reshape2 = importr('reshape2')
clickstream = importr('clickstream')
MASS = importr('MASS')
r = robjects.r


def from_url_to_id(searched_url):
    ''' This function given the url the user is in returns the id mapped to
        that url
    '''
    file = "/home/souhagaa/Bureau/test/server/UX/UX/data/final/url_ids.json"
    with open(file) as f:
        data = json.load(f)
    for id, url in data.items():
        if url == searched_url:
            return id


def from_id_to_url(id):
    ''' This function given the id of url returns the url mapped to
            that id
    '''
    file = "/home/souhagaa/Bureau/test/server/UX/UX/data/final/url_ids.json"
    with open(file) as f:
        data = json.load(f)
    for key, url in data.items():
        if key == id:
            return url
    return None


def fit():
    training_set_path = "/home/souhagaa/Bureau/test/server/UX/UX/data/interm/identified/sessions_user_3.csv"
    output_model_path = "./test_model.rds"
    r('train_set = "{}"'.format(training_set_path))
    r('cls <- readClickstreams(file = train_set, sep=",", header=FALSE)')
    r('mc <- fitMarkovChain(clickstreamList = cls, order = 1, control = list(optimizer = "quadratic"))')
    r('saveRDS(mc, "{}")'.format(output_model_path))
    print("success")


def predict(pattern):
    new_pattern = from_url_to_id(pattern)
    globalenv['link'] = ro.StrVector(new_pattern)
    for i in range(1, len(new_pattern)):
        print("elmnt", i,  globalenv['link'][i])
        globalenv['link'][0] += globalenv['link'][i]
    print("out of for final value is", globalenv['link'][0])
    # globalenv['new'] = ro.StrVector(globalenv['link'][0])
    globalenv['new'] = globalenv['link'][0]
    print("finally", globalenv['new'])
    print("converting link to pattern succeeded")

    model_path = "/home/souhagaa/Bureau/test/server/UX/UX/data/final/user_3_markov_model.rds"
    r("mc <- readRDS('{}')".format(model_path))
    print("model read success")
    try:
        r('pattern <- new("Pattern", sequence = c(new))')
        print("pattern to new pattern success")
        print(r('pattern'))
    except:
        print("failed")
    resultPattern = r('resultPattern <- predict(mc, startPattern = pattern, dist = 1)')
    r('resultPattern')
    seq = resultPattern.slots['sequence']
    print(seq)


# fit()
predict("/history/apollo/images/apollo-logo1.gif")
