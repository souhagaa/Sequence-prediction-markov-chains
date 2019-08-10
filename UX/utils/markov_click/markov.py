from rpy2.robjects.packages import importr
import rpy2.robjects as robjects
import rpy2.robjects.packages as rpackages
from rpy2.robjects.vectors import StrVector
# import pandas as pd
# from rpy2.robjects import pandas2ri
import json
import os
from rpy2.robjects import globalenv as globalenv
import rpy2.robjects as ro
import time


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


def fitting_model(training_set_path, test_set_path, output_model_path, i):
    print("fitting")
    start_time = time.time()
    r = robjects.r
    r('train_set = "{}"'.format(training_set_path))
    r('cls <- readClickstreams(file = train_set, sep=",", header=FALSE)')
    r('mc <- fitMarkovChain(clickstreamList = cls, order = 1, control = list(optimizer = "quadratic"))')
    try:
        r('''
        png("/home/souhagaa/Bureau/test/server/UX/UX/data/final/myplot_{}.png", 1800, 1800)
        myplot <- plot(mc, order = 1, digits = 2)
        print(myplot)
        dev.off()
        '''.format(i))
    except:
        print("error in plotting")
    # r('''
    # png("hm.png", 1800, 1800)
    # myplot <- hmPlot(mc, order = 2, absorptionProbability = FALSE, title = NA,
    #     lowColor = 'white', highColor = 'red', flip = FALSE)
    # print(myplot)
    # dev.off()
    # ''')
    # r('test <- readClickstreams(file = "{}" , sep = ",", header = FALSE)'.format(test_set_path))
    # matrix = r('mcEvaluateAll(mc, cls, test, includeChiSquare = TRUE)')
    # # chisquare_np = numpy.array(matrix)
    # chisquare_df = pandas2ri.ri2py(matrix)
    # pd.DataFrame(chisquare_df).to_csv("chi_square.csv")
    # return it or save it
    finally:
        r('saveRDS(mc, "{}")'.format(output_model_path))
        end_time = time.time()
        return end_time-start_time


def predict(pattern, model_path):
    start_time = time.time()
    new_pattern = from_url_to_id(pattern)
    print("the associated id is:", new_pattern)
    # new_pattern = pattern  # testing
    if not new_pattern:
        raise Exception("url not on the server")
    print("the link id is", new_pattern)
    print("predicting")
    r = robjects.r
    try:
        r("mc <- readRDS('{}')".format(model_path))
    except:
        print("Cannot read model file")
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
    probability = resultPattern.slots['probability']
    f = probability[0]
    if f < 0.2:
        return None
    else:
        seq = resultPattern.slots['sequence']
        print(seq)
        list_seq = []
        for i in seq:
            # print(i)
            list_seq.append(i)
        url = from_id_to_url(list_seq[0])
        # write_to_json('predicted_seq.json', 'w', url)
        end_time = time.time()
        print("execution time:", end_time-start_time)
        return url
        # with open('predicted_seq.json', 'a') as f:
        #     json.dump(list_seq, f)
        #     f.write(os.linesep)


def write_to_json(file, mode, object):
    with open(file, mode) as f:
        json.dump(object, f)
        f.write(os.linesep)
