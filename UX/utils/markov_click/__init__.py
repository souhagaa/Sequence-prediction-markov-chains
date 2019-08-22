from rpy2.robjects.packages import importr
import rpy2.robjects as robjects
import rpy2.robjects.packages as rpackages
from rpy2.robjects.vectors import StrVector
import json
import os
import time
from rpy2.robjects import globalenv as globalenv
import rpy2.robjects as ro


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


def fitting_model(training_set_path, test_set_path, output_model_path, i):
    print("fitting")
    start_time = time.time()
    r = robjects.r
    try:
        r('train_set = "{}"'.format(training_set_path))
        r('cls <- readClickstreams(file = train_set, sep=",", header=FALSE)')
    except IOError:
        print("could not open model file")
    # r('mc <- fitMarkovChain(clickstreamList = cls, order = 1, control = list(optimizer = "quadratic"))')
    else:
        try:
            r('''
            png("/home/souhagaa/Bureau/test/test_log_chronos/presentation/models/myplot_{}.png", 1800, 1800)
            myplot <- plot(mc, order = 1, digits = 2)
            print(myplot)
            dev.off()
            '''.format(i))
        except:
            print("error in plotting")

        # try:
        #     r('clusters <- clusterClickstreams(clickstreamList = cls, order = 1, centers = 2)')
        #     print("clustering")
            # r('print(clusters)')
        # except:
            # print("could not cluster clickstreams, data is too large")
        try:
            print("fitting the markov model to all clickstream with order 1")
            r('mc <- fitMarkovChain(clickstreamList = cls, order = 1, control = list(optimizer = "quadratic"))')
        except:
            print("fitting the markov model to all clickstream with order 0")
            r('mc <- fitMarkovChain(clickstreamList = cls, order = 0, control = list(optimizer = "quadratic"))')
        # else:
        #     print("fitting model to clusters")
        #     r('fitMarkovChains(clusters, order = 1)')

        # try:
        #     r('matrix <- chiSquareTest(cls, mc)')
        #     r('capture.output(summary(matrix), file = "/home/souhagaa/Bureau/test/test_log_chronos/presentation/models/chi_square.txt")')
        #     # r('''cat(matrix,file="/home/souhagaa/Bureau/test/test_log_chronos/presentation/models/aaloui_chi_square.txt",sep='\n')''')
        # except:
        #     print("data is too large for calculating the chi square metric")
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
