from UX.extensions import celery
from UX.utils.data_cleaning.data_cleaning import data_cleaning, \
    preprocessing_for_markov, sampling
from UX.utils.markov_click.markov import fitting_model
from UX.utils.log_parsing.parser import LogParser
from UX.config import DATA_DIR
from UX.utils.session_identification.session_identification import \
    execute_session_identification
import os
import traceback
import sys


@celery.task
def processing(filenames):
    parsed_file_dir = str(os.path.join(DATA_DIR, 'interm'))
    cleaned_file_dir = str(os.path.join(DATA_DIR, 'interm', 'clean'))
    identified_file_dir = str(os.path.join(DATA_DIR, 'interm', 'identified/'))
    model_file_dir = str(os.path.join(DATA_DIR, 'final/'))

    print("*----------------------------------*")
    print("starting parsing")
    print("*----------------------------------*")
    parsed_files = parsing(filenames, parsed_file_dir)

    print("*----------------------------------*")
    print("moving on to cleaning")
    print("*----------------------------------*")
    cleaned_files = cleaning(parsed_files, cleaned_file_dir)

    print("*----------------------------------*")
    print("moving on to session identication")
    print("*----------------------------------*")
    session_files = session_identification(cleaned_files, output_dir=identified_file_dir)

    print("*----------------------------------*")
    print("moving on to training")
    print("*----------------------------------*")
    return training(session_files, model_file_dir)


def parsing(filenames, output_dir):  # spark
    parser = LogParser(4)
    log = parser.load_log_file(filenames)
    return parser.data_wrangling(log, output_dir)


def cleaning(parsed_files, output_dir):
    parsed_file_path = parsed_files["parsed_log"]
    try:
        df = data_cleaning(parsed_file_path)
        preprocessing_for_markov(df)
        interm_cleaned_files = {}
        interm_cleaned_files["data_clean"] = "/home/souhagaa/Bureau/test/server/UX/UX/data/interm/data_clean.csv"
        return sampling(interm_cleaned_files["data_clean"], output_dir, 30)
    except:
        print("something went wrong while cleaning data")
        traceback.print_exc(file=sys.stdout)
        return []


def session_identification(cleaned_files, output_dir):
    created_files = []
    for f in cleaned_files:
        output_file = output_dir + 'sessions_user_{}.csv'.format(f[0])
        created_files.append((f[0], output_file))
        execute_session_identification(f[1], output_file)
    return created_files


def training(session_files, output_dir):
    """
    training_set_path="", test_set_path="", output_model_path=""
    """
    created_files = []
    i = 0
    for f in session_files:
        output_file = output_dir + "user_{}_markov_model.rds".format(f[0])
        created_files.append((f[0], output_file))
        fitting_model(f[1], None, output_file, i)
        i += 1
    return created_files
