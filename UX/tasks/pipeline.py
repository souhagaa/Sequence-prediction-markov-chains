from UX.extensions import celery
from UX.config import DATA_DIR
from UX.utils.log_parsing import get_log_files, parse_access_logs, \
    parse_presentation_logs
from UX.utils.data_cleaning import clean_parsed_logs
from UX.utils.session_identification import session_period_identification, \
    actions_to_sessions
from UX.utils.markov_click import fitting_model
import os
from pathlib import Path


@celery.task
def processing(input_dir):
    parsed_file_dir = str(os.path.join(DATA_DIR, 'interm/'))
    cleaned_file_dir = str(os.path.join(DATA_DIR, 'interm', 'clean/'))
    identified_file_dir = str(os.path.join(DATA_DIR, 'interm', 'identified/'))
    model_file_dir = str(os.path.join(DATA_DIR, 'final/'))

    print("*----------------------------------*")
    print("starting parsing")
    print("*----------------------------------*")
    parsed_files = parsing(input_dir, parsed_file_dir)

    print("*----------------------------------*")
    print("moving on to cleaning")
    print("*----------------------------------*")
    cleaned_files = cleaning(parsed_files, cleaned_file_dir)

    print("*----------------------------------*")
    print("moving on to session identication")
    print("*----------------------------------*")
    session_files = session_identification(cleaned_files, identified_file_dir)

    print("*----------------------------------*")
    print("moving on to training")
    print("*----------------------------------*")
    return training(session_files, model_file_dir)


def parsing(input_dir, output_dir):
    parsed_access_log = output_dir+"parsed_access_log.csv"
    parsed_presentation_log = output_dir+"parsed_presentation_log.csv"

    access_logs_filenames, presentation_logs_filenames = get_log_files(input_dir)
    parse_presentation_logs(presentation_logs_filenames, parsed_presentation_log)
    parse_access_logs(access_logs_filenames, parsed_access_log)

    return {"access": parsed_access_log, "presentation": parsed_presentation_log}


def cleaning(parsed_files, output_dir):
    input_access = parsed_files['access']
    input_pres = parsed_files['presentation']
    output_pres = output_dir + "clean_presentation_log.csv"
    output_access = output_dir + "clean_access_log.csv"
    screen_ids = str(Path(output_dir).parent.parent) + '/final/screen_ids.json'
    clean_parsed_logs(input_access, input_pres, output_pres, output_access, screen_ids)

    return {"access": output_access, "presentation": output_pres}


def session_identification(cleaned_files, output_dir):
    input_access = cleaned_files['access']
    input_pres = cleaned_files['presentation']
    sessions_period_file = str(Path(output_dir).parent) + '/sessions_periods.csv'
    print("executing session_period_identification", sessions_period_file)
    session_period_identification(input_access, sessions_period_file)
    output_file = output_dir + "user_{}_sessions.csv"
    created_files = actions_to_sessions(input_pres, sessions_period_file, output_file)
    return created_files


def training(session_files, output_dir):
    created_files = []
    print("session files", type(session_files), session_files)
    for f in session_files:
        user = f.split('_')[1]
        print(user, "sessions")
        output_file = output_dir + "user_{}_markov_model.rds".format(user)
        created_files.append((user, output_file))
        fitting_model(f, output_file)
    return created_files
