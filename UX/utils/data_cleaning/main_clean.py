from .data_cleaning import data_cleaning
from .data_cleaning import preprocessing_for_markov
from .data_cleaning import sample_training_set
from .data_cleaning import sample_test_set
# from data_cleaning import data_cleaning
# from data_cleaning import preprocessing_for_markov
# from data_cleaning import sample_training_set
# from data_cleaning import sample_test_set
import sys
import traceback


def process(parsed_file_path="/home/souhagaa/Bureau/test/server/UX/UX/data/interm/parsed_log.csv",
            output_training_set=None, output_test_set=None):  # def process():
    try:
        df = data_cleaning(parsed_file_path)  # file in log_parsing
        preprocessing_for_markov(df)
        start = sample_training_set(output_file=output_training_set)
        sample_test_set(start, output_file=output_test_set)
    except:
        print("something went wrong while cleaning data")
        traceback.print_exc(file=sys.stdout)
        return None
