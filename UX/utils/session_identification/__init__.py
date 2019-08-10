from .session_identification import execute_session_identification
from .session_identification import delete_redundancy_sessions


def session_identification(
    training_set_path='/home/souhagaa/Bureau/test/server/UX/UX/data/interm/data_sample.csv',
    test_set_path='/home/souhagaa/Bureau/test/server/UX/UX/data/interm/test_sample.csv',
    output_training_set_final='/home/souhagaa/Bureau/test/server/UX/UX/data/final/training_set.csv',
    output_test_set_final='/home/souhagaa/Bureau/test/server/UX/UX/data/final/test_set.csv'):

    execute_session_identification(dataset_file=training_set_path)
    # test set how?
    execute_session_identification(name="test", dataset_file=test_set_path)
    delete_redundancy_sessions(output_file=output_training_set_final)
    delete_redundancy_sessions(name="test", output_file=output_test_set_final)
