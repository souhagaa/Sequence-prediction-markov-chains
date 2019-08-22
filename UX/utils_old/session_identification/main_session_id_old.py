from session_identification import execute_session_identification
from session_identification import delete_redundancy_sessions

def main():
    execute_session_identification()
    # test set how?
    execute_session_identification(name="test", dataset_file='/home/souhagaa/Bureau/test/server/UX/UX/tmp/test_sample.csv')
    delete_redundancy_sessions()
    delete_redundancy_sessions(name="test")


if __name__ == "__main__":
    main()
