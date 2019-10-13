from evaluation import actions_to_sessions, training, execute_evaluation


def main(data_path):
    sessions_period_file = data_path + '/interm/sessions_periods.csv'
    clean_pres_log = data_path + '/interm/clean/clean_presentation_log.csv'
    output_file = data_path + '/evaluation_data/train/user_{}_sessions.csv'
    output_test_file = data_path + '/evaluation_data/test/user_{}_test_sessions.csv'
    created_train_files, created_test_files = actions_to_sessions(clean_pres_log, sessions_period_file, output_file, output_test_file)
    models_path = data_path + '/evaluation_data/model/'
    models = training(created_train_files, models_path)
    mean_error = execute_evaluation(created_test_files, created_train_files, '/home/souhagaa/Bureau/test/server/UX/UX/data')
    print("the average model accuracy is:", 1-mean_error)


if __name__ == "__main__":
    main('/home/souhagaa/Bureau/test/server/UX/UX/data')
