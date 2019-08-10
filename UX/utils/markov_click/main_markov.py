from markov import fitting_model
from markov import predict
from pathlib import Path


def main():
    # my_file = Path("./markov_model.rds")
    # if my_file.is_file() and new_data:  # and we have new data
    #     # a verifier
    #     # refit/update model
    #     # add new data to old data
    #     print("file exists and new data")
    #     fitting_model()
    #     predict(link)
    # else:
    #     # a verifier
    #     if my_file.is_file():
    #         print("file does not exist and no new data")
    #         # predict
    #         # link_id = "P15"
    #         predict(link)
    #     else:
    #         print("no file")
    #         # fit model and save it
    #         time_fitting = fitting_model()
    #         print("fitting model time", time_fitting)
    #         predicted_link, predict_time = predict(link)
    #         print(predicted_link)
    #         print("time taken to predict", predict_time)
    #  host 3 "01-dynamic-c.wokingham.luna.net"
    fitting_model("/home/souhagaa/Bureau/test/server/UX/UX/data/interm/identified/sessions_user_3.csv", None, "/home/souhagaa/Bureau/test/server/UX/UX/utils/markov_click/test_model.rds")
    try:
        link = predict("/history/apollo/images/apollo-logo1.gif", "/home/souhagaa/Bureau/test/server/UX/UX/utils/markov_click/test_model.rds")
        print("predicted link", link)
    except:
        print("url or user error")

if __name__ == "__main__":
    # stuff only to run when not called via 'import' here
    # predict("/history/apollo/apollo-13/apollo-13.html")
    main()
    print("the end")
