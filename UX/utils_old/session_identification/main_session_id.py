from session_identification import execute_session_identification


def main():
    name = execute_session_identification("/home/souhagaa/Bureau/test/server/UX/UX/data/interm/clean/training_user_3.csv",
    "/home/souhagaa/Bureau/test/server/UX/UX/data/interm/identified/training_set_3.csv")
    print("name of file: ", name)

if __name__ == "__main__":
    main()
