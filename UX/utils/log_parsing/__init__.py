import pandas as pd
import os
import re
from datetime import datetime
import time
import locale
locale.setlocale(locale.LC_ALL, str('en_US.UTF-8'))
pd.options.mode.chained_assignment = None


def get_log_files(directory_in_str="/home/souhagaa/Bureau/test/server/UX/UX/data/raw/"):
    directory = os.fsencode(directory_in_str)
    access_logs_filenames = []
    presentation_logs_filenames = []
    r1 = re.compile("(?i).*presentation.*\..*")
    r2 = re.compile("(?i).*access.*\..*")
    for file in os.listdir(directory):
        filename = os.fsdecode(file)
        if r2.search(filename):
            str_file = str(os.path.join(directory_in_str, filename))
            access_logs_filenames.append(str_file)
        else:
            if r1.search(filename):
                str_file = str(os.path.join(directory_in_str, filename))
                presentation_logs_filenames.append(str_file)

    return access_logs_filenames, presentation_logs_filenames


def parse_presentation_logs(presentation_logs_filenames,
                            output_file="/home/souhagaa/Bureau/test/server/UX/UX/data/interm/parsed_presentation_log.csv"):
    df_all = pd.DataFrame(columns=['datetime', 'username', 'screen'])
    for f in presentation_logs_filenames:
        df = pd.read_csv(f, header=None, sep="|")
        df = df[[0, 4, 5]]
        df.columns = ['datetime', 'username', 'usecases']
        df['username'] = df['username'].str[4:]
        df['usecases'] = df['usecases'].str[16:]

        # only take rows containing the screen visited
        df = df[df['usecases'].astype(str).str.startswith('[')]

        # taking only the last usecase in a nested usecase
        use_cases_values = df.usecases.str.split('->').str[-1].tolist()
        del df['usecases']
        df['screen'] = use_cases_values
        df['screen'] = df['screen'].str.partition(",")[0].str[9:]
        df['datetime'] = df['datetime'].map(lambda x: str(x)[:-1])
        print("parsing dates now")
        df['datetime'] = pd.to_datetime(df['datetime'], format='%b %d, %Y %H:%M:%S:%f')
        df_all = df_all.append(df)
    print("showing df_all")
    print(df_all.head())
    df_all.to_csv(output_file, index=False)


def parse_access_logs(access_logs_filenames,
                      output_file="/home/souhagaa/Bureau/test/server/UX/UX/data/interm/parsed_access_log.csv"):
    df_all = pd.DataFrame(columns=['datetime', 'username', 'session_id'])
    for f in access_logs_filenames:
        df_temp = pd.read_csv(f, header=None, sep="\n")
        df = pd.DataFrame(columns=['datetime', 'username', 'session_id'])
        df['session_id'] = df_temp[0].str.extract(r'(?P<session_id>[a-zA-Z0-9\+\-\*%\$@]{24}) (?P<port>http\-\/)')['session_id']
        df['datetime'] = df_temp[0].str.extract(r'(?P<datetime>\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2}\s\+\d{4})\])')[1]
        df['username'] = df_temp[0].str.extract(r'(^\S+\.[\S+\.]+\S+) \- (?P<username>\-|[a-zA-Z]{1,20})')['username']
        del df_temp
        print("parsing dates now")
        try:
            df['datetime'] = pd.to_datetime(df['datetime'], format='%d/%b/%Y:%H:%M:%S %z')
        except ValueError:
            df['datetime'] = pd.to_datetime(df['datetime'], infer_datetime_format=True)
        # df['datetime'] = df['datetime'].str[:-6]
        # '30/May/2019:00:40:21 +0200'[:-6]
        # df['datetime'] = pd.to_datetime(df['datetime'], format='%d/%b/%Y:%H:%M:%S %z')
        # try:
        #     df['datetime'] = pd.to_datetime(df['datetime'], format='%d/%b/%Y:%H:%M:%S')
        # except ValueError:
        #     print("error while parsing date")
        #     df['datetime'] = df['datetime'].apply(lambda x: time.strptime(x,'%d/%b/%Y:%H:%M:%S'))
        df_all = df_all.append(df)
    print("showing parsed access log", df_all.head())
    df_all.to_csv(output_file, index=False)
