from log_Analytics_Spark import load_log_file
from log_Analytics_Spark import data_wrangling
from log_Analytics_Spark import content_size_summary
from log_Analytics_Spark import http_status_analysis
from log_Analytics_Spark import frequent_hosts
from log_Analytics_Spark import top_twenty_frequent_urls
from log_Analytics_Spark import top_ten_error_endpoints
from log_Analytics_Spark import number_unique_hosts
from log_Analytics_Spark import number_unique_daily_hosts
from log_Analytics_Spark import daily_requests_per_host
from log_Analytics_Spark import print_total_404_responses
from log_Analytics_Spark import top_twenty_404_responses_endpoints
from log_Analytics_Spark import top_twenty_404_hosts
from log_Analytics_Spark import errors_404_per_day
from log_Analytics_Spark import top_three_days_404_error
from log_Analytics_Spark import hourly_404_errors
from log_Analytics_Spark import load_dataframe


def main():
    print('hello')
    # if there are new log files else pass directly to load_dataframe
    log = load_log_file()
    data_wrangling(log)

    # logs_df = load_dataframe()
    # print("completed wrangling now loading df and showing 10 rows")
    # logs_df.show(10)
    # content_size_summary(logs_df)
    # http_status_analysis(logs_df)
    # frequent_hosts(logs_df)
    # top_twenty_frequent_urls(logs_df)
    # top_ten_error_endpoints(logs_df)
    # number_unique_hosts(logs_df)
    # number_unique_daily_hosts(logs_df)
    # daily_requests_per_host(logs_df)
    # print_total_404_responses(logs_df)
    # top_twenty_404_responses_endpoints(logs_df)
    # top_twenty_404_hosts(logs_df)
    # errors_404_per_day(logs_df)
    # top_three_days_404_error(logs_df)
    # hourly_404_errors(logs_df)


if __name__ == "__main__":
    # stuff only to run when not called via 'import' here
    main()
    print("tada")
