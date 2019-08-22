import seaborn as sns
import matplotlib.pyplot as plt
# import findspark
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import col
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from py4j.java_gateway import java_import
from pyspark.sql.functions import unix_timestamp
import pandas as pd
import glob


class LogParser:
    def __init__(self, nbCores = 2):
        self.sc = SparkContext("local[{}]".format(nbCores))
        self.sqlContext = SQLContext(self.sc)
        self.spark = SparkSession(self.sc)

    def load_log_file(self, raw_data_files=None):
        if raw_data_files is None:
            # raw_data_files = glob.glob('*.gz')  # or .log
            raise Exception("no file")
        base_df = self.spark.read.text(raw_data_files)
        base_df.printSchema()
        type(base_df)
        base_df.show(10)
        return base_df

    def load_dataframe(self):
        print("loading the dataframe")
        base_df = self.spark.read.format("csv").option("header", "true").load("/home/souhagaa/Bureau/test/server/UX/UX/data/interm/analytic_log.csv")
        return base_df

    def save_spark_df(self, df, name, output_file_path=None):
        output1 = "/home/souhagaa/Bureau/test/server/UX/UX/data/interm/{}.csv-temp".format(name)
        part = '/home/souhagaa/Bureau/test/server/UX/UX/data/interm/{}.csv-temp/part*'.format(name)
        final = output_file_path.format(name)

        print("inside save spark df to csv")
        df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(output1)

        java_import(self.spark._jvm, 'org.apache.hadoop.fs.Path')

        fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(self.spark._jsc.hadoopConfiguration())
        file = fs.globStatus(self.sc._jvm.Path(part))[0].getPath().getName()
        fs.rename(self.sc._jvm.Path(output1 + '/' + file), self.sc._jvm.Path(final))
        fs.delete(self.sc._jvm.Path(output1), True)
        path = '/home/souhagaa/Bureau/test/server/UX/UX/data/interm/.{}.csv.crc'.format(name)
        fs.delete(self.sc._jvm.Path(path), True)
        print("dataframe saved")

    # Data Wrangling
    def data_wrangling(self, base_df, output_dir=None):
        print("starting data wrangling by extracting regular expressions")
        print((base_df.count(), len(base_df.columns)))
        # extracting host names
        host_pattern = r'(^\S+\.[\S+\.]+\S+)\s'
        # extracting timestamps
        ts_pattern = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
        # extracting http requests, URI and protocol
        method_uri_protocol_pattern = r'\"(\S+)\s(\S+)\s*(\S*)\"'
        # extracting HTTP status code
        status_pattern = r'\s(\d{3})\s'
        # extracting http response content size
        content_size_pattern = r'\s(\d+)$'

        logs_df = base_df.select(regexp_extract('value', host_pattern, 1).alias('host'),
                                 regexp_extract('value', ts_pattern,
                                                1).alias('timestamp'),
                                 regexp_extract(
                                     'value', method_uri_protocol_pattern, 1).alias('method'),
                                 regexp_extract('value', method_uri_protocol_pattern, 2).alias(
                                     'endpoint'),
                                 regexp_extract('value', method_uri_protocol_pattern, 3).alias(
                                     'protocol'),
                                 regexp_extract('value', status_pattern, 1).cast(
                                     'integer').alias('status'),
                                 regexp_extract('value', content_size_pattern, 1).cast('integer').alias('content_size'))
        logs_df.show(10, truncate=True)
        print((logs_df.count(), len(logs_df.columns)))
        print("finding null values now")
        # Finding Missing Values
        # If our data parsing and extraction worked properly, we should not have
        # any rows with potential null values. Let's try and put that to test!
        bad_rows_df = logs_df.filter(logs_df['host'].isNull() |
                                     logs_df['timestamp'].isNull() |
                                     logs_df['method'].isNull() |
                                     logs_df['endpoint'].isNull() |
                                     logs_df['status'].isNull() |
                                     logs_df['content_size'].isNull() |
                                     logs_df['protocol'].isNull())
        bad_rows_df.count()
        # we have over 33K missing values in our data!

        # this is not a regular pandas dataframe which you can directly
        # query and get which columns have null. Our big dataset is
        # residing on disk which can potentially be present in multiple nodes in a
        # spark cluster. So how do we find out which columns have potential nulls?

        # Finding Null Counts
        # We can typically use the following technique to find out which columns
        # have null values.This approach is adapted from an
        # [excellent answer](http://stackoverflow.com/a/33901312) on StackOverflow.
        def count_null(col_name):
            return spark_sum(col(col_name).isNull().cast('integer')).alias(col_name)

        # Build up a list of column expressions, one per column.
        exprs = [count_null(col_name) for col_name in logs_df.columns]
        # Run the aggregation. The *exprs converts the list of expressions into
        # variable function arguments.
        logs_df.agg(*exprs).show()
        # we have one missing value in the `status` column and everything else
        # is in the `content_size` column.

        # Handling nulls in HTTP status
        # Our original parsing regular expression for the `status` column was:
        # regexp_extract('value', r'\s(\d{3})\s', 1).cast('integer').alias('status')
        # Could it be that there are more digits making our regular expression wrong?
        # or is the data point itself bad? Let's try and find out!
        # **Note**: In the expression below, `~` means "not".

        null_status_df = base_df.filter(~base_df['value'].rlike(r'\s(\d{3})\s'))
        null_status_df.count()
        null_status_df.show(truncate=False)
        bad_status_df = null_status_df.select(regexp_extract('value', host_pattern, 1).alias('host'),
                                              regexp_extract('value', ts_pattern, 1).alias(
                                                  'timestamp'),
                                              regexp_extract(
                                                  'value', method_uri_protocol_pattern, 1).alias('method'),
                                              regexp_extract('value', method_uri_protocol_pattern, 2).alias(
                                                  'endpoint'),
                                              regexp_extract('value', method_uri_protocol_pattern, 3).alias(
                                                  'protocol'),
                                              regexp_extract('value', status_pattern, 1).cast(
                                                  'integer').alias('status'),
                                              regexp_extract('value', content_size_pattern, 1).cast('integer').alias('content_size'))
        bad_status_df.show(truncate=False)

        # Looks like the record itself is an incomplete record with no useful
        # information, the best option would be to drop this record as follows!
        logs_df.count()
        logs_df = logs_df[logs_df['status'].isNotNull()]
        logs_df.count()
        exprs = [count_null(col_name) for col_name in logs_df.columns]
        logs_df.agg(*exprs).show()

        # Handling nulls in HTTP content size
        # Based on our previous regular expression, our original parsing
        # regular expression for the `content_size` column was:
        # regexp_extract('value', r'\s(\d+)$', 1).cast('integer').alias('content_size')
        # Could there be missing data in our original dataset itself?
        # Find out the records in our base data frame with potential missing content sizes
        null_content_size_df = base_df.filter(~base_df['value'].rlike(r'\s\d+$'))
        null_content_size_df.count()
        # Display the top ten records of your data frame having missing content sizes
        null_content_size_df.take(10)

        # It is quite evident that the bad raw data records correspond to error
        # responses, where no content was sent back and the server emitted a "`-`"
        # for the `content_size` field. Since we don't want to discard those
        # rows from our analysis, let's impute or fill them to 0.
        # Fix the rows with null content\_size
        logs_df = logs_df.na.fill({'content_size': 0})

        # Now assuming everything we have done so far worked, we should have
        # no missing values \ nulls in our dataset. Let's verify this!

        exprs = [count_null(col_name) for col_name in logs_df.columns]
        logs_df.agg(*exprs).show()
        # no missing values!

        # Handling Temporal Fields (Timestamp)
        # Now that we have a clean, parsed DataFrame, we have to parse the
        # timestamp field into an actual timestamp. The Common Log Format time
        # is somewhat non-standard. A User-Defined Function (UDF) is the most
        # straightforward way to parse it.
        month_map = {
            'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6, 'Jul': 7,
            'Aug': 8,  'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
        }

        def parse_clf_time(text):
            """ Convert Common Log time format into a Python datetime object
            Args:
                text (str): date and time in Apache time format [dd/mmm/yyyy:hh:mm:ss (+/-)zzzz]
            Returns:
                a string suitable for passing to CAST('timestamp')
            """
            # We're ignoring the time zones here, might need to be handled
            # depending on the problem you are solving
            return "{0:04d}-{1:02d}-{2:02d} {3:02d}:{4:02d}:{5:02d}".format(
                int(text[7:11]),
                month_map[text[3:6]],
                int(text[0:2]),
                int(text[12:14]),
                int(text[15:17]),
                int(text[18:20])
            )
        print("parsing datetimes")
        udf_parse_time = udf(parse_clf_time)
        logs_df = logs_df.select(
            '*', udf_parse_time(logs_df['timestamp']).cast('timestamp').alias('time')).drop('timestamp')
        logs_df.show(10, truncate=True)
        df = logs_df.withColumn('timestamp', unix_timestamp('time'))
        df = df.drop('time')
        df.printSchema()
        df.limit(5).toPandas()

        output_dir += "/{}.csv"
        self.save_spark_df(logs_df, "analytic_log", output_dir)
        self.save_spark_df(df, "parsed_log", output_dir)

        return {"analytic_log": output_dir.format("analytic_log"),
                "parsed_log": output_dir.format("parsed_log")}
    # Let's now cache `logs_df` since we will be using it extensively for our data
    # analysis section in the next part!
    # logs_df.cache()

    def content_size_summary(self, logs_df):
        ''' Data Analysis on our Web Logs
        Now that we have a DataFrame containing the parsed log file as a
        dataframe, we can perform some interesting exploratory data analysis(EDA)
        Content Size Statistics
        Let's compute some statistics about the sizes of content being returned
        by the web server. In particular, we'd like to know what are the average,
        minimum, and maximum content sizes.
        We can compute the statistics by calling `.describe()` on the
        `content_size` column of `logs_df`.  The `.describe()` function returns
        the count, mean, stddev, min, and max of a given column.'''

        content_size_summary_df = logs_df.describe(['content_size'])
        content_size_summary_df.toPandas()
        # After we apply the `.agg()` function, we call `toPandas()` to extract
        # and convert the result into a `pandas` dataframe which has better
        # formatting on Jupyter notebooks
        (logs_df.agg(F.min(logs_df['content_size']).alias('min_content_size'),
                     F.max(logs_df['content_size']).alias('max_content_size'),
                     F.mean(logs_df['content_size']).alias('mean_content_size'),
                     F.stddev(logs_df['content_size']).alias('std_content_size'),
                     F.count(logs_df['content_size']).alias('count_content_size'))
         .toPandas())

    def http_status_analysis(self, logs_df):
        '''HTTP Status Code Analysis
        let's look at the status code values that appear in the log.
        We want to know which status code values appear in the data and how many
        times. We again start with `logs_df`, then group by the `status` column,
        apply the `.count()` aggregation function, and sort by the `status` column.
        '''
        status_freq_df = (logs_df.groupBy('status').count().sort('status').cache())
        print('Total distinct HTTP Status Codes:', status_freq_df.count())
        status_freq_pd_df = (status_freq_df
                             .toPandas()
                             .sort_values(by=['count'],
                                          ascending=False))
        status_freq_pd_df
        sns.catplot(x='status', y='count', data=status_freq_pd_df,
                    kind='bar', order=status_freq_pd_df['status'])
        plt.title('Total distinct HTTP Status Codes')
        plt.show()
        # Not too bad. But several status codes are nearly invisible due to
        # the data's huge skew. Let’s do a log transform and see if things improve.
        # Usually log transforms help us transform highly skewed data to an
        # approximate normal distribution, so that we can visualize the data
        # distribution in a more comprehensible manner:

        log_freq_df = status_freq_df.withColumn(
            'log(count)', F.log(status_freq_df['count']))
        log_freq_df.show()

        log_freq_pd_df = (log_freq_df
                          .toPandas()
                          .sort_values(by=['log(count)'],
                                       ascending=False))
        sns.catplot(x='status', y='log(count)', data=log_freq_pd_df,
                    kind='bar', order=status_freq_pd_df['status'])
        plt.title('Total distinct HTTP Status Codes without skewed data')
        plt.show()
        # This chart definitely looks much better and less skewed,
        # giving us a better idea of the distribution of status codes!

    def frequent_hosts(self, logs_df):
        # Analyzing Frequent Hosts
        # Let's look at hosts that have accessed the server frequently.
        # We will try to get the count of total accesses by each `host` and
        # then sort by the counts and display only the top ten most frequent hosts.
        host_sum_df = (logs_df
                       .groupBy('host')
                       .count()
                       .sort('count', ascending=False).limit(10))

        host_sum_df.show(truncate=False)
        host_sum_pd_df = host_sum_df.toPandas()
        host_sum_pd_df.iloc[8]['host']

        # Looks like we have some empty strings as one of the top host names!
        # This teaches us a valuable lesson to not just check for nulls but also
        # potentially empty strings when data wrangling.

    def top_twenty_frequent_urls(self, logs_df):
        # Display the Top 20 Frequent EndPoints
        # Now, let's visualize the number of hits to endpoints (URIs) in the log.
        # To perform this task, we start with our `logs_df` and group by
        # the `endpoint` column, aggregate by count, and sort in descending
        # order like the previous question.
        paths_df = (logs_df
                    .groupBy('endpoint')
                    .count()
                    .sort('count', ascending=False).limit(20))
        paths_pd_df = paths_df.toPandas()
        paths_pd_df

    def top_ten_error_endpoints(self, logs_df):
        # Top Ten Error Endpoints
        # What are the top ten endpoints requested which did not have return
        # code 200 (HTTP Status OK)?
        # We create a sorted list containing the endpoints and the number of times
        # that they were accessed with a non-200 return code and show the top ten.
        not200_df = (logs_df
                     .filter(logs_df['status'] != 200))

        error_endpoints_freq_df = (not200_df
                                   .groupBy('endpoint')
                                   .count()
                                   .sort('count', ascending=False)
                                   .limit(10)
                                   )

        error_endpoints_freq_df.show(truncate=False)

    def number_unique_hosts(self, logs_df):
        # Total number of Unique Hosts
        # What were the total number of unique hosts who visited the NASA website
        # in these two months? We can find this out with a few transformations.
        unique_host_count = (logs_df
                             .select('host')
                             .distinct()
                             .count())
        print("Total number of Unique Hosts ", unique_host_count)


    def get_unique_daily_users(self, logs_df):
        ''' this is an inside function
        '''
        host_day_df = logs_df.select(logs_df.host,
                                     F.dayofmonth('time').alias('day'))
        # This DataFrame has the same columns as `host_day_df`, but with duplicate
        # (`day`, `host`) rows removed.
        host_day_distinct_df = (host_day_df
                                .dropDuplicates())
        pd.get_option('max_rows')
        pd.set_option('max_rows', 10)
        return host_day_distinct_df


    def number_unique_daily_hosts(self, logs_df):
        # Number of Unique Daily Hosts
        # the number of unique hosts in the entire log on a day-by-day basis
        # We'd like a DataFrame sorted by increasing day of the month which
        # includes the day of the month and the associated number of unique
        # hosts for that day.
        # *Since the log only covers a single month, we ignore the month.*
        host_day_distinct_df = self.get_unique_daily_users(logs_df)

        daily_hosts_df = (host_day_distinct_df
                          .groupBy('day')
                          .count()
                          .sort("day"))
        daily_hosts_df = daily_hosts_df.toPandas()
        print("Number of Unique Daily Hosts ", daily_hosts_df)
        c = sns.catplot(x='day', y='count',
                        data=daily_hosts_df,
                        kind='point', height=5,
                        aspect=1.5)
        plt.title("Number of Unique Daily Hosts")
        plt.show()


    def daily_requests_per_host(self, logs_df):
        # Average Number of Daily Requests per Host
        # we determine the number of unique hosts in the entire log by day
        # Let's now try and find the average number of requests being made per Host
        # to the NASA website per day based on our logs.
        # We'd like a DataFrame sorted by increasing day of the month which
        # includes the day of the month and the associated number of average
        # requests made for that day per Host.
        host_day_distinct_df = self.get_unique_daily_users(logs_df)
        daily_hosts_df = (host_day_distinct_df
                          .groupBy('day')
                          .count()
                          .select(col("day"),
                                  col("count").alias("total_hosts")))

        total_daily_reqests_df = (logs_df
                                  .select(F.dayofmonth("time")
                                          .alias("day"))
                                  .groupBy("day")
                                  .count()
                                  .select(col("day"),
                                          col("count").alias("total_reqs")))

        avg_daily_reqests_per_host_df = total_daily_reqests_df.join(
            daily_hosts_df, 'day')
        avg_daily_reqests_per_host_df = (avg_daily_reqests_per_host_df
                                         .withColumn('avg_reqs', col('total_reqs') / col('total_hosts'))
                                         .sort("day"))
        avg_daily_reqests_per_host_df = avg_daily_reqests_per_host_df.toPandas()
        print("The average daily requests per host: ", avg_daily_reqests_per_host_df.head(10))
        c = sns.catplot(x='day', y='avg_reqs',
                        data=avg_daily_reqests_per_host_df,
                        kind='point', height=5, aspect=1.5)
        plt.title("The average daily requests per host")
        plt.show()


    def get_404_responses(self, logs_df):
        ''' this is an inside function
        '''
        return logs_df.filter(logs_df["status"] == 404)


    def get_errors_by_date(self, logs_df):
        ''' this is an inside function
        '''
        not_found_df = self.get_404_responses(logs_df)
        errors_by_date_sorted_df = (not_found_df
                                    .groupBy(F.dayofmonth('time').alias('day'))
                                    .count()
                                    .sort("day"))
        return errors_by_date_sorted_df

    def print_total_404_responses(self, logs_df):
        # Counting 404 Response Codes
        # Create a DataFrame containing only log records with a 404 status code
        # We make sure to `cache()` the `not_found_df` dataframe as we will use it
        # in the rest of the examples here
        # __How many 404 records are in the log?__
        not_found_df = self.get_404_responses(logs_df).cache()
        print(('Total 404 responses: {}').format(not_found_df.count()))

    def top_twenty_404_responses_endpoints(self, logs_df):
        not_found_df = self.get_404_responses(logs_df)
        # Listing the Top Twenty 404 Response Code Endpoints
        # Using the DataFrame containing only log records with a 404
        # response code that we cached earlier, we will now print out a list of
        # the top twenty endpoints that generate the most 404 errors.
        endpoints_404_count_df = (not_found_df
                                  .groupBy("endpoint")
                                  .count()
                                  .sort("count", ascending=False)
                                  .limit(20))
        print("The top twenty endpoints that generate the most 404 errors:")
        endpoints_404_count_df.show(truncate=False)

    def top_twenty_404_hosts(self, logs_df):
        not_found_df = self.get_404_responses(logs_df)
        # Listing the Top Twenty 404 Response Code Hosts
        #
        # Using the DataFrame containing only log records with a 404 response code
        # that we cached earlier, we will now print out a list of the top twenty
        # hosts that generate the most 404 errors.
        hosts_404_count_df = (not_found_df
                              .groupBy("host")
                              .count()
                              .sort("count", ascending=False)
                              .limit(20))
        print("The top twenty hosts that generate the most 404 errors:")
        hosts_404_count_df.show(truncate=False)

    def errors_404_per_day(self, logs_df):
        # Visualizing 404 Errors per Day
        #
        # Let's explore our 404 records temporally (by time) now. Similar to the
        # example showing the number of unique daily hosts, we will break down the
        # 404 requests by day and get the daily counts sorted by day in
        # `errors_by_date_sorted_df`.
        errors_by_date_sorted_df = self.get_errors_by_date(logs_df)

        errors_by_date_sorted_pd_df = errors_by_date_sorted_df.toPandas()
        print("The daily counts of 404 requests: ")
        errors_by_date_sorted_pd_df.head(10)
        c = sns.catplot(x='day', y='count',
                        data=errors_by_date_sorted_pd_df,
                        kind='point', height=5, aspect=1.5)
        plt.title('The daily counts of 404 requests')
        plt.show()

    def top_three_days_404_error(self, logs_df):
        errors_by_date_sorted_df = self.get_errors_by_date(logs_df)
        # Top Three Days for 404 Errors
        #
        # What are the top three days of the month having the most 404 errors,
        # we can leverage our previously created __`errors_by_date_sorted_df`__ for
        # this.
        print("Top Three Days for 404 Errors")
        (errors_by_date_sorted_df
            .sort("count", ascending=False)
            .show(3))

    def hourly_404_errors(self, logs_df):
        # Visualizing Hourly 404 Errors
        # Using the DataFrame `not_found_df` we cached earlier, we will now group
        # and sort by hour of the day in increasing order, to create a DataFrame
        # containing the total number of 404 responses for HTTP requests for each
        # hour of the day (midnight starts at 0)
        not_found_df = self.get_404_responses(logs_df)
        hourly_avg_errors_sorted_df = (not_found_df
                                       .groupBy(F.hour('time')
                                                .alias('hour'))
                                       .count()
                                       .sort('hour'))
        hourly_avg_errors_sorted_pd_df = hourly_avg_errors_sorted_df.toPandas()
        print("Visualizing Hourly 404 Errors: ")
        c = sns.catplot(x='hour', y='count',
                        data=hourly_avg_errors_sorted_pd_df,
                        kind='bar', height=5, aspect=1.5)
        plt.title('Hourly 404 Errors')
        plt.show()
        # Looks like total 404 errors occur the most in the afternoon and the
        # least in the early morning.
