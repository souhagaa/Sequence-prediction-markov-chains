from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import regexp_extract
import re

sc = SparkContext("local[3]")
sqlContext = SQLContext(sc)
spark = SparkSession(sc)

base_df = spark.read.text("/home/souhagaa/Bureau/test/server/UX/UX/utils/log_parsing/log_sample.log")
base_df.printSchema()
base_df.show(10)
# text = '''83.198.250.175 - - [22/Mar/2009:07:40:06 +0100] 1lm78df89sr "GET / HTTP/1.1" 200 8714 "http://www.google.fr/search?hl=fr&q=stand+de+foire&meta=&aq=4&oq=stand+de+" "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Wanadoo 6.7; Orange 8.0)" "-"'''
sample_logs = [item['value'] for item in base_df.take(5)]
sample_logs
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
# (\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})? (\S+) (\S+) (\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]) (.*?) \"(.*?)\" (\d{3}) (\S+) \"(.*?)\" \"(.*?)\)
# works with 94.102.63.11 - - [21/Jul/2009:02:48:13 -0700] lhk78dkggl1 "GET / HTTP/1.1" 200 18209 "http://acme.com/foo.php" "Mozilla/4.0 (compatible; MSIE 5.01; Windows NT 5.0)
# hosts = [re.search(host_pattern, item).group(1)
#            if re.search(host_pattern, item)
#            else 'no match'
#            for item in sample_logs]
# hosts
# timestamps = [re.search(ts_pattern, item).group(1) for item in sample_logs]
# timestamps
logs_df = base_df.select(regexp_extract('value', host_pattern, 1).alias('host'),
                         regexp_extract('value', ts_pattern, 1).alias('timestamp'),
                         regexp_extract('value', method_uri_protocol_pattern, 1).alias('method'),
                         regexp_extract('value', method_uri_protocol_pattern, 2).alias('endpoint'),
                         regexp_extract('value', method_uri_protocol_pattern, 3).alias('protocol'),
                         regexp_extract('value', status_pattern, 1).cast('integer').alias('status'),
                         regexp_extract('value', content_size_pattern, 1).cast('integer').alias('content_size'))
logs_df.show(10, truncate=True)
