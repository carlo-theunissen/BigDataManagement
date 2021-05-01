import findspark

findspark.init()

from pyspark.sql import SparkSession

from configparser import ConfigParser
config = ConfigParser()
# create your own config.ini in root of project folder to store project configurations
config.read('../config.ini')

data_is_clean = config.getboolean('main', 'preprocessing')

if data_is_clean:
    print("data is clean")
    path_to_csv = config.get('main', 'clean_csv')

else:
    print("data is dirty")
    path_to_csv = config.get('main', 'dirty_csv')


logFile = path_to_csv  # Should be some file on your system
spark = SparkSession.builder \
    .config("spark.driver.memory", "15g") \
    .appName("SparkFlight").getOrCreate()

logData = spark.read.csv(logFile).cache()

if data_is_clean:
    # 
    logData.head()
else:
    # result = logData.limit(10**3).
    amount_rows = logData.count()
    # > 120 million
    print(amount_rows)
    # logData.rdd.map(lambda row: ())

# result = logData.take(10)

# print(result)

# numAs = logData.filter(logData.value.contains('a')).count()
# numBs = logData.filter(logData.value.contains('b')).count()
spark.stop()

# def unique_val_cols(row):
#     row.
