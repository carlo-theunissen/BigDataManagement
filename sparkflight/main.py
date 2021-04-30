import findspark

findspark.init()

from pyspark.sql import SparkSession

from configparser import ConfigParser
config = ConfigParser()
config.read('../config.ini')
path_to_csv = config.get('main', 'csv')

# path_to_csv = "/mnt/c/Users/twanv/flight_data/airline.csv"


logFile = path_to_csv  # Should be some file on your system
spark = SparkSession.builder \
    .config("spark.driver.memory", "15g") \
    .appName("SparkFlight").getOrCreate()

logData = spark.read.text(logFile).cache()

numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

spark.stop()
