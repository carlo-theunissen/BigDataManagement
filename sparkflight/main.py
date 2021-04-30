import findspark

findspark.init()

from pyspark.sql import SparkSession

logFile = r'C:\Users\20180180\Downloads\sparkflight\airline.csv'  # Should be some file on your system
spark = SparkSession.builder \
    .config("spark.driver.memory", "15g") \
    .appName("SparkFlight").getOrCreate()

logData = spark.read.text(logFile).cache()

numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

spark.stop()
