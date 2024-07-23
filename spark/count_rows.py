import os

from pyspark.sql import SparkSession

pathDataFolder = os.getenv("DATA_FOLDER")


def count_accidents_spark():
    spark = SparkSession.builder.appName("CountAccidents").getOrCreate()
    df = spark.read.csv(path=pathDataFolder + "/crash-data-monroe-county-2003-to-2015-1.csv",
                        sep=",",
                        header=True,
                        inferSchema=True)
    rows = df.count()
    f = open(pathDataFolder + "/number.txt", "w")
    f.write(str(rows))
    f.close()
    print("Numer of rows " + str(rows))
    spark.stop()


count_accidents_spark()
