
from pyspark import SparkConf
from pyspark.sql import SparkSession

spark_session = (SparkSession.builder
                             .master("local")
                             .appName("imdb-pyspark-project")
                             .config(conf=SparkConf())
                             .getOrCreate())

def main():
    spark_session = ...

    movies_df = spark_session.read.csv(path)
    movies_df.show()

if __name__ == "__main__":
    main()
