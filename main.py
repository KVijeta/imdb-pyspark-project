# Python for Big Data and Data Science final project from Viktor Kutsak
# Launch of the project

from pyspark import SparkConf
from pyspark.sql import SparkSession
import task1
import task2
import task3
import task4
import task5
import task6
import task7
import task8


spark_session = (SparkSession.builder
                             .master("local")
                             .appName("imdb-pyspark-project")
                             .config(conf=SparkConf())
                             .getOrCreate())


def main():
    task1.task1()
    task2.task2()
    task3.task3()
    task4.task4()
    task5.task5()
    task6.task6()
    task7.task7()
    task8.task8()


if __name__ == "__main__":
    main()
